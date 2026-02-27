/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.remote.GdbDriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.util.BatchTransactionWork;
import org.apache.tinkerpop.gremlin.groovy.GdbGroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@code GdbClient} is constructed from a {@link GdbCluster} and represents a way to send messages to Gremlin Server.
 * This class itself is a base class as there are different implementations that provide differing kinds of
 * functionality.  See the implementations for specifics on their individual usage.
 * <p>
 * The {@code GdbClient} is designed to be re-used and shared across threads.
 */
public abstract class GdbClient {
    public static final int DEFAULT_SCRIPT_EVAL_TIMEOUT = 30000;
    private static final Logger logger = LoggerFactory.getLogger(GdbClient.class);
    // private static final Collection<String> WRITABLE_TEMPLATE = Arrays.asList("addV(", "addE(", "drop(", "property(");
    protected final GdbCluster cluster;
    protected final GdbClient.Settings settings;
    protected final int retryCnt;
    protected volatile boolean initialized;
    private final boolean kickTimeoutReadonlyHost;
    private final int defaultReadonlyTimeout;

    GdbClient(final GdbCluster cluster, final GdbClient.Settings settings) {
        this.cluster = cluster;
        this.settings = settings;
        this.retryCnt = cluster.getRetryCnt();
        this.kickTimeoutReadonlyHost = cluster.getKickTimeoutReadonlyHost();
        this.defaultReadonlyTimeout = cluster.getDefaultReadonlyTimeout();
    }

    /**
     * Makes any initial changes to the builder and returns the constructed {@link RequestMessage}.  Implementers
     * may choose to override this message to append data to the request before sending.  By default, this method
     * will simply return the {@code builder} passed in by the caller.
     * @param builder the request message builder to modify
     * @return the modified request message builder
     */
    public RequestMessage.Builder buildMessage(final RequestMessage.Builder builder) {
        return builder;
    }

    /**
     * Called in the {@link #init} method.
     */
    protected abstract void initializeImplementation();

    /**
     * Chooses a {@link GdbConnection} to write the message to.
     * @param msg the request message to be sent
     * @param isReqRead whether the request is a read-only operation
     * @param deadHosts collection of hosts that are known to be unavailable
     * @return a connection to use for sending the message
     * @throws TimeoutException if a connection cannot be obtained within the timeout period
     * @throws ConnectionException if there is an error establishing a connection
     */
    protected abstract GdbConnection chooseConnection(final RequestMessage msg, boolean isReqRead, Collection<GdbHost> deadHosts) throws TimeoutException, ConnectionException;

    /**
     * Asynchronous close of the {@code GdbClient}.
     * @return a future that completes when the client is closed
     */
    public abstract CompletableFuture<Void> closeAsync();


    /**
     * submit a serial query in one transaction
     *
     * @param <T> the type of client, extending GdbClient
     * @param <U> the type of graph traversal source, extending GraphTraversalSource
     * @param work the batch transaction work to execute
     * @throws RuntimeException if the batch transaction fails
     */
    public <T extends GdbClient, U extends GraphTraversalSource> void batchTransaction(BatchTransactionWork<T, U> work) throws RuntimeException {
        throw new UnsupportedOperationException("batc htransaction unsupport");
    }

    /**
     * Create a new {@code GdbClient} that aliases the specified {@link Graph} or {@link TraversalSource} name on the
     * server to a variable called "g" for the context of the requests made through that {@code GdbClient}.
     *
     * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
     * @return a new GdbClient with the specified alias
     */
    public GdbClient alias(final String graphOrTraversalSource) {
        return alias(makeDefaultAliasMap(graphOrTraversalSource));
    }

    /**
     * Creates a {@code GdbClient} that supplies the specified set of aliases, thus allowing the user to re-name
     * one or more globally defined {@link Graph} or {@link TraversalSource} server bindings for the context of
     * the created {@code GdbClient}.
     * @param aliases a map of aliases where the key is the alias name and the value is the global variable name
     * @return a new GdbClient with the specified aliases
     */
    public GdbClient alias(final Map<String, String> aliases) {
        return new AliasClusteredClient(this, aliases, settings);
    }

    /**
     * Submit a {@link Traversal} to the server for remote execution.Results are returned as {@link Traverser}
     * instances and are therefore bulked, meaning that to properly iterate the contents of the result each
     * {@link Traverser#bulk()} must be examined to determine the number of times that object should be presented in
     * iteration.
     * @param traversal the traversal to submit for execution
     * @return a result set containing the results of the traversal execution
     */
    public GdbResultSet submit(final Traversal traversal) {
        try {
            return submitAsync(traversal).get();
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Submit a {@link Traversal} to the server for remote execution.Results are returned as {@link Traverser}
     * instances and are therefore bulked, meaning that to properly iterate the contents of the result each
     * {@link Traverser#bulk()} must be examined to determine the number of times that object should be presented in
     * iteration.
     *
     * @param traversal   user traversal
     * @param executeTime max wait time
     * @return request's results, empty if no result
     */
    public List<Result> exec(final Traversal traversal, int executeTime) {
        try {
            long start = System.currentTimeMillis();
            long left = executeTime;
            List<Result> results = new ArrayList<>();
            RequestOptions.Builder options = RequestOptions.build().timeout(executeTime);
            GdbResultSet resultSet = submitAsync(traversal, options).get(executeTime, TimeUnit.MILLISECONDS);
            left -= (System.currentTimeMillis() - start);
            while (true) {
                start = System.currentTimeMillis();
                List<Result> list = resultSet.some(64).get(left, TimeUnit.MILLISECONDS);
                left -= (System.currentTimeMillis() - start);
                if (list.size() <= 0) {
                    break;
                } else if (left <= 0) {
                    throw new RuntimeException("timeout - " + String.valueOf(executeTime));
                } else {
                    results.addAll(list);
                }
            }
            return results;
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * An asynchronous version of {@link #submit(Traversal)}. Results are returned as {@link Traverser} instances and
     * are therefore bulked, meaning that to properly iterate the contents of the result each {@link Traverser#bulk()}
     * must be examined to determine the number of times that object should be presented in iteration.
     * @param traversal the traversal to submit for execution
     * @return a future that completes with a result set containing the results of the traversal execution
     */
    public CompletableFuture<GdbResultSet> submitAsync(final Traversal traversal) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a session GdbClient");
    }

    /**
     * An asynchronous version of {@link #submit(Traversal)}. Results are returned as {@link Traverser} instances and
     * are therefore bulked, meaning that to properly iterate the contents of the result each {@link Traverser#bulk()}
     * must be examined to determine the number of times that object should be presented in iteration.
     * @param traversal the traversal to submit for execution
     * @param requestOptions options to configure the request
     * @return a future that completes with a result set containing the results of the traversal execution
     */
    public CompletableFuture<GdbResultSet> submitAsync(final Traversal traversal, RequestOptions.Builder requestOptions) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a session GdbClient");
    }

    /**
     * Submit a {@link Bytecode} to the server for remote execution. Results are returned as {@link Traverser}
     * instances and are therefore bulked, meaning that to properly iterate the contents of the result each
     * {@link Traverser#bulk()} must be examined to determine the number of times that object should be presented in
     * iteration.
     * @param bytecode the bytecode to submit for execution
     * @return a result set containing the results of the bytecode execution
     */
    public GdbResultSet submit(final Bytecode bytecode) {
        try {
            return submitAsync(bytecode).get();
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * A version of {@link #submit(Bytecode)} which provides the ability to set per-request options.
     *
     * @param bytecode request in the form of gremlin {@link Bytecode}
     * @param options  for the request
     * @return a result set containing the results of the bytecode execution
     * @see #submit(Bytecode)
     */
    public GdbResultSet submit(final Bytecode bytecode, final RequestOptions options) {
        try {
            return submitAsync(bytecode, options).get();
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * An asynchronous version of {@link #submit(Traversal)}. Results are returned as {@link Traverser} instances and
     * are therefore bulked, meaning that to properly iterate the contents of the result each {@link Traverser#bulk()}
     * must be examined to determine the number of times that object should be presented in iteration.
     * @param bytecode the bytecode to submit for execution
     * @return a future that completes with a result set containing the results of the bytecode execution
     */
    public CompletableFuture<GdbResultSet> submitAsync(final Bytecode bytecode) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a sessionless GdbClient created with from the alias() method");
    }

    /**
     * A version of {@link #submit(Bytecode)} which provides the ability to set per-request options.
     *
     * @param bytecode request in the form of gremlin {@link Bytecode}
     * @param options  for the request
     * @return a future that completes with a result set containing the results of the bytecode execution
     * @see #submitAsync(Bytecode)
     */
    public CompletableFuture<GdbResultSet> submitAsync(final Bytecode bytecode, final RequestOptions options) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a sessionless GdbClient created with from the alias() method");
    }

    /**
     * Initializes the gdbClient which typically means that a connection is established to the server.  Depending on the
     * implementation and configuration this blocking call may take some time.  This method will be called
     * automatically if it is not called directly and multiple calls will not have effect.
     * @return the initialized GdbClient instance
     */
    public synchronized GdbClient init() {
        if (initialized) {
            return this;
        }

        logger.debug("Initializing gdbClient on cluster [{}]", cluster);

        cluster.init();
        initializeImplementation();

        initialized = true;
        return this;
    }

    /**
     * Submits a Gremlin script to the server and returns a {@link ResultSet} once the write of the request is
     * complete.
     *
     * @param gremlin the gremlin script to execute
     * @return a result set containing the results of the script execution
     */
    public GdbResultSet submit(final String gremlin) {
        return submit(gremlin, RequestOptions.EMPTY);
    }

    /**
     * Submits a Gremlin script and bound parameters to the server and returns a {@link ResultSet} once the write of
     * the request is complete.  If a script is to be executed repeatedly with slightly different arguments, prefer
     * this method to concatenating a Gremlin script from dynamically produced strings and sending it to
     * {@link #submit(String)}.  Parameterized scripts will perform better.
     *
     * @param gremlin    the gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     * @return a result set containing the results of the script execution
     */
    public GdbResultSet submit(final String gremlin, final Map<String, Object> parameters) {
        try {
            return submitAsync(gremlin, parameters).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Submits a Gremlin script and bound parameters to the server and returns a {@link ResultSet} once the write of
     * the request is complete.  If a script is to be executed repeatedly with slightly different arguments, prefer
     * this method to concatenating a Gremlin script from dynamically produced strings and sending it to
     * {@link #submit(String)}.  Parameterized scripts will perform better.
     *
     * @param gremlin     the gremlin script to execute
     * @param parameters  a map of parameters that will be bound to the script on execution
     * @param executeTime max wait time
     * @return request's results, empty if no result
     */
    public List<Result> exec(final String gremlin, final Map<String, Object> parameters, int executeTime) {
        try {
            long start = System.currentTimeMillis();
            long left = executeTime;
            List<Result> results = new ArrayList<>();
            RequestOptions.Builder options = RequestOptions.build().timeout(executeTime);
            GdbResultSet resultSet = submitAsync(gremlin, parameters).get(executeTime, TimeUnit.MILLISECONDS);
            left -= (System.currentTimeMillis() - start);
            while (true) {
                start = System.currentTimeMillis();
                List<Result> list = resultSet.some(64).get(left, TimeUnit.MILLISECONDS);
                left -= (System.currentTimeMillis() - start);
                if (list.size() <= 0) {
                    break;
                } else if (left <= 0) {
                    throw new RuntimeException("timeout - " + String.valueOf(executeTime));
                } else {
                    results.addAll(list);
                }
            }
            return results;
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Submits a Gremlin script to the server and returns a {@link ResultSet} once the write of the request is
     * complete.
     *
     * @param gremlin the gremlin script to execute
     * @param options for the request
     * @return a result set containing the results of the script execution
     */
    public GdbResultSet submit(final String gremlin, final RequestOptions options) {
        try {
            return submitAsync(gremlin, options).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * The asynchronous version of {@link #submit(String)} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin the gremlin script to execute
     * @return a future that completes with a result set containing the results of the script execution
     */
    public CompletableFuture<GdbResultSet> submitAsync(final String gremlin) {
        return submitAsync(gremlin, RequestOptions.build().create());
    }

    /**
     * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin    the gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     * @return a future that completes with a result set containing the results of the script execution
     */
    public CompletableFuture<GdbResultSet> submitAsync(final String gremlin, final Map<String, Object> parameters) {
        final RequestOptions.Builder options = RequestOptions.build();
        if (parameters != null && !parameters.isEmpty()) {
            parameters.forEach(options::addParameter);
        }

        return submitAsync(gremlin, options.create());
    }

    /**
     * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin                the gremlin script to execute
     * @param parameters             a map of parameters that will be bound to the script on execution
     * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
     * @return a future that completes with a result set containing the results of the script execution
     * @deprecated As of release 3.4.0, replaced by {@link #submitAsync(String, RequestOptions)}.
     */
    @Deprecated
    public CompletableFuture<GdbResultSet> submitAsync(final String gremlin, final String graphOrTraversalSource,
                                                       final Map<String, Object> parameters) {
        Map<String, String> aliases = null;
        if (graphOrTraversalSource != null && !graphOrTraversalSource.isEmpty()) {
            aliases = makeDefaultAliasMap(graphOrTraversalSource);
        }

        return submitAsync(gremlin, aliases, parameters);
    }

    /**
     * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin    the gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     * @param aliases    aliases the specified global Gremlin Server variable some other name that then be used in the
     *                   script where the key is the alias name and the value represents the global variable on the
     *                   server
     * @return a future that completes with a result set containing the results of the script execution
     * @deprecated As of release 3.4.0, replaced by {@link #submitAsync(String, RequestOptions)}.
     */
    @Deprecated
    public CompletableFuture<GdbResultSet> submitAsync(final String gremlin, final Map<String, String> aliases,
                                                       final Map<String, Object> parameters) {
        final RequestOptions.Builder options = RequestOptions.build();
        if (aliases != null && !aliases.isEmpty()) {
            aliases.forEach(options::addAlias);
        }

        if (parameters != null && !parameters.isEmpty()) {
            parameters.forEach(options::addParameter);
        }

        options.batchSize(cluster.connectionPoolSettings().resultIterationBatchSize);

        return submitAsync(gremlin, options.create());
    }

    /**
     * The asynchronous version of {@link #submit(String, RequestOptions)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin the gremlin script to execute
     * @param options the options to supply for this request
     * @return a future that completes with a result set containing the results of the script execution
     */
    public CompletableFuture<GdbResultSet> submitAsync(final String gremlin, final RequestOptions options) {
        final int batchSize = options.getBatchSize().orElse(cluster.connectionPoolSettings().resultIterationBatchSize);

        // need to call buildMessage() right away to get gdbClient specific configurations, that way request specific
        // ones can override as needed
        final RequestMessage.Builder request = buildMessage(RequestMessage.build(Tokens.OPS_EVAL))
                .add(Tokens.ARGS_GREMLIN, gremlin)
                .add(Tokens.ARGS_BATCH_SIZE, batchSize);

        // apply settings if they were made available
        options.getTimeout().ifPresent(timeout -> request.add(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, timeout));
        options.getParameters().ifPresent(params -> request.addArg(Tokens.ARGS_BINDINGS, params));
        options.getAliases().ifPresent(aliases -> request.addArg(Tokens.ARGS_ALIASES, aliases));
        options.getOverrideRequestId().ifPresent(request::overrideRequestId);

        return submitAsync(request.create());
    }

    private boolean isRequestReadOnly(Object gremlin) {
        String strGremlin = (gremlin instanceof Bytecode) ? ((Bytecode) gremlin).toString() : (String) gremlin;
        for (int i = 0; i < strGremlin.length(); i++) {
            if (strGremlin.charAt(i) == 'a') {
                if (strGremlin.substring(i).startsWith("addV(") || strGremlin.substring(i).startsWith("addE(")) {
                    return false;
                }
            } else if (strGremlin.charAt(i) == 'd') {
                if (strGremlin.substring(i).startsWith("drop(")) {
                    return false;
                }
            } else if (strGremlin.charAt(i) == 'p') {
                if (strGremlin.substring(i).startsWith("property(")) {
                    return false;
                }
            }
        }
        return true;
    }

    private int RefreshAndGetTimeout(final RequestMessage msg, GdbConnection connection) {
        assert(connection.isReadOnly());

        int timeout = defaultReadonlyTimeout;
        if (timeout <= 0) {
            timeout = (int) msg.getArgs().getOrDefault(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, DEFAULT_SCRIPT_EVAL_TIMEOUT);
        }
        msg.getArgs().put(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, timeout);

        return timeout;
    }

    /**
     * A low-level method that allows the submission of a manually constructed {@link RequestMessage}.
     * may be gremlin / bytecode
     * may be retry at most retryCnt numbers
     * @param msg the request message to submit
     * @return a future that completes with a result set containing the results of the request execution
     */
    public CompletableFuture<GdbResultSet> submitAsync(final RequestMessage msg) {
        if (isClosing()) {
            throw new IllegalStateException("GdbClient has been closed");
        }

        if (!initialized) {
            init();
        }

        int curr_retry_cnt = 0;
        GdbConnection connection = null;
        Collection<GdbHost> deadHosts = new ArrayList<>();

        boolean isReqRead = isRequestReadOnly(msg.getArgs().get(Tokens.ARGS_GREMLIN));
        while (true) {
            try {
                // the connection is returned to the pool once the response has been completed...see GdbConnection.write()
                // the connection may be returned to the pool with the host being marked as "unavailable"
                connection = chooseConnection(msg, isReqRead, deadHosts);

                final CompletableFuture<GdbResultSet> future = new CompletableFuture<>();
                if (cluster.clusterReadMode() && connection.isReadOnly()) {
                    int timeout = RefreshAndGetTimeout(msg, connection);
                    connection.write(msg, future);
                    List<Result> results = future.get().all().get(timeout, TimeUnit.MILLISECONDS);
                    final CompletableFuture<GdbResultSet> future2 = new CompletableFuture<>();
                    future2.complete(new GdbPackResultSet(null, cluster.executor(), null, msg, connection.getHost(), results));
                    return future2;
                } else {
                    connection.write(msg, future);
                    return future;
                }
            } catch (Throwable toe) {
                if (connection != null) {
                    if (connection.isReadOnly()) {
                        if (toe instanceof TimeoutException && kickTimeoutReadonlyHost) {
                            connection.getPool().considerHostUnavailable();
                            System.out.println("kicking out " + connection);
                        }

                        // master as the last server, should not kick off
                        deadHosts.add(connection.getHost());
                    }
                }
                System.out.println("Submitted " + msg + " to " + (null == connection ? "connection not initialized" : connection.toString()) + " exception - " + toe.getMessage() + ", retrying...");
                // there was a timeout borrowing a connection
                if (cluster.clusterReadMode() && (++curr_retry_cnt) <= retryCnt) {
                    logger.warn("Submitted {} to - {}, exception - {}, retrying...", msg, null == connection ? "connection not initialized" : connection.toString(), toe.getMessage());
                    continue;
                }

                throw new RuntimeException(toe);
            } finally {
                if (logger.isDebugEnabled()) {
                    logger.debug("Submitted {} to - {}", msg, null == connection ? "connection not initialized" : connection.toString());
                }
            }
        }
    }

    public abstract boolean isClosing();

    /**
     * Closes the gdbClient by making a synchronous call to {@link #closeAsync()}.
     */
    public void close() {
        closeAsync().join();
    }

    /**
     * Gets the {@link GdbClient.Settings}.
     * @return the settings for this client
     */
    public GdbClient.Settings getSettings() {
        return settings;
    }

    /**
     * Gets the {@link GdbCluster} that spawned this {@code GdbClient}.
     * @return the cluster that created this client
     */
    public GdbCluster getGdbCluster() {
        return cluster;
    }

    protected Map<String, String> makeDefaultAliasMap(final String graphOrTraversalSource) {
        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g", graphOrTraversalSource);
        return aliases;
    }

    /**
     * A {@code GdbClient} implementation that does not operate in a session.  Requests are sent to multiple servers
     * given a {@link LoadBalancingStrategy}.  Transactions are automatically committed
     * (or rolled-back on error) after each request.
     */
    public final static class ClusteredClient extends GdbClient {

        private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
        protected ConcurrentMap<GdbHost, GdbConnectionPool> hostConnectionPools = new ConcurrentHashMap<>();

        ClusteredClient(final GdbCluster cluster, final GdbClient.Settings settings) {
            super(cluster, settings);
        }

        @Override
        public boolean isClosing() {
            return closing.get() != null;
        }

        /**
         * Submits a Gremlin script to the server and returns a {@link ResultSet} once the write of the request is
         * complete.
         *
         * @param gremlin the gremlin script to execute
         * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
         * @return a result set containing the results of the script execution
         */
        public GdbResultSet submit(final String gremlin, final String graphOrTraversalSource) {
            return submit(gremlin, graphOrTraversalSource, null);
        }

        /**
         * Submits a Gremlin script and bound parameters to the server and returns a {@link ResultSet} once the write of
         * the request is complete.  If a script is to be executed repeatedly with slightly different arguments, prefer
         * this method to concatenating a Gremlin script from dynamically produced strings and sending it to
         * {@link #submit(String)}.  Parameterized scripts will perform better.
         *
         * @param gremlin                the gremlin script to execute
         * @param parameters             a map of parameters that will be bound to the script on execution
         * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
         * @return a result set containing the results of the script execution
         */
        public GdbResultSet submit(final String gremlin, final String graphOrTraversalSource, final Map<String, Object> parameters) {
            try {
                return submitAsync(gremlin, graphOrTraversalSource, parameters).get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public GdbClient alias(final String graphOrTraversalSource) {
            final Map<String, String> aliases = new HashMap<>();
            aliases.put("g", graphOrTraversalSource);
            return alias(aliases);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public GdbClient alias(final Map<String, String> aliases) {
            return new AliasClusteredClient(this, aliases, settings);
        }

        /**
         * Uses a {@link LoadBalancingStrategy} to choose the best {@link GdbHost} and then selects the best connection
         * from that host's connection pool.
         */
        @Override
        protected GdbConnection chooseConnection(final RequestMessage msg, boolean isReqRead, Collection<GdbHost> deadHosts) throws TimeoutException, ConnectionException {
            final Iterator<GdbHost> possibleHosts;
            if (msg.optionalArgs(Tokens.ARGS_HOST).isPresent()) {
                // TODO: not sure what should be done if unavailable - select new host and re-submit traversal?
                final GdbHost host = (GdbHost) msg.getArgs().get(Tokens.ARGS_HOST);
                msg.getArgs().remove(Tokens.ARGS_HOST);
                possibleHosts = IteratorUtils.of(host);
            } else {
                possibleHosts = this.cluster.loadBalancingStrategy().select(msg, isReqRead, deadHosts);
            }

            // you can get no possible hosts in more than a few situations. perhaps the servers are just all down.
            // or perhaps the gdbClient is not configured properly (disables ssl when ssl is enabled on the server).
            if (!possibleHosts.hasNext()) {
                throw new TimeoutException("Timed out while waiting for an available host - check the gdbClient configuration and connectivity to the server if this message persists");
            }

            final GdbHost bestHost = possibleHosts.next();
            final GdbConnectionPool pool = hostConnectionPools.get(bestHost);
            return pool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        /**
         * Initializes the connection pools on all hosts.
         */
        @Override
        protected void initializeImplementation() {
            Stream.of(cluster.allMasterHosts(), cluster.allReadonlyHosts()).flatMap(Collection::stream).forEach(host -> {
                try {
                    // hosts that don't initialize connection pools will come up as a dead host
                    hostConnectionPools.put(host, new GdbConnectionPool(host, this));

                    // added a new host to the cluster so let the load-balancer know
                    this.cluster.loadBalancingStrategy().onNew(host);
                } catch (Exception ex) {
                    // catch connection errors and prevent them from failing the creation
                    logger.warn("Could not initialize connection pool for {} - will try later", host);
                }
            });
        }

        /**
         * Closes all the connection pools on all hosts.
         */
        @Override
        public synchronized CompletableFuture<Void> closeAsync() {
            if (closing.get() != null) {
                return closing.get();
            }

            final CompletableFuture[] poolCloseFutures = new CompletableFuture[hostConnectionPools.size()];
            hostConnectionPools.values().stream().map(GdbConnectionPool::closeAsync).collect(Collectors.toList()).toArray(poolCloseFutures);
            closing.set(CompletableFuture.allOf(poolCloseFutures));
            return closing.get();
        }
    }

    /**
     * Uses a {@link ClusteredClient} that rebinds requests to a
     * specified {@link Graph} or {@link TraversalSource} instances on the server-side.
     */
    public static class AliasClusteredClient extends GdbClient {
        final CompletableFuture<Void> close = new CompletableFuture<>();
        private final GdbClient gdbClient;
        private final Map<String, String> aliases = new HashMap<>();

        AliasClusteredClient(final GdbClient gdbClient, final Map<String, String> aliases, final GdbClient.Settings settings) {
            super(gdbClient.cluster, settings);
            this.gdbClient = gdbClient;
            this.aliases.putAll(aliases);
        }

        @Override
        public CompletableFuture<GdbResultSet> submitAsync(final Bytecode bytecode) {
            return submitAsync(bytecode, RequestOptions.EMPTY);
        }

        @Override
        public CompletableFuture<GdbResultSet> submitAsync(final Bytecode bytecode, final RequestOptions options) {
            try {
                // need to call buildMessage() right away to get gdbClient specific configurations, that way request specific
                // ones can override as needed
                final RequestMessage.Builder request = buildMessage(RequestMessage.build(Tokens.OPS_BYTECODE)
                        .processor("traversal")
                        .addArg(Tokens.ARGS_GREMLIN, bytecode));

                // apply settings if they were made available
                options.getBatchSize().ifPresent(batchSize -> request.add(Tokens.ARGS_BATCH_SIZE, batchSize));
                options.getTimeout().ifPresent(timeout -> request.add(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, timeout));

                return submitAsync(request.create());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public CompletableFuture<GdbResultSet> submitAsync(final RequestMessage msg) {
            final RequestMessage.Builder builder = RequestMessage.from(msg);

            // only add aliases which aren't already present. if they are present then they represent request level
            // overrides which should be mucked with
            if (!aliases.isEmpty()) {
                final Map original = (Map) msg.getArgs().getOrDefault(Tokens.ARGS_ALIASES, Collections.emptyMap());
                aliases.forEach((k, v) -> {
                    if (!original.containsKey(k)) {
                        builder.addArg(Tokens.ARGS_ALIASES, aliases);
                    }
                });
            }

            return super.submitAsync(builder.create());
        }

        @Override
        public CompletableFuture<GdbResultSet> submitAsync(final Traversal traversal) {
            return submitAsync(traversal.asAdmin().getBytecode());
        }

        @Override
        public synchronized GdbClient init() {
            if (close.isDone()) {
                throw new IllegalStateException("GdbClient is closed");
            }

            // the underlying gdbClient may not have been init'd
            gdbClient.init();

            return this;
        }

        @Override
        public RequestMessage.Builder buildMessage(final RequestMessage.Builder builder) {
            if (close.isDone()) {
                throw new IllegalStateException("GdbClient is closed");
            }
            if (!aliases.isEmpty()) {
                builder.addArg(Tokens.ARGS_ALIASES, aliases);
            }

            return gdbClient.buildMessage(builder);
        }

        @Override
        protected void initializeImplementation() {
            // no init required
            if (close.isDone()) {
                throw new IllegalStateException("GdbClient is closed");
            }
        }

        /**
         * Delegates to the underlying {@link ClusteredClient}.
         */
        @Override
        protected GdbConnection chooseConnection(final RequestMessage msg, boolean isReqRead, Collection<GdbHost> deadHosts) throws TimeoutException, ConnectionException {
            if (close.isDone()) {
                throw new IllegalStateException("GdbClient is closed");
            }
            return gdbClient.chooseConnection(msg, isReqRead, deadHosts);
        }

        /**
         * Prevents messages from being sent from this {@code GdbClient}. Note that calling this method does not call
         * close on the {@code GdbClient} that created it.
         */
        @Override
        public synchronized CompletableFuture<Void> closeAsync() {
            close.complete(null);
            return close;
        }

        @Override
        public boolean isClosing() {
            return close.isDone();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public GdbClient alias(final Map<String, String> aliases) {
            if (close.isDone()) {
                throw new IllegalStateException("GdbClient is closed");
            }
            return new AliasClusteredClient(gdbClient, aliases, settings);
        }
    }

    /**
     * A {@code GdbClient} implementation that operates in the context of a session.  Requests are sent to a single
     * server, where each request is bound to the same thread with the same set of bindings across requests.
     * Transaction are not automatically committed. It is up the gdbClient to issue commit/rollback commands.
     */
    public final static class SessionedClient extends GdbClient {
        private final String sessionId;
        private final boolean manageTransactions;
        private final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(GdbDriverRemoteConnection.using(this));
        private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
        private GdbConnectionPool connectionPool;

        SessionedClient(final GdbCluster cluster, final GdbClient.Settings settings) {
            super(cluster, settings);
            this.sessionId = settings.getSession().get().sessionId;
            this.manageTransactions = settings.getSession().get().manageTransactions;
        }

        String getSessionId() {
            return sessionId;
        }

        /**
         * Adds the {@link Tokens#ARGS_SESSION} value to every {@link RequestMessage}.
         */
        @Override
        public RequestMessage.Builder buildMessage(final RequestMessage.Builder builder) {
            builder.processor("session");
            builder.addArg(Tokens.ARGS_SESSION, sessionId);
            builder.addArg(Tokens.ARGS_MANAGE_TRANSACTION, manageTransactions);
            return builder;
        }

        /**
         * Since the session is bound to a single host, simply borrow a connection from that pool.
         */
        @Override
        protected GdbConnection chooseConnection(final RequestMessage msg, boolean isReqRead, Collection<GdbHost> deadHosts) throws TimeoutException, ConnectionException {
            return connectionPool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        /**
         * Randomly choose an available {@link GdbHost} to bind the session too and initialize the {@link GdbConnectionPool}.
         */
        @Override
        protected void initializeImplementation() {
            // chooses an available host at random
            final List<GdbHost> hosts = cluster.allMasterHosts()
                    .stream().filter(GdbHost::isAvailable).collect(Collectors.toList());
            if (hosts.isEmpty()) {
                throw new IllegalStateException("No available host in the cluster");
            }
            Collections.shuffle(hosts);
            final GdbHost host = hosts.get(0);
            connectionPool = new GdbConnectionPool(host, this, Optional.of(1), Optional.of(1));
        }

        @Override
        public boolean isClosing() {
            return closing.get() != null;
        }

        /**
         * Close the bound {@link GdbConnectionPool}.
         */
        @Override
        public synchronized CompletableFuture<Void> closeAsync() {
            if (closing.get() != null) {
                return closing.get();
            }

            // the connection pool may not have been initialized if requests weren't sent across it. in those cases
            // we just need to return a pre-completed future
            final CompletableFuture<Void> connectionPoolClose = null == connectionPool ?
                    CompletableFuture.completedFuture(null) : connectionPool.closeAsync();
            closing.set(connectionPoolClose);
            return connectionPoolClose;
        }

        /**
         * run a bath request in one transaction
         *
         * @param <T> the type of client, extending GdbClient
         * @param <U> the type of graph traversal source, extending GraphTraversalSource
         * @param work the batch transaction work to execute
         * @throws RuntimeException if the batch transaction fails
         */
        @Override
        public <T extends GdbClient, U extends GraphTraversalSource> void batchTransaction(BatchTransactionWork<T, U> work) throws RuntimeException {
            try {
                transaction.open(this);
                work.execute((T) this, (U) g);
                transaction.commit(this);
            } catch (Throwable throwable) {
                System.out.println("rollback");
                transaction.rollback(this);
                throw new RuntimeException(throwable);
            }
        }

        /**
         * receive traversal and then change into script
         *
         * @param traversal the traversal to submit for execution
         * @return a result set containing the results of the traversal execution
         */
        @Override
        public GdbResultSet submit(final Traversal traversal) {
            try {

                GdbGroovyTranslator.ParameterizedScriptResult result = GdbGroovyTranslator.of("g").translateParameterization(traversal.asAdmin().getBytecode());
                return this.submit(result.getScript(), result.getBindings());
            } catch (UnsupportedOperationException uoe) {
                throw uoe;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * receive traversal and then change into script
         *
         * @param traversal the traversal to submit for execution
         * @param options   other args
         * @return a future that completes with a result set containing the results of the traversal execution
         */
        @Override
        public CompletableFuture<GdbResultSet> submitAsync(final Traversal traversal, RequestOptions.Builder options) {
            try {
                GdbGroovyTranslator.ParameterizedScriptResult result = GdbGroovyTranslator.of("g").translateParameterization(traversal.asAdmin().getBytecode());
                if (result.getBindings() != null && !result.getBindings().isEmpty()) {
                    result.getBindings().forEach(options::addParameter);
                }
                return this.submitAsync(result.getScript(), options.create());
            } catch (UnsupportedOperationException uoe) {
                throw uoe;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public static class transaction {
            /**
             * open a transaction
             * @param client the client to use for opening the transaction
             */
            public static void open(GdbClient client) {
                String dsl = "g.tx().open()";
                client.submit(dsl, new HashMap<>(0)).all().join();
            }

            public static void commit(GdbClient client) {
                try {
                    String dsl = "g.tx().commit()";
                    client.submit(dsl, new HashMap<>(0)).all().join();
                } catch (Exception ex) {
                    System.out.println("rollback");
                    throw new RuntimeException(ex);
                }
            }

            public static void rollback(GdbClient client) {
                String dsl = "g.tx().rollback()";
                client.submit(dsl, new HashMap<>(0)).all().join();
            }

        }
    }

    /**
     * GdbSettings given to {@link GdbCluster#connect(GdbClient.Settings)} that configures how a {@link GdbClient} will behave.
     */
    public static class Settings {
        private final Optional<GdbClient.SessionSettings> session;

        private Settings(final GdbClient.Settings.Builder builder) {
            this.session = builder.session;
        }

        public static GdbClient.Settings.Builder build() {
            return new GdbClient.Settings.Builder();
        }

        /**
         * Determines if the {@link GdbClient} is to be constructed with a session. If the value is present, then a
         * session is expected.
         * @return an optional containing the session settings if a session is enabled, empty otherwise
         */
        public Optional<GdbClient.SessionSettings> getSession() {
            return session;
        }

        public static class Builder {
            private Optional<GdbClient.SessionSettings> session = Optional.empty();

            private Builder() {
            }

            /**
             * Enables a session. By default this will create a random session name and configure transactions to be
             * unmanaged. This method will override settings provided by calls to the other overloads of
             * {@code useSession}.
             * @param enabled whether to enable a session
             * @return this builder instance
             */
            public GdbClient.Settings.Builder useSession(final boolean enabled) {
                session = enabled ? Optional.of(GdbClient.SessionSettings.build().create()) : Optional.empty();
                return this;
            }

            /**
             * Enables a session. By default this will create a session with the provided name and configure
             * transactions to be unmanaged. This method will override settings provided by calls to the other
             * overloads of {@code useSession}.
             * @param sessionId the session identifier to use
             * @return this builder instance
             */
            public GdbClient.Settings.Builder useSession(final String sessionId) {
                session = sessionId != null && !sessionId.isEmpty() ?
                        Optional.of(GdbClient.SessionSettings.build().sessionId(sessionId).create()) : Optional.empty();
                return this;
            }

            /**
             * Enables a session. This method will override settings provided by calls to the other overloads of
             * {@code useSession}.
             * @param settings the session settings to use
             * @return this builder instance
             */
            public GdbClient.Settings.Builder useSession(final GdbClient.SessionSettings settings) {
                session = Optional.ofNullable(settings);
                return this;
            }

            public GdbClient.Settings create() {
                return new GdbClient.Settings(this);
            }

        }
    }

    /**
     * GdbSettings for a {@link GdbClient} that involve a session.
     */
    public static class SessionSettings {
        private final boolean manageTransactions;
        private final String sessionId;
        private final boolean forceClosed;

        private SessionSettings(final GdbClient.SessionSettings.Builder builder) {
            manageTransactions = builder.manageTransactions;
            sessionId = builder.sessionId;
            forceClosed = builder.forceClosed;
        }

        public static GdbClient.SessionSettings.Builder build() {
            return new GdbClient.SessionSettings.Builder();
        }

        /**
         * If enabled, transactions will be "managed" such that each request will represent a complete transaction.
         * @return true if transactions are managed, false otherwise
         */
        public boolean manageTransactions() {
            return manageTransactions;
        }

        /**
         * Provides the identifier of the session.
         * @return the session identifier
         */
        public String getSessionId() {
            return sessionId;
        }

        /**
         * Determines if the session will be force closed. See {@link GdbClient.SessionSettings.Builder#forceClosed(boolean)} for more details
         * on what that means.
         * @return true if the session will be force closed, false otherwise
         */
        public boolean isForceClosed() {
            return forceClosed;
        }

        public static class Builder {
            private boolean manageTransactions = false;
            private String sessionId = UUID.randomUUID().toString();
            private boolean forceClosed = false;

            private Builder() {
            }

            /**
             * If enabled, transactions will be "managed" such that each request will represent a complete transaction.
             * By default this value is {@code false}.
             * @param manage whether to enable transaction management
             * @return this builder instance
             */
            public GdbClient.SessionSettings.Builder manageTransactions(final boolean manage) {
                manageTransactions = manage;
                return this;
            }

            /**
             * Provides the identifier of the session. This value cannot be null or empty. By default it is set to
             * a random {@code UUID}.
             * @param sessionId the session identifier to use
             * @return this builder instance
             */
            public GdbClient.SessionSettings.Builder sessionId(final String sessionId) {
                if (null == sessionId || sessionId.isEmpty()) {
                    throw new IllegalArgumentException("sessionId cannot be null or empty");
                }
                this.sessionId = sessionId;
                return this;
            }

            /**
             * Determines if the session should be force closed when the gdbClient is closed. Force closing will not
             * attempt to close open transactions from existing running jobs and leave it to the underlying graph to
             * decided how to proceed with those orphaned transactions. Setting this to {@code true} tends to lead to
             * faster close operation which can be desirable if Gremlin Server has a long session timeout and a long
             * script evaluation timeout as attempts to close long run jobs can occur more rapidly. By default, this
             * value is {@code false}.
             * @param forced whether to force close the session
             * @return this builder instance
             */
            public GdbClient.SessionSettings.Builder forceClosed(final boolean forced) {
                this.forceClosed = forced;
                return this;
            }

            public GdbClient.SessionSettings create() {
                return new GdbClient.SessionSettings(this);
            }
        }
    }

}
