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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.GdbClient;
import org.apache.tinkerpop.gremlin.driver.GdbCluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeUtil;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.tinkerpop.gremlin.driver.Tokens.ARGS_BATCH_SIZE;
import static org.apache.tinkerpop.gremlin.driver.Tokens.ARGS_SCRIPT_EVAL_TIMEOUT;
import static org.apache.tinkerpop.gremlin.driver.Tokens.REQUEST_ID;

/**
 * A {@link RemoteConnection} implementation for Gremlin Server. Each {@code DriverServerConnection} is bound to one
 * graph instance hosted in Gremlin Server.
 */
public class GdbDriverRemoteConnection implements RemoteConnection {

    public static final String GREMLIN_REMOTE_DRIVER_CLUSTERFILE = GREMLIN_REMOTE + "driver.clusterFile";
    public static final String GREMLIN_REMOTE_DRIVER_SOURCENAME = GREMLIN_REMOTE + "driver.sourceName";

    private static final String DEFAULT_TRAVERSAL_SOURCE = "g";

    private final GdbClient client;
    private final boolean tryCloseGdbCluster;
    private final boolean tryCloseGdbClient;
    private final String remoteTraversalSourceName;
    private transient Optional<Configuration> conf = Optional.empty();

    private final boolean attachElements;

    public GdbDriverRemoteConnection(final Configuration conf) {
        final boolean hasGdbClusterConf = IteratorUtils.anyMatch(conf.getKeys(), k -> k.startsWith("clusterConfiguration"));
        if (conf.containsKey(GREMLIN_REMOTE_DRIVER_CLUSTERFILE) && hasGdbClusterConf) {
            throw new IllegalStateException(String.format("A configuration should not contain both '%s' and 'clusterConfiguration'", GREMLIN_REMOTE_DRIVER_CLUSTERFILE));
        }

        remoteTraversalSourceName = conf.getString(GREMLIN_REMOTE_DRIVER_SOURCENAME, DEFAULT_TRAVERSAL_SOURCE);

        try {
            final GdbCluster cluster;
            if (!conf.containsKey(GREMLIN_REMOTE_DRIVER_CLUSTERFILE) && !hasGdbClusterConf) {
                cluster = GdbCluster.open();
            } else {
                cluster = conf.containsKey(GREMLIN_REMOTE_DRIVER_CLUSTERFILE) ?
                        GdbCluster.open(conf.getString(GREMLIN_REMOTE_DRIVER_CLUSTERFILE)) : GdbCluster.open(conf.subset("clusterConfiguration"));
            }

            client = cluster.connect(GdbClient.Settings.build().create()).alias(remoteTraversalSourceName);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        attachElements = false;

        tryCloseGdbCluster = true;
        tryCloseGdbClient = true;
        this.conf = Optional.of(conf);
    }

    private GdbDriverRemoteConnection(final GdbCluster cluster, final boolean tryCloseGdbCluster, final String remoteTraversalSourceName) {
        client = cluster.connect(GdbClient.Settings.build().create()).alias(remoteTraversalSourceName);
        this.remoteTraversalSourceName = remoteTraversalSourceName;
        this.tryCloseGdbCluster = tryCloseGdbCluster;
        attachElements = false;
        tryCloseGdbClient = true;
    }

    /**
     * This constructor is largely just for unit testing purposes and should not typically be used externally.
     */
    GdbDriverRemoteConnection(final GdbCluster cluster, final Configuration conf) {
        remoteTraversalSourceName = conf.getString(GREMLIN_REMOTE_DRIVER_SOURCENAME, DEFAULT_TRAVERSAL_SOURCE);

        attachElements = conf.containsKey(GREMLIN_REMOTE + "attachment");

        client = cluster.connect(GdbClient.Settings.build().create()).alias(remoteTraversalSourceName);
        tryCloseGdbCluster = false;
        tryCloseGdbClient = true;
        this.conf = Optional.of(conf);
    }

    private GdbDriverRemoteConnection(final GdbClient client, final String remoteTraversalSourceName) {
        this.client = client.alias(remoteTraversalSourceName);
        this.remoteTraversalSourceName = remoteTraversalSourceName;
        this.tryCloseGdbCluster = false;
        attachElements = false;
        tryCloseGdbClient = false;
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} using an existing {@link GdbClient} object. The {@link GdbClient} will not
     * be closed on calls to {@link #close()}.
     *
     * @param client the existing GdbClient to use
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final GdbClient client) {
        return using(client, DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} using an existing {@link GdbClient} object. The {@link GdbClient} will not
     * be closed on calls to {@link #close()}.
     *
     * @param client the existing GdbClient to use
     * @param remoteTraversalSourceName the name of the remote traversal source to bind to
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final GdbClient client, final String remoteTraversalSourceName) {
        return new GdbDriverRemoteConnection(client, remoteTraversalSourceName);
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} using a new {@link GdbCluster} instance created from the supplied host
     * and port and binds it to a remote {@link GraphTraversalSource} named "g". When {@link #close()} is called,
     * this new {@link GdbCluster} is also closed. By default, this method will bind the {@link RemoteConnection} to a
     * graph on the server named "graph".
     *
     * @param host the host to connect to
     * @param port the port to connect to
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final String host, final int port) {
        return using(GdbCluster.build(host).port(port).create(), DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} using a new {@link GdbCluster} instance created from the supplied host
     * port and aliases it to the specified remote {@link GraphTraversalSource}. When {@link #close()} is called, this
     * new {@link GdbCluster} is also closed. By default, this method will bind the {@link RemoteConnection} to the
     * specified graph traversal source name.
     *
     * @param host the host to connect to
     * @param port the port to connect to
     * @param remoteTraversalSourceName the name of the remote traversal source to bind to
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final String host, final int port, final String remoteTraversalSourceName) {
        return using(GdbCluster.build(host).port(port).create(), remoteTraversalSourceName);
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} from an existing {@link GdbCluster} instance. When {@link #close()} is
     * called, the {@link GdbCluster} is left open for the caller to close. By default, this method will bind the
     * {@link RemoteConnection} to a graph on the server named "graph".
     *
     * @param cluster the existing GdbCluster to use
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final GdbCluster cluster) {
        return using(cluster, DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} from an existing {@link GdbCluster} instance. When {@link #close()} is
     * called, the {@link GdbCluster} is left open for the caller to close.
     *
     * @param cluster the existing GdbCluster to use
     * @param remoteTraversalSourceName the name of the remote traversal source to bind to
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final GdbCluster cluster, final String remoteTraversalSourceName) {
        return new GdbDriverRemoteConnection(cluster, false, remoteTraversalSourceName);
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} using a new {@link GdbCluster} instance created from the supplied
     * configuration file. When {@link #close()} is called, this new {@link GdbCluster} is also closed. By default,
     * this method will bind the {@link RemoteConnection} to a graph on the server named "graph".
     *
     * @param clusterConfFile the path to the cluster configuration file
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final String clusterConfFile) {
        return using(clusterConfFile, DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} using a new {@link GdbCluster} instance created from the supplied
     * configuration file. When {@link #close()} is called, this new {@link GdbCluster} is also closed.
     *
     * @param clusterConfFile the path to the cluster configuration file
     * @param remoteTraversalSourceName the name of the remote traversal source to bind to
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final String clusterConfFile, final String remoteTraversalSourceName) {
        try {
            return new GdbDriverRemoteConnection(GdbCluster.open(clusterConfFile), true, remoteTraversalSourceName);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Creates a {@link GdbDriverRemoteConnection} using an Apache {@code Configuration} object. The
     * {@code Configuration} object should contain one of two required keys, either: {@code clusterConfigurationFile}
     * or {@code clusterConfiguration}. The {@code clusterConfigurationFile} key is a pointer to a file location
     * containing a configuration for a {@link GdbCluster}. The {@code clusterConfiguration} should contain the actual
     * contents of a configuration that would be used by a {@link GdbCluster}.  This {@code configuration} may also
     * contain the optional, but likely necessary, {@code remoteTraversalSourceName} which tells the
     * {@code DriverServerConnection} which graph on the server to bind to.
     *
     * @param conf the configuration object containing cluster configuration
     * @return a new GdbDriverRemoteConnection instance
     */
    public static GdbDriverRemoteConnection using(final Configuration conf) {
        if (conf.containsKey("clusterConfigurationFile") && conf.containsKey("clusterConfiguration")) {
            throw new IllegalStateException("A configuration should not contain both 'clusterConfigurationFile' and 'clusterConfiguration'");
        }

        if (!conf.containsKey("clusterConfigurationFile") && !conf.containsKey("clusterConfiguration")) {
            throw new IllegalStateException("A configuration must contain either 'clusterConfigurationFile' and 'clusterConfiguration'");
        }

        final String remoteTraversalSourceName = conf.getString(DEFAULT_TRAVERSAL_SOURCE, DEFAULT_TRAVERSAL_SOURCE);
        if (conf.containsKey("clusterConfigurationFile")) {
            return using(conf.getString("clusterConfigurationFile"), remoteTraversalSourceName);
        } else {
            return using(GdbCluster.open(conf.subset("clusterConfiguration")), remoteTraversalSourceName);
        }
    }

    /**
     * @param bytecode the bytecode to submit
     * @return a CompletableFuture containing the remote traversal
     * @throws RemoteConnectionException if there is an error submitting the bytecode
     */
    @Override
    public <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(final Bytecode bytecode) throws RemoteConnectionException {
        try {
            return client.submitAsync(bytecode, getRequestOptions(bytecode)).thenApply(rs -> new GdbDriverRemoteTraversal<>(rs, client, attachElements, conf));
        } catch (Exception ex) {
            throw new RemoteConnectionException(ex);
        }
    }

    /**
     * @param bytecode the bytecode to extract options from
     * @return the request options extracted from the bytecode
     */
    protected static RequestOptions getRequestOptions(final Bytecode bytecode) {
        final Iterator<OptionsStrategy> itty = BytecodeUtil.findStrategies(bytecode, OptionsStrategy.class);
        final RequestOptions.Builder builder = RequestOptions.build();
        while (itty.hasNext()) {
            final OptionsStrategy optionsStrategy = itty.next();
            final Map<String,Object> options = optionsStrategy.getOptions();
            if (options.containsKey(ARGS_SCRIPT_EVAL_TIMEOUT)) {
                builder.timeout((long) options.get(ARGS_SCRIPT_EVAL_TIMEOUT));
            } else if (options.containsKey(REQUEST_ID)) {
                builder.overrideRequestId((UUID) options.get(REQUEST_ID));
            } else if (options.containsKey(ARGS_BATCH_SIZE)) {
                builder.batchSize((int) options.get(ARGS_BATCH_SIZE));
            }
        }
        return builder.create();
    }

    /**
     * @throws Exception if there is an error closing the connection
     */
    @Override
    public void close() throws Exception {
        try {
            if (tryCloseGdbClient) {
                client.close();
            }
        } catch (Exception ex) {
            throw new RemoteConnectionException(ex);
        } finally {
            if (tryCloseGdbCluster) {
                client.getGdbCluster().close();
            }
        }
    }

    /**
     * @return a string representation of this connection
     */
    @Override
    public String toString() {
        return "DriverServerConnection-" + client.getGdbCluster() + " [graph=" + remoteTraversalSourceName + "]";
    }
}
