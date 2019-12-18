/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.gdb.java.client;


import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.driver.GdbClient;
import org.apache.tinkerpop.gremlin.driver.GdbCluster;
import org.apache.tinkerpop.gremlin.driver.GdbResultSet;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.GdbDriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Demo implements  AutoCloseable {
    private ThreadLocal<GdbClient> client = ThreadLocal.withInitial(() -> null);
    GdbCluster cluster = null;
    /**
     * GdbClient init
     * @param yaml server yaml config
     * @param session if session is true, the GdbClient is {@link GdbClient.SessionedClient}, Requests are sent to a single
     *                  server, where each request is bound to the same thread with the same set of bindings across requests.
     *                  Transaction are not automatically committed. It is up the client to issue commit/rollback commands.
     *                else if session is false, the GdbClient is {@link GdbClient.ClusteredClient},Requests are sent to multiple servers
     *                  given a {@link org.apache.tinkerpop.gremlin.driver.LoadBalancingStrategy}.  Transactions are automatically committed
     *                  (or rolled-back on error) after each request.
     */
    private Demo(String yaml, boolean session) {
        try {
            String sessionId = UUID.randomUUID().toString();

            /// Case 1: based on server yaml
            cluster = GdbCluster.build(new File(yaml)).create();

            /// Case 2: of course you can  do some special init
            //  Cluster.Builder builder = Cluster.build()
            //      .addContactPoint("127.0.0.1")
            //      .port(8182)
            //      .credentials("userName","userPassword")
            //      .serializer(new GraphBinaryMessageSerializerV1());
            //  Cluster cluster = builder.create();
            GdbClient c = session ? cluster.connect(sessionId) : cluster.connect();
            c.init();
            client.set(c);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * send request to gdb server, then wait to get all results
     * @param script Parameterized request
     * @param parameters Parameterized arguments
     * @return request's results, empty if no result
     */
    private List<Result> submit(String script, Map<String, Object> parameters) {
        List<Result> results = new ArrayList<>();
        int executeTime = 30000;
        long start = System.currentTimeMillis();
        try {
            RequestOptions.Builder options = RequestOptions.build().timeout(executeTime);
            if (parameters != null && !parameters.isEmpty()) {
                parameters.forEach(options::addParameter);
            }

            GdbResultSet resultSet = client.get().submitAsync(script, options.create()).get(executeTime,TimeUnit.MILLISECONDS);
            while (true) {
                List<Result> list = resultSet.some(64).get(executeTime, TimeUnit.MILLISECONDS);
                if (list.size() > 0) {
                    results.addAll(list);
                } else {
                    break;
                }
            }
            return results;
        } catch (Exception exception) {
            throw new RuntimeException("submit exception - ",exception);
        } finally {
            System.out.println(" Result -  " + script + " result size - " + results.size() + " cost - " +
                    String.valueOf(System.currentTimeMillis() - start));
        }
    }

    /**
     * release client resource
     */
    @Override
    public void close() {
        client.get().close();
        cluster.close();
    }

    static class TestCase {
        private static final int DEFAULT_TIMEOUT_MILLSECOND = 30000;
        /**
         * String-based Gremlin scripts, every request is completely encapsulated within a single transaction
         * @param yaml gdb server configuration
         * @param vertexLabel vertex's label
         * @param edgeLabel edge's label
         * @param vertexStart test start vertex
         * @param vertexEnd test end vertex
         */
        public static void scriptSessionLessTest(String yaml,String vertexLabel,String edgeLabel, String vertexStart,String vertexEnd) {
            // init Clustered GdbClient
            boolean session = false;
            Demo demo = new Demo(yaml, session);
            GdbClient gdbClient = demo.client.get();
            try {
                // init parameters
                Map<String, Object> parameters = new HashMap();
                parameters.put("vertexStart", vertexStart);
                parameters.put("vertexEnd", vertexEnd);
                parameters.put("vertexLabel", vertexLabel);
                parameters.put("edgeLabel", edgeLabel);

                // user script here, ever request is a single transaction
                {
                    // drop all the vertex / edge
                    String dsl = "g.V().drop()";
                    gdbClient.exec(dsl, new HashMap<>(), DEFAULT_TIMEOUT_MILLSECOND);

                    dsl = "g.addV(vertexLabel).property(T.id, vertexStart)";
                    gdbClient.exec(dsl, parameters, DEFAULT_TIMEOUT_MILLSECOND);

                    // if you do not care about result
                    dsl = "g.addV(vertexLabel).property(T.id, vertexEnd)";
                    gdbClient.submitAsync(dsl, parameters).get().all().get(DEFAULT_TIMEOUT_MILLSECOND,TimeUnit.MILLISECONDS);

                    // if you do not care about result
                    dsl = "g.addE(edgeLabel).from(V(vertexStart)).to(V(vertexEnd))";
                    gdbClient.exec(dsl, parameters,DEFAULT_TIMEOUT_MILLSECOND).forEach(System.out::println);

                    // if you care about result
                    dsl = "g.V().limit(100).properties()";
                    gdbClient.submit(dsl, parameters).forEach(result -> {
                        if (result.getObject() instanceof ReferenceVertex) {
                            ReferenceVertex vertex = (ReferenceVertex) result.getObject();
                            System.out.println("ReferenceVertex id - " + vertex.id() + " label - " + vertex.label() + " properties - ");
                            vertex.properties().forEachRemaining(p -> System.out.println(p));
                        } else if (result.getObject() instanceof DetachedVertex) {
                            System.out.println("DetachedVertex id - " + result.getVertex().id() + " label - " + result.getVertex().label() + " properties - ");
                            result.getVertex().properties().forEachRemaining(p -> System.out.println(p));
                        } else {
                            System.out.println("DefaultResult - " + result);
                        }
                    });
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            } finally {
                // close Clustered GdbClient
                demo.close();
            }
        }

        /**
         * String-based Gremlin scripts, with sessions, the user is in complete control of the start and end of the transaction
         * @param yaml gdb server configuration
         * @param vertexLabel vertex's label
         * @param edgeLabel edge's label
         * @param vertexStart test start vertex
         * @param vertexEnd test end vertex
         */
        public static void scriptSessionTest(String yaml,String vertexLabel,String edgeLabel, String vertexStart,String vertexEnd) {
            // init Session GdbClient
            boolean session = true;

            Demo demo = new Demo(yaml, session);
            GdbClient txClient = demo.client.get();
            try {
                // init parameters
                Map<String, Object> parameters = new HashMap();
                parameters.put("vertexStart", vertexStart);
                parameters.put("vertexEnd", vertexEnd);
                parameters.put("vertexLabel", vertexLabel);
                parameters.put("edgeLabel", edgeLabel);

                txClient.batchTransaction((tx, g) -> {
                    // user batch script here
                    // 1. you should alway get result
                    String dsl = "g.V().drop()";
                    tx.exec(dsl, parameters, DEFAULT_TIMEOUT_MILLSECOND);

                    // 2. you should alway get result
                    dsl = "g.addV(vertexLabel).property(T.id, vertexStart)";
                    tx.exec(dsl, parameters, DEFAULT_TIMEOUT_MILLSECOND);

                    // 3. if you care about result
                    try {
                        dsl = "g.addV(vertexLabel).property(T.id, vertexEnd)";
                        List<Result> results = tx.submit(dsl, parameters).all().get(30000, TimeUnit.MILLISECONDS);
                        results.forEach(result -> {
                            if (result.getObject() instanceof ReferenceVertex) {
                                ReferenceVertex vertex = (ReferenceVertex) result.getObject();
                                System.out.println("ReferenceVertex id - " + vertex.id() + " label - " + vertex.label() + " properties - ");
                                vertex.properties().forEachRemaining(p -> System.out.println(p));
                            } else if (result.getObject() instanceof DetachedVertex) {
                                System.out.println("DetachedVertex id - " + result.getVertex().id() + " label - " + result.getVertex().label() + " properties - ");
                                result.getVertex().properties().forEachRemaining(p -> System.out.println(p));
                            } else {
                                System.out.println("DefaultResult - " + result);
                            }
                        });
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }

                    dsl = "g.addE(edgeLabel).from(V(vertexEnd)).to(V(vertexStart))";
                    tx.exec(dsl, parameters, DEFAULT_TIMEOUT_MILLSECOND);

                    dsl = "g.V().limit(100).properties()";
                    tx.exec(dsl, parameters, DEFAULT_TIMEOUT_MILLSECOND).forEach(t -> System.out.println(t));
                });
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                // close Session GdbClient
                demo.close();
            }
        }

        /**
         * Bytecode-based Gremlin traversals, every request is completely encapsulated within a single transaction
         *  You should notice when use bytecode, the last step should be Terminal Steps, please refer:
         *  (http://tinkerpop.apache.org/docs/current/reference/#terminal-steps)
         * @param yaml gdb server configuration
         * @param vertexLabel vertex's label
         * @param edgeLabel edge's label
         * @param vertexStart test start vertex
         * @param vertexEnd test end vertex
         */
        public static void bytecodeSessionLessTest(String yaml, String vertexLabel, String edgeLabel, String vertexStart, String vertexEnd) {
            // init Clustered GdbClient
            boolean session = false;
            Demo demo = new Demo(yaml, session);
            try {
                GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(GdbDriverRemoteConnection.using(demo.client.get()));

                // user script here, ever request is a single transaction
                {
                    // drop all the vertex / edge
                    g.V().drop().iterate();

                    g.addV(vertexLabel).property(T.id, vertexStart).property("name", "marko").next();
                    g.addV(vertexLabel).property(T.id, vertexEnd).property("name", "stephen").next().properties().forEachRemaining(System.out::println);
                    GraphTraversal<?, Vertex> startVertex = g.V(vertexStart);
                    g.addE(edgeLabel).from(startVertex).to(g.V(vertexEnd)).next();
                    g.addE(edgeLabel).from(__.V(vertexEnd)).to(startVertex).next();
                }
            } catch (Exception e) {
                throw  new RuntimeException("exception - ",e);
            } finally {
                // close Clustered GdbClient
                demo.close();
            }
        }

        /**
         * Bytecode-based Gremlin traversals, with sessions, the user is in complete control of the start and end of the transaction
         * @param yaml gdb server configuration
         * @param vertexLabel vertex's label
         * @param edgeLabel edge's label
         * @param vertexStart test start vertex
         * @param vertexEnd test end vertex
         */
        public static void bytecodeSessionTest(String yaml, String vertexLabel, String edgeLabel, String vertexStart, String vertexEnd) {
            // init Session GdbClient
            boolean session = true;
            Demo demo = new Demo(yaml, session);
            GdbClient txClient = demo.client.get();
            try {
                txClient.batchTransaction((tx, g) -> {
                    try {
                        // user batch script here
//                        tx.exec(g.V().has("name", P.eq("vadas").or(TextP.containing("m"))), DEFAULT_TIMEOUT_MILLSECOND).forEach(t -> System.out.println(t));
//                        GraphTraversal<?, Vertex> startVertex = g.V(vertexStart);

                        tx.exec(g.V().drop(), DEFAULT_TIMEOUT_MILLSECOND);

                        tx.exec(g.addV(vertexLabel).property(T.id, vertexStart).property("name", "vadas"), DEFAULT_TIMEOUT_MILLSECOND);

                        // with timeout
                        long executeTime = 10000;
                        RequestOptions.Builder options = RequestOptions.build().timeout(executeTime);
                        GdbResultSet results = tx.submitAsync(g.V().count(), options).get();

                        List<Result> sum = results.some(1).get();
                        if (sum.size() > 0) {
                            System.out.println("sum - " + sum.get(0).getDouble());
                        } else {
                            System.out.println("sum - " + "empty");
                        }

                        //List<Result> results = tx.submit(g.addV(vertexLabel).property(T.id, vertexEnd).property("name", "josh")).all().get(30000, TimeUnit.MILLISECONDS);
                        results.forEach(result -> {
                            if (result.getObject() instanceof ReferenceVertex) {
                                ReferenceVertex vertex = (ReferenceVertex) result.getObject();
                                System.out.println("ReferenceVertex id - " + vertex.id() + " label - " + vertex.label() + " properties - ");
                                vertex.properties().forEachRemaining(p -> System.out.println(p));
                            } else if (result.getObject() instanceof DetachedVertex) {
                                System.out.println("DetachedVertex id - " + result.getVertex().id() + " label - " + result.getVertex().label() + " properties - ");
                                result.getVertex().properties().forEachRemaining(p -> System.out.println(p));
                            } else {
                                System.out.println("DefaultResult - " + result);
                            }
                        });

                        tx.exec(g.addE(edgeLabel).from(vertexStart).to(g.V(vertexEnd)), DEFAULT_TIMEOUT_MILLSECOND);
                    } catch (Exception ex) {
                        throw  new RuntimeException(ex);
                    }
                });
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                // close Session GdbClient
                demo.close();
            }
        }


        /**
         * Bytecode-based Gremlin traversals, with sessions, the user is in complete control of the start and end of the transaction
         * @param yaml gdb server configuration
         * @param vertexLabel vertex's label
         * @param edgeLabel edge's label
         * @param vertexStart test start vertex
         * @param vertexEnd test end vertex
         * @param threadCount thread count
         */
        public static void multiThreadBatchTest(String yaml, String vertexLabel, String edgeLabel, String vertexStart, String vertexEnd, int threadCount) {
            Demo demo = new Demo(yaml, false);
            GdbClient txClient = demo.client.get();
            txClient.exec("g.V().drop()", new HashMap<>(), DEFAULT_TIMEOUT_MILLSECOND);

            final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
            ThreadFactory namedThreadFactory = new BasicThreadFactory.Builder().namingPattern("thread-%d").build();
            ExecutorService singleThreadPool = new ThreadPoolExecutor(threadCount, threadCount,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(16), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
            for(int i = 0; i < threadCount; i++) {
                singleThreadPool.submit(() -> {
                    try {
                        Demo.TestCase.bytecodeSessionTest(yaml, vertexLabel, edgeLabel, vertexStart + "-" + Thread.currentThread().getName(),
                            vertexEnd + "-" + Thread.currentThread().getName());
                        barrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            try {
                barrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            singleThreadPool.shutdown();
            demo.close();
        }
    }

    public static void main( String[] args ) {
        if (args.length != 1) {
            throw new IllegalArgumentException("gdb-remote.yaml should not be empty");
        }
        String yaml = args[0];

        String vertexLabel = "person";
        String edgeLabel = "knows";
        String vertexStart = "gdb-start-vertex";
        String vertexEnd = "gdb-end-vertex";
        try {
            /**
             * Case 1: script sessionless
             */
            Demo.TestCase.scriptSessionLessTest(yaml, vertexLabel, edgeLabel, vertexStart, vertexEnd);

            /**
             * Case 2: script in-session
             */
            Demo.TestCase.scriptSessionTest(yaml, vertexLabel, edgeLabel, vertexStart, vertexEnd);

            /**
             * Case 3: bytecode sessionless
             */
            Demo.TestCase.bytecodeSessionLessTest(yaml, vertexLabel, edgeLabel, vertexStart, vertexEnd);

            /**
             * Case 4: bytecode in-session
             */
            Demo.TestCase.bytecodeSessionTest(yaml, vertexLabel, edgeLabel, vertexStart, vertexEnd);

            /**
             * Case 5: multiThread thread
             */
            Demo.TestCase.multiThreadBatchTest(yaml, vertexLabel, edgeLabel, vertexStart, vertexEnd, 10);

            System.exit(0);
        } catch (Throwable e) {
            throw  new RuntimeException(e);
        }
    }
}
