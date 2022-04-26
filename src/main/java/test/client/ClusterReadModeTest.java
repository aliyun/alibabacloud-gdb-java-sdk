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
package test.client;

import org.apache.tinkerpop.gremlin.driver.GdbClient;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Arrays;
import java.util.List;


public class ClusterReadModeTest {

    public static Function4<InteractionMethodTest.TestCase, String, Integer, Long> scriptRunnable = (client, uyaml, i, uperiod) -> {
        client.simpleRequest(uyaml, getScriptDsls().get(i % getScriptDsls().size()), uperiod);
    };

    public static Function4<InteractionMethodTest.TestCase, String, Integer, Long> bytecodeRunnable = (client, uyaml, i, uperiod) -> {
        client.simpleByteCodeRequest(uyaml, (uclient, g) -> {
            try {
                long start = System.currentTimeMillis();
                for (long end = start; end - start <= 1000 * uperiod; end = System.currentTimeMillis()) {
                    // read request
                    getBytecodeDsls(g).get(i % getScriptDsls().size()).toList().forEach(result -> {
                        InteractionMethodTest.show(System.out::print, "TimeMs: ", System.currentTimeMillis(), ", DefaultResult: ", result, "\n");
                    });
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    };

    static Function4<InteractionMethodTest.TestCase, String, Integer, Long> scriptSessionRunable = (client, uyaml, i, uperiod) -> {
        client.simpleSessionRequest(uyaml, (uclient, g) -> {
            long start = System.currentTimeMillis();
            for (long end = start; end - start <= 1000 * uperiod; end = System.currentTimeMillis()) {
                uclient.batchTransaction((tx, gg) -> {
                    try {
                        tx.submit(getScriptDsls().get(i % getScriptDsls().size())).forEach(result -> {
                            InteractionMethodTest.show(System.out::print, "TimeMs: ", System.currentTimeMillis(), ", DefaultResult: ", result, "\n");
                        });
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
                Thread.sleep(100);
            }
        });
    };

    static Function4<InteractionMethodTest.TestCase, String, Integer, Long> bytecodeSessionRunable = (client, uyaml, i, uperiod) -> {
        client.simpleSessionRequest(uyaml, (uclient, g) -> {
            long start = System.currentTimeMillis();
            for (long end = start; end - start <= 1000 * uperiod; end = System.currentTimeMillis()) {
                uclient.batchTransaction((tx, gg) -> {
                    try {
                        tx.exec(getBytecodeDsls(gg).get(i % getScriptDsls().size()), GdbClient.DEFAULT_SCRIPT_EVAL_TIMEOUT).forEach(result -> {
                            InteractionMethodTest.show(System.out::print, "TimeMs: ", System.currentTimeMillis(), ", DefaultResult: ", result, "\n");
                        });
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
                Thread.sleep(100);
            }
        });
    };

    static List<String> getScriptDsls() {
        String readDsl1 = "g.V().limit(1000).count()";
        String readDsl2 = "g.V().limit(1000).valueMap(true)";
        String writeDsl1 = "g.V('m1099511649633').property('browserUsed', 'Chrome')";
        String writeDsl2 = "g.V('m1099511649633').properties('browserUsed-no').drop()";
//        String writeDsl3 = "g.addV('no-label').property(T.id, 'm1099511649633')";
        return Arrays.asList(readDsl1, readDsl2, writeDsl1, writeDsl2, writeDsl2);
    }

    static  <G extends GraphTraversalSource> List<GraphTraversal> getBytecodeDsls(G g) {
        // read request
        GraphTraversal readDsl1 = g.V().limit(1000).count();
        GraphTraversal  readDsl2 = g.V().limit(1000).valueMap(true);
        // write request, change property, may conflict
        GraphTraversal writeDsl1 = g.V("m1099511649633").property("browserUsed", "Chrome");
        // write request, drop, nothing happend
        GraphTraversal writeDsl2 = g.V("m1099511649633-no").drop();
        // write request, addVertex, conflict
        GraphTraversal writeDsl3 = g.addV("m1099511649633-no").properties(T.id.getAccessor(), "m1099511649633");
        // write request, addEdge
        GraphTraversal writeDsl4 = g.V("m1099511649633-no").addE("src/main/test").to("m1099511649633-noop").properties(T.id.getAccessor(), "edgeid");
        return Arrays.asList(readDsl1, readDsl2, writeDsl1, writeDsl2, writeDsl3, writeDsl4);
    }

    public static void main(String[] args) {
        long period = 300;
        int thread_number = 1;
        if (args.length < 1) {
            throw new IllegalArgumentException("gdb-remote.yaml should not be empty, format: yaml [period/s]");
        } else if (args.length >= 2) {
            period = Long.valueOf(args[1]);
        }
        String yaml = args[0];
        System.out.println("yaml: " + yaml + ", period:" + period);

        InteractionMethodTest.TestCase testCase = new InteractionMethodTest.TestCase();

        try {
            Thread[] threads = new Thread[thread_number];
            for (int i = 0; i < threads.length; i++) {
                long finalPeriod = period;
                int finalI = i;
                threads[i] = new Thread(() -> {
                    // script test
                    // scriptRunnable.apply(testCase, yaml, finalI, finalPeriod);

                    // bytecode test, bytecode 再server端(翻译)执行
                    bytecodeRunnable.apply(testCase, yaml, finalI, finalPeriod);

                    // script session test, just send to master
                    // scriptSessionRunable.apply(testCase, yaml, finalI, finalPeriod);

                    // script session test, just send to master, 再client端翻译，server端执行
                    // bytecodeSessionRunable.apply(testCase, yaml, finalI, finalPeriod);
                });
                threads[i].start();
            }

            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            System.exit(0);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    interface Function4<One, Two, Three, Four> {
        public void apply(One one, Two two, Three three, Four four);
    }

    @FunctionalInterface
    interface Function3<One, Two, Three> {
        public void apply(One one, Two two, Three three);
    }

}
