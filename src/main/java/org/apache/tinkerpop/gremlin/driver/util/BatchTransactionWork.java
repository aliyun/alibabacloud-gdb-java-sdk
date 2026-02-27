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
package org.apache.tinkerpop.gremlin.driver.util;

import org.apache.tinkerpop.gremlin.driver.GdbClient;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * Callback that executes operations against a given {@link GdbClient}.
 * To be used with {@link GdbClient.SessionedClient}
 *
 */
public interface BatchTransactionWork<T extends GdbClient, U extends GraphTraversalSource> {
    /**
     * Executes all given operations against the same transaction.
     *
     * @param tx the transaction to use.
     * @param g the graph traversal source to use.
     * @throws Throwable when error
     */
    void execute(T tx, U g) throws  Throwable;
}
