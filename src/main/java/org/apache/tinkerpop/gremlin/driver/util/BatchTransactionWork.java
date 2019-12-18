/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
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
     * throw exception when error
     */
    void execute(T tx, U g) throws  Throwable;
}
