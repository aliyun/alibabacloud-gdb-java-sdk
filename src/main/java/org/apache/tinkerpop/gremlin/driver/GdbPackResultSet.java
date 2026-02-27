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

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * Results has been filled up into the result list, just return to upper user program!
 */
public final class GdbPackResultSet extends GdbResultSet {
    private static final Logger logger = LoggerFactory.getLogger(GdbConnectionPool.class);

    private final List<Result>  results;
    private int fromIndex = 0;
    public GdbPackResultSet(final ResultQueue resultQueue, final ExecutorService executor,
                                                         final CompletableFuture<Void> readCompleted, final RequestMessage requestMessage,
                                                         final GdbHost host, List<Result> results) {
        super(resultQueue, executor, readCompleted, requestMessage, host);
        this.results = results;
    }

    @Override
    public RequestMessage getOriginalRequestMessage() {
        return super.originalRequestMessage;
    }



    /**
     * Returns a future that will complete when {@link #allItemsAvailable()} is {@code true} and will contain the
     * attributes from the response.
     */
    @Override
    public CompletableFuture<Map<String,Object>> statusAttributes() {
        final CompletableFuture<Map<String,Object>> attrs = new CompletableFuture<>();
        attrs.complete(Collections.emptyMap());
        return attrs;
    }

    /**
     * Determines if all items have been returned to the client.
     */
    @Override
    public boolean allItemsAvailable() {
        return true;
    }

    /**
     * Returns a future that will complete when all items have been returned from the server.
     */
    @Override
    public CompletableFuture<Void> allItemsAvailableAsync() {
        final CompletableFuture<Void> allAvailable = new CompletableFuture<>();
        allAvailable.complete(null);
        return allAvailable;
    }

    /**
     * Gets the number of items available on the client.
     */
    @Override
    public int getAvailableItemCount() {
        return results.size();
    }

    /**
     * Get the next {@link Result} from the stream, blocking until one is available.
     */
    @Override
    public Result one() {
        final List<Result> results;
        try {
            results = some(1).get();

            assert results.size() <= 1;

            return results.size() == 1 ? results.get(0) : null;
        } catch (Exception e) {
            logger.warn("exception - {}", e.getMessage());
            return null;
        }
    }

    /**
     * The returned {@link CompletableFuture} completes when the number of items specified are available.  The
     * number returned will be equal to or less than that number.  They will only be less if the stream is
     * completed and there are less than that number specified available.
     */
    @Override
    public CompletableFuture<List<Result>> some(final int items) {
        final CompletableFuture<List<Result>> allAvailable = new CompletableFuture<>();
        if (fromIndex < results.size()) {
            int toIndex = fromIndex + items;
            if (toIndex > results.size()) {
                toIndex = results.size();
            }
            allAvailable.complete(results.subList(fromIndex, toIndex));
            fromIndex = toIndex;
        } else {
            allAvailable.complete(new ArrayList<>());
        }
        return allAvailable;
    }

    /**
     * The returned {@link CompletableFuture} completes when all reads are complete for this request and the
     * entire result has been accounted for on the client. While this method is named "all" it really refers to
     * retrieving all remaining items in the set.  For large result sets it is preferred to use
     * {@link Iterator} or {@link Stream} options, as the results will be held in memory at once.
     */
    @Override
    public CompletableFuture<List<Result>> all() {
        return some(results.size());
    }

    /**
     * Stream items with a blocking iterator.
     */
    @Override
    public Stream<Result> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(),
                Spliterator.IMMUTABLE | Spliterator.SIZED), false);
    }

    /**
     * Returns a blocking iterator of the items streaming from the server to the client. This {@link Iterator} will
     * consume results as they arrive and leaving the {@code ResultSet} empty when complete.
     * <p>
     * The returned {@link Iterator} does not support the {@link Iterator#remove} method.
     */
    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>() {
            private Result nextOne = null;

            @Override
            public boolean hasNext() {
                if (null == nextOne) {
                    nextOne = one();
                }
                return nextOne != null;
            }

            @Override
            public Result next() {
                if (null != nextOne || hasNext()) {
                    final Result r = nextOne;
                    nextOne = null;
                    return r;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
