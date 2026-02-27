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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a method for selecting the host from a {@link Cluster}.
 */
public interface GdbLoadBalancingStrategy extends GdbHost.Listener {
    static final Logger logger = LoggerFactory.getLogger(GdbLoadBalancingStrategy.class);

    /**
     * Initialize the strategy with the {@link Cluster} instance and the expected host list.
     *
     * @param cluster the Cluster instance
     * @param hosts the collection of hosts to use
     */
    public void initialize(final Cluster cluster, final Collection<GdbHost> hosts);

    /**
     * Provide an ordered list of hosts to send the given {@link RequestMessage} to.
     *
     * @param msg the RequestMessage to send
     * @param isReqRead whether this is a read request
     * @param deadHosts collection of hosts that should be considered dead
     * @return an Iterator of hosts to try
     */
    public Iterator<GdbHost> select(final RequestMessage msg, boolean isReqRead, Collection<GdbHost> deadHosts);

    /**
     * A simple round-robin strategy that simply selects the next host in the {@link Cluster} to send the
     * {@link RequestMessage} to.
     */
    public static class RoundRobin implements GdbLoadBalancingStrategy {
        private final CopyOnWriteArrayList<GdbHost> availableMasterHosts = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<GdbHost> availableReadonlyHosts = new CopyOnWriteArrayList<>();
        private final AtomicInteger index = new AtomicInteger();

        /**
         * @param cluster the Cluster instance
         * @param hosts the collection of hosts to use
         */
        @Override
        public void initialize(final Cluster cluster, final Collection<GdbHost> hosts) {
            this.availableMasterHosts.addAll(hosts);
            this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
        }

        /**
         * @param msg the RequestMessage to send
         * @param isReqRead whether this is a read request
         * @param deadHosts collection of hosts that should be considered dead
         * @return an Iterator of hosts to try
         */
        @Override
        public Iterator<GdbHost> select(final RequestMessage msg, boolean isReqRead, Collection<GdbHost> deadHosts) {
            final List<GdbHost> hosts = new ArrayList<>();

            // if availableReadonlyHosts is not empty, readonly request will more inclined to readonly hosts;
            if (isReqRead && (! availableReadonlyHosts.isEmpty())) {
                availableReadonlyHosts.iterator().forEachRemaining(host -> {
                    if (host.isAvailable() && (! deadHosts.contains(host))) {
                        hosts.add(host);
                    }
                });
            }

            if (hosts.isEmpty()) {
                // a host could be marked as dead in which case we dont need to send messages to it - just skip it for
                // now. it might come back online later
                availableMasterHosts.iterator().forEachRemaining(host -> {
                    if (host.isAvailable() && (! deadHosts.contains(host))) {
                        hosts.add(host);
                    }
                });
            }

            final int startIndex = index.getAndIncrement();

            if (startIndex > Integer.MAX_VALUE - 10000) {
                index.set(0);
            }

            return new Iterator<GdbHost>() {
                private int currentIndex = startIndex;
                private int remainingGdbHosts = hosts.size();

                @Override
                public boolean hasNext() {
                    return remainingGdbHosts > 0;
                }

                @Override
                public GdbHost next() {
                    remainingGdbHosts--;
                    int c = currentIndex++ % hosts.size();
                    if (c < 0) {
                        c += hosts.size();
                    }
                    return hosts.get(c);
                }
            };
        }

        /**
         * @param host the host that became available
         */
        @Override
        public void onAvailable(final GdbHost host) {
            logger.warn("onAvailable on {}", host);
            if (host.isMaster()) {
                this.availableMasterHosts.addIfAbsent(host);
            } else {
                this.availableReadonlyHosts.addIfAbsent(host);
            }
        }

        /**
         * @param host the host that became unavailable
         */
        @Override
        public void onUnavailable(final GdbHost host) {
            logger.warn("onUnavailable on {}", host);
            if (host.isMaster()) {
                this.availableMasterHosts.remove(host);
            } else {
                this.availableReadonlyHosts.remove(host);
            }
        }

        @Override
        public void onNew(final GdbHost host) {
            onAvailable(host);
        }

        @Override
        public void onRemove(final GdbHost host) {
            onUnavailable(host);
        }
    }
}

