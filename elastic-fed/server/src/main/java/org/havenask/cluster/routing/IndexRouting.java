/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.cluster.routing;

import java.util.function.IntConsumer;

import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Nullable;

/**
 * Generates the shard id for {@code (id, routing)} pairs.
 */
public abstract class IndexRouting {
    /**
     * Build the routing from {@link IndexMetadata}.
     */
    public static IndexRouting fromIndexMetadata(IndexMetadata indexMetadata) {
        if (indexMetadata.isRoutingPartitionedIndex()) {
            return new Partitioned(
                    indexMetadata.getRoutingNumShards(),
                    indexMetadata.getRoutingFactor(),
                    indexMetadata.getRoutingPartitionSize()
            );
        }
        return new Unpartitioned(indexMetadata.getRoutingNumShards(), indexMetadata.getRoutingFactor());
    }

    private final int routingNumShards;
    private final int routingFactor;

    private IndexRouting(int routingNumShards, int routingFactor) {
        this.routingNumShards = routingNumShards;
        this.routingFactor = routingFactor;
    }

    /**
     * Generate the single shard id that should contain a document with the
     * provided {@code id} and {@code routing}.
     */
    public abstract int shardId(String id, @Nullable String routing);

    /**
     * Collect all of the shard ids that *may* contain documents with the
     * provided {@code routing}. Indices with a {@code routing_partition}
     * will collect more than one shard. Indices without a partition
     * will collect the same shard id as would be returned
     * by {@link #shardId}.
     */
    public abstract void collectSearchShards(String routing, IntConsumer consumer);

    /**
     * Convert a hash generated from an {@code (id, routing}) pair into a
     * shard id.
     */
    protected final int hashToShardId(int hash) {
        return Math.floorMod(hash, routingNumShards) / routingFactor;
    }

    /**
     * Convert a routing value into a hash.
     */
    private static int effectiveRoutingToHash(String effectiveRouting) {
        return Murmur3HashFunction.hash(effectiveRouting);
    }

    /**
     * Strategy for indices that are not partitioned.
     */
    private static class Unpartitioned extends IndexRouting {
        Unpartitioned(int routingNumShards, int routingFactor) {
            super(routingNumShards, routingFactor);
        }

        @Override
        public int shardId(String id, @Nullable String routing) {
            return hashToShardId(effectiveRoutingToHash(routing == null ? id : routing));
        }

        @Override
        public void collectSearchShards(String routing, IntConsumer consumer) {
            consumer.accept(hashToShardId(effectiveRoutingToHash(routing)));
        }
    }

    /**
     * Strategy for partitioned indices.
     */
    private static class Partitioned extends IndexRouting {
        private final int routingPartitionSize;

        Partitioned(int routingNumShards, int routingFactor, int routingPartitionSize) {
            super(routingNumShards, routingFactor);
            this.routingPartitionSize = routingPartitionSize;
        }

        @Override
        public int shardId(String id, @Nullable String routing) {
            if (routing == null) {
                throw new IllegalArgumentException("A routing value is required for gets from a partitioned index");
            }
            int offset = Math.floorMod(effectiveRoutingToHash(id), routingPartitionSize);
            return hashToShardId(effectiveRoutingToHash(routing) + offset);
        }

        @Override
        public void collectSearchShards(String routing, IntConsumer consumer) {
            int hash = effectiveRoutingToHash(routing);
            for (int i = 0; i < routingPartitionSize; i++) {
                consumer.accept(hashToShardId(hash + i));
            }
        }
    }
}
