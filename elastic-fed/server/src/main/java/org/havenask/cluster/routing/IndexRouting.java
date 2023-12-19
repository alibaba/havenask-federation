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

import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.IntConsumer;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.havenask.action.RoutingMissingException;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.Nullable;
import org.havenask.common.ParsingException;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParser.Token;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.support.filtering.FilterPath;
import org.havenask.transport.Transports;

/**
 * Generates the shard id for {@code (id, routing)} pairs.
 */
public abstract class IndexRouting {
    /**
     * Build the routing from {@link IndexMetadata}.
     */
    public static IndexRouting fromIndexMetadata(IndexMetadata metadata) {
        if (false == metadata.getRoutingPaths().isEmpty()) {
            return new ExtractFromSource(metadata);
        }
        if (metadata.isRoutingPartitionedIndex()) {
            return new Partitioned(metadata);
        }
        return new Unpartitioned(metadata);
    }

    protected final String indexName;
    private final int routingNumShards;
    private final int routingFactor;
    protected final IndexMetadata metadata;

    private IndexRouting(IndexMetadata metadata) {
        this.indexName = metadata.getIndex().getName();
        this.routingNumShards = metadata.getRoutingNumShards();
        this.routingFactor = metadata.getRoutingFactor();
        this.metadata = metadata;
    }

    /**
     * Called when indexing a document to generate the shard id that should contain
     * a document with the provided parameters.
     */
    public abstract int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source);

    /**
     * Called when updating a document to generate the shard id that should contain
     * a document with the provided {@code _id} and (optional) {@code _routing}.
     */
    public abstract int updateShard(String id, @Nullable String routing);

    /**
     * Called when deleting a document to generate the shard id that should contain
     * a document with the provided {@code _id} and (optional) {@code _routing}.
     */
    public abstract int deleteShard(String id, @Nullable String routing);

    /**
     * Called when getting a document to generate the shard id that should contain
     * a document with the provided {@code _id} and (optional) {@code _routing}.
     */
    public abstract int getShard(String id, @Nullable String routing);

    /**
     * Collect all of the shard ids that *may* contain documents with the
     * provided {@code routing}. Indices with a {@code routing_partition}
     * will collect more than one shard. Indices without a partition
     * will collect the same shard id as would be returned
     * by {@link #getShard}.
     * <p>
     * Note: This is called for any search-like requests that have a
     * routing specified but <strong>only</strong> if they have a routing
     * specified. If they do not have a routing they just use all shards
     * in the index.
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
     * Check if the _split index operation is allowed for an index
     * @throws IllegalArgumentException if the operation is not allowed
     */
    public void checkIndexSplitAllowed() {}

    private abstract static class IdAndRoutingOnly extends IndexRouting {
        private final boolean routingRequired;

        IdAndRoutingOnly(IndexMetadata metadata) {
            super(metadata);
            MappingMetadata mapping = metadata.mapping();
            this.routingRequired = mapping == null ? false : mapping.routing().required();
        }

        protected abstract int shardId(String id, @Nullable String routing);

        @Override
        public int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source) {
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
        }

        @Override
        public int updateShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
        }

        @Override
        public int deleteShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
        }

        @Override
        public int getShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(id, routing);
        }

        private void checkRoutingRequired(String id, @Nullable String routing) {
            if (routingRequired && routing == null) {
                throw new RoutingMissingException(indexName, "_doc", id);
            }
        }
    }

    /**
     * Strategy for indices that are not partitioned.
     */
    private static class Unpartitioned extends IdAndRoutingOnly {
        Unpartitioned(IndexMetadata metadata) {
            super(metadata);
        }

        @Override
        protected int shardId(String id, @Nullable String routing) {
            if (OperationRouting.customShardIdGenerator != null) {
                String effectiveRouting = routing == null ? id : routing;
                return OperationRouting.customShardIdGenerator.apply(metadata, effectiveRouting, 0);
            } else {
                return hashToShardId(effectiveRoutingToHash(routing == null ? id : routing));
            }
        }

        @Override
        public void collectSearchShards(String routing, IntConsumer consumer) {
            int shardId = shardId(null, routing);
            consumer.accept(shardId);
        }
    }

    /**
     * Strategy for partitioned indices.
     */
    private static class Partitioned extends IdAndRoutingOnly {
        private final int routingPartitionSize;

        Partitioned(IndexMetadata metadata) {
            super(metadata);
            this.routingPartitionSize = metadata.getRoutingPartitionSize();
        }

        @Override
        protected int shardId(String id, @Nullable String routing) {
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

    private static class ExtractFromSource extends IndexRouting {
        private final IndexMetadata metadata;
        private final boolean routingRequired;
        private final FilterPath[] include;

        ExtractFromSource(IndexMetadata metadata) {
            super(metadata);
            this.metadata = metadata;
            if (metadata.isRoutingPartitionedIndex()) {
                throw new IllegalArgumentException("routing_partition_size is incompatible with routing_path");
            }
            MappingMetadata mapping = metadata.mapping();
            this.routingRequired = mapping == null ? false : mapping.routing().required();
            this.include = FilterPath.compile(org.havenask.common.collect.Set.copyOf(metadata.getRoutingPaths()));
        }

        @Override
        public int indexShard(String id, @Nullable String routing, XContentType sourceType, BytesReference source) {
            if (metadata.getRoutingPaths().size() == 1 && false == metadata.getRoutingPaths().get(0).contains("*")) {
                return getRoutingHash(id, routing, sourceType, source);
            }

            if (routing != null) {
                throw new IllegalArgumentException(error("indexing with a specified routing and mismatch with routing_path"));
            }

            assert Transports.assertNotTransportThread("parsing the _source can get slow");

            List<NameAndHash> hashes = new ArrayList<>();
            try {
                try (XContentParser parser = sourceType.xContent()
                        .createParser(
                                NamedXContentRegistry.EMPTY,
                                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                source.streamInput(),
                                include,
                             null)
                ) {
                    parser.nextToken(); // Move to first token
                    if (parser.currentToken() == null) {
                        String routingPath = metadata.getRoutingPaths().toString();
                        int maxPathLength = 50;
                        throw new IllegalArgumentException(
                                "Error extracting routing: source didn't contain any routing dimension fields "
                                        + (routingPath.length() > maxPathLength
                                        ? routingPath.substring(0, maxPathLength - 1) + "...]"
                                        : routingPath)
                        );
                    }
                    parser.nextToken();
                    extractObject(hashes, null, parser);
                    ensureExpectedToken(null, parser.nextToken(), parser);
                }
            } catch (IOException | ParsingException e) {
                throw new IllegalArgumentException("Error extracting routing: " + e.getMessage(), e);
            }
            return hashToShardId(hashesToHash(hashes));
        }

        private int getRoutingHash(String id, String routing, XContentType sourceType, BytesReference source) {
            try (XContentParser parser = sourceType.xContent()
                    .createParser(
                            NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            source.streamInput(),
                            include,
                            null)
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                String sourceRouting = extractObject(parser);

                if (routing != null && false == routing.equals(sourceRouting)) {
                    throw new IllegalArgumentException(
                            "routing value is not the same with source routing field, routing="
                                    + routing
                                    + ", source routing field="
                                    + sourceRouting
                    );
                }

                return shardId(sourceRouting);
            } catch (Exception e) {
                throw new IllegalArgumentException("Error extracting routing value: " + e.getMessage(), e);
            }
        }

        private static String extractObject(XContentParser parser) throws IOException {
            while (parser.currentToken() != Token.END_OBJECT) {
                ensureExpectedToken(Token.FIELD_NAME, parser.nextToken(), parser);
                switch (parser.nextToken()) {
                    case VALUE_STRING:
                        return parser.text();
                    case VALUE_NUMBER:
                        return String.valueOf(parser.longValue());
                    case START_OBJECT:
                        return extractObject(parser);
                    default:
                        throw new ParsingException(
                                parser.getTokenLocation(),
                                String.format(
                                        Locale.ROOT,
                                        "Failed to parse routing value: expecting token of type [%s] or [%s] but found [%s]",
                                        XContentParser.Token.VALUE_STRING,
                                        XContentParser.Token.VALUE_NUMBER,
                                        parser.currentToken()
                                )
                        );
                }
            }

            return "";
        }

        private int shardId(@Nullable String routing) {
            if (routing == null) {
                throw new IllegalArgumentException("A routing value is required");
            }
            return hashToShardId(effectiveRoutingToHash(routing));
        }

        private void checkRoutingRequired(String id, @Nullable String routing) {
            if (routingRequired && routing == null) {
                throw new RoutingMissingException(indexName, "_doc", id);
            }
        }

        private static void extractObject(List<NameAndHash> hashes, @Nullable String path, XContentParser source) throws IOException {
            while (source.currentToken() != Token.END_OBJECT) {
                ensureExpectedToken(Token.FIELD_NAME, source.currentToken(), source);
                String fieldName = source.currentName();
                String subPath = path == null ? fieldName : path + "." + fieldName;
                source.nextToken();
                extractItem(hashes, subPath, source);
            }
        }

        private static void extractItem(List<NameAndHash> hashes, String path, XContentParser source) throws IOException {
            switch (source.currentToken()) {
                case START_OBJECT:
                    source.nextToken();
                    extractObject(hashes, path, source);
                    source.nextToken();
                    break;
                case VALUE_STRING:
                    hashes.add(new NameAndHash(new BytesRef(path), hash(new BytesRef(source.text()))));
                    source.nextToken();
                    break;
                case VALUE_NULL:
                    source.nextToken();
                    break;
                default:
                    throw new ParsingException(
                            source.getTokenLocation(),
                            "Routing values must be strings but found [{}]",
                            source.currentToken()
                    );
            }
        }

        private static int hash(BytesRef ref) {
            return StringHelper.murmurhash3_x86_32(ref, 0);
        }

        private static int hashesToHash(List<NameAndHash> hashes) {
            Collections.sort(hashes);
            Iterator<NameAndHash> itr = hashes.iterator();
            if (itr.hasNext() == false) {
                throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
            }
            NameAndHash prev = itr.next();
            int hash = hash(prev.name) ^ prev.hash;
            while (itr.hasNext()) {
                NameAndHash next = itr.next();
                if (prev.name.equals(next.name)) {
                    throw new IllegalArgumentException("Duplicate routing dimension for [" + next.name + "]");
                }
                int thisHash = hash(next.name) ^ next.hash;
                hash = 31 * hash + thisHash;
                prev = next;
            }
            return hash;
        }

        @Override
        public int updateShard(String id, @Nullable String routing) {
            throw new IllegalArgumentException(error("update"));
        }

        @Override
        public int deleteShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(routing);
        }

        @Override
        public int getShard(String id, @Nullable String routing) {
            checkRoutingRequired(id, routing);
            return shardId(routing);
        }

        @Override
        public void checkIndexSplitAllowed() {
            throw new IllegalArgumentException(error("index-split"));
        }

        @Override
        public void collectSearchShards(String routing, IntConsumer consumer) {
            int hash = effectiveRoutingToHash(routing);
            consumer.accept(hashToShardId(hash));
        }

        private String error(String operation) {
            return operation + " is not supported because the destination index [" + indexName + "] is in time series mode";
        }
    }

    private static class NameAndHash implements Comparable<NameAndHash> {
        private final BytesRef name;
        private final int hash;

        NameAndHash(BytesRef name, int hash) {
            this.name = name;
            this.hash = hash;
        }

        @Override
        public int compareTo(NameAndHash o) {
            return name.compareTo(o.name);
        }
    }
}
