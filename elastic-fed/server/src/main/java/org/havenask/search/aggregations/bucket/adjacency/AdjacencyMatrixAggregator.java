/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.search.aggregations.bucket.adjacency;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.lucene.Lucene;
import org.havenask.common.xcontent.ObjectParser.NamedObjectParser;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.query.QueryBuilder;
import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.CardinalityUpperBound;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.aggregations.LeafBucketCollector;
import org.havenask.search.aggregations.LeafBucketCollectorBase;
import org.havenask.search.aggregations.bucket.BucketsAggregator;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.havenask.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * Aggregation for adjacency matrices.
 *
 */
public class AdjacencyMatrixAggregator extends BucketsAggregator {

    public static final ParseField FILTERS_FIELD = new ParseField("filters");

    protected static class KeyedFilter implements Writeable, ToXContentFragment {
        private final String key;
        private final QueryBuilder filter;

        public static final NamedObjectParser<KeyedFilter, String> PARSER =
                (XContentParser p, String aggName, String name) ->
                     new KeyedFilter(name, parseInnerQueryBuilder(p));

        public KeyedFilter(String key, QueryBuilder filter) {
            if (key == null) {
                throw new IllegalArgumentException("[key] must not be null");
            }
            if (filter == null) {
                throw new IllegalArgumentException("[filter] must not be null");
            }
            this.key = key;
            this.filter = filter;
        }

        /**
         * Read from a stream.
         */
        public KeyedFilter(StreamInput in) throws IOException {
            key = in.readString();
            filter = in.readNamedWriteable(QueryBuilder.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeNamedWriteable(filter);
        }

        public String key() {
            return key;
        }

        public QueryBuilder filter() {
            return filter;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(key, filter);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, filter);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            KeyedFilter other = (KeyedFilter) obj;
            return Objects.equals(key, other.key) && Objects.equals(filter, other.filter);
        }
    }

    private final String[] keys;
    private final Weight[] filters;
    private final int totalNumKeys;
    private final int totalNumIntersections;
    private final String separator;

    public AdjacencyMatrixAggregator(String name, AggregatorFactories factories, String separator, String[] keys,
            Weight[] filters, SearchContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this.separator = separator;
        this.keys = keys;
        this.filters = filters;
        this.totalNumIntersections = ((keys.length * keys.length) - keys.length) / 2;
        this.totalNumKeys = keys.length + totalNumIntersections;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // no need to provide deleted docs to the filter
        final Bits[] bits = new Bits[filters.length];
        for (int i = 0; i < filters.length; ++i) {
            bits[i] = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filters[i].scorerSupplier(ctx));
        }
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                // Check each of the provided filters
                for (int i = 0; i < bits.length; i++) {
                    if (bits[i].get(doc)) {
                        collectBucket(sub, doc, bucketOrd(bucket, i));
                    }
                }
                // Check all the possible intersections of the provided filters
                int pos = filters.length;
                for (int i = 0; i < filters.length; i++) {
                    if (bits[i].get(doc)) {
                        for (int j = i + 1; j < filters.length; j++) {
                            if (bits[j].get(doc)) {
                                collectBucket(sub, doc, bucketOrd(bucket, pos));
                            }
                            pos++;
                        }
                    } else {
                        // Skip checks on all the other filters given one half of the pairing failed
                        pos += (filters.length - (i + 1));
                    }
                }
                assert pos == bits.length + totalNumIntersections;
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        // Buckets are ordered into groups - [keyed filters] [key1&key2 intersects]
        int maxOrd = owningBucketOrds.length * totalNumKeys;
        int totalBucketsToBuild = 0;
        for (int ord = 0; ord < maxOrd; ord++) {
            if (bucketDocCount(ord) > 0) {
                totalBucketsToBuild++;
            }
        }
        long[] bucketOrdsToBuild = new long[totalBucketsToBuild];
        int builtBucketIndex = 0;
        for (int ord = 0; ord < maxOrd; ord++) {
            if (bucketDocCount(ord) > 0) {
                bucketOrdsToBuild[builtBucketIndex++] = ord;
            }
        }
        assert builtBucketIndex == totalBucketsToBuild;
        builtBucketIndex = 0;
        InternalAggregations[] bucketSubAggs = buildSubAggsForBuckets(bucketOrdsToBuild);
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int owningBucketOrdIdx = 0; owningBucketOrdIdx < owningBucketOrds.length; owningBucketOrdIdx++) {
            List<InternalAdjacencyMatrix.InternalBucket> buckets = new ArrayList<>(filters.length);
            for (int i = 0; i < keys.length; i++) {
                long bucketOrd = bucketOrd(owningBucketOrds[owningBucketOrdIdx], i);
                int docCount = bucketDocCount(bucketOrd);
                // Empty buckets are not returned because this aggregation will commonly be used under a
                // a date-histogram where we will look for transactions over time and can expect many
                // empty buckets.
                if (docCount > 0) {
                    InternalAdjacencyMatrix.InternalBucket bucket = new InternalAdjacencyMatrix.InternalBucket(keys[i],
                            docCount, bucketSubAggs[builtBucketIndex++]);
                    buckets.add(bucket);
                }
            }
            int pos = keys.length;
            for (int i = 0; i < keys.length; i++) {
                for (int j = i + 1; j < keys.length; j++) {
                    long bucketOrd = bucketOrd(owningBucketOrds[owningBucketOrdIdx], pos);
                    int docCount = bucketDocCount(bucketOrd);
                    // Empty buckets are not returned due to potential for very sparse matrices
                    if (docCount > 0) {
                        String intersectKey = keys[i] + separator + keys[j];
                        InternalAdjacencyMatrix.InternalBucket bucket = new InternalAdjacencyMatrix.InternalBucket(intersectKey,
                                docCount, bucketSubAggs[builtBucketIndex++]);
                        buckets.add(bucket);
                    }
                    pos++;
                }
            }
            results[owningBucketOrdIdx] = new InternalAdjacencyMatrix(name, buckets, metadata());
        }
        assert builtBucketIndex == totalBucketsToBuild;
        return results;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        List<InternalAdjacencyMatrix.InternalBucket> buckets = new ArrayList<>(0);
        return new InternalAdjacencyMatrix(name, buckets, metadata());
    }

    final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
        return owningBucketOrdinal * totalNumKeys + filterOrd;
    }

}
