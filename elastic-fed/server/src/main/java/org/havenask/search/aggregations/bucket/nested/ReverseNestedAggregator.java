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

package org.havenask.search.aggregations.bucket.nested;

import com.carrotsearch.hppc.LongIntHashMap;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.havenask.common.ParseField;
import org.havenask.common.lucene.search.Queries;
import org.havenask.index.mapper.ObjectMapper;
import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.CardinalityUpperBound;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.LeafBucketCollector;
import org.havenask.search.aggregations.LeafBucketCollectorBase;
import org.havenask.search.aggregations.bucket.BucketsAggregator;
import org.havenask.search.aggregations.bucket.SingleBucketAggregator;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class ReverseNestedAggregator extends BucketsAggregator implements SingleBucketAggregator {

    static final ParseField PATH_FIELD = new ParseField("path");

    private final Query parentFilter;
    private final BitSetProducer parentBitsetProducer;

    public ReverseNestedAggregator(String name, AggregatorFactories factories, ObjectMapper objectMapper,
            SearchContext context, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
            throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        if (objectMapper == null) {
            parentFilter = Queries.newNonNestedFilter(context.mapperService().getIndexSettings().getIndexVersionCreated());
        } else {
            parentFilter = objectMapper.nestedTypeFilter();
        }
        parentBitsetProducer = context.bitsetFilterCache().getBitSetProducer(parentFilter);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // In Havenask if parent is deleted, then also the children are deleted, so the child docs this agg receives
        // must belong to parent docs that is alive. For this reason acceptedDocs can be null here.
        final BitSet parentDocs = parentBitsetProducer.getBitSet(ctx);
        if (parentDocs == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final LongIntHashMap bucketOrdToLastCollectedParentDoc = new LongIntHashMap(32);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int childDoc, long bucket) throws IOException {
                // fast forward to retrieve the parentDoc this childDoc belongs to
                final int parentDoc = parentDocs.nextSetBit(childDoc);
                assert childDoc <= parentDoc && parentDoc != DocIdSetIterator.NO_MORE_DOCS;

                int keySlot = bucketOrdToLastCollectedParentDoc.indexOf(bucket);
                if (bucketOrdToLastCollectedParentDoc.indexExists(keySlot)) {
                    int lastCollectedParentDoc = bucketOrdToLastCollectedParentDoc.indexGet(keySlot);
                    if (parentDoc > lastCollectedParentDoc) {
                        collectBucket(sub, parentDoc, bucket);
                        bucketOrdToLastCollectedParentDoc.indexReplace(keySlot, parentDoc);
                    }
                } else {
                    collectBucket(sub, parentDoc, bucket);
                    bucketOrdToLastCollectedParentDoc.indexInsert(keySlot, bucket, parentDoc);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(owningBucketOrds, (owningBucketOrd, subAggregationResults) ->
            new InternalReverseNested(name, bucketDocCount(owningBucketOrd), subAggregationResults, metadata()));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalReverseNested(name, 0, buildEmptySubAggregations(), metadata());
    }

    Query getParentFilter() {
        return parentFilter;
    }
}
