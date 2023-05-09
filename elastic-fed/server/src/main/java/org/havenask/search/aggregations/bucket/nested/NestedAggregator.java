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

import com.carrotsearch.hppc.LongArrayList;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
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

public class NestedAggregator extends BucketsAggregator implements SingleBucketAggregator {

    static final ParseField PATH_FIELD = new ParseField("path");

    private final BitSetProducer parentFilter;
    private final Query childFilter;
    private final boolean collectsFromSingleBucket;

    private BufferingNestedLeafBucketCollector bufferingNestedLeafBucketCollector;

    NestedAggregator(String name, AggregatorFactories factories, ObjectMapper parentObjectMapper, ObjectMapper childObjectMapper,
                     SearchContext context, Aggregator parent, CardinalityUpperBound cardinality,
                     Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);

        Query parentFilter = parentObjectMapper != null ? parentObjectMapper.nestedTypeFilter()
            : Queries.newNonNestedFilter(context.mapperService().getIndexSettings().getIndexVersionCreated());
        this.parentFilter = context.bitsetFilterCache().getBitSetProducer(parentFilter);
        this.childFilter = childObjectMapper.nestedTypeFilter();
        this.collectsFromSingleBucket = cardinality.map(estimate -> estimate < 2);
    }

    @Override
    public LeafBucketCollector getLeafCollector(final LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(ctx);
        IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        Weight weight = searcher.createWeight(searcher.rewrite(childFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer childDocsScorer = weight.scorer(ctx);

        final BitSet parentDocs = parentFilter.getBitSet(ctx);
        final DocIdSetIterator childDocs = childDocsScorer != null ? childDocsScorer.iterator() : null;
        if (collectsFromSingleBucket) {
            return new LeafBucketCollectorBase(sub, null) {
                @Override
                public void collect(int parentDoc, long bucket) throws IOException {
                    // if parentDoc is 0 then this means that this parent doesn't have child docs (b/c these appear always before the parent
                    // doc), so we can skip:
                    if (parentDoc == 0 || parentDocs == null || childDocs == null) {
                        return;
                    }

                    final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                    int childDocId = childDocs.docID();
                    if (childDocId <= prevParentDoc) {
                        childDocId = childDocs.advance(prevParentDoc + 1);
                    }

                    for (; childDocId < parentDoc; childDocId = childDocs.nextDoc()) {
                        collectBucket(sub, childDocId, bucket);
                    }
                }
            };
        } else {
            return bufferingNestedLeafBucketCollector = new BufferingNestedLeafBucketCollector(sub, parentDocs, childDocs);
        }
    }

    @Override
    protected void preGetSubLeafCollectors() throws IOException {
        processBufferedDocs();
    }

    @Override
    protected void doPostCollection() throws IOException {
        processBufferedDocs();
    }

    private void processBufferedDocs() throws IOException {
        if (bufferingNestedLeafBucketCollector != null) {
            bufferingNestedLeafBucketCollector.processBufferedChildBuckets();
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(owningBucketOrds, (owningBucketOrd, subAggregationResults) ->
            new InternalNested(name, bucketDocCount(owningBucketOrd), subAggregationResults, metadata()));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalNested(name, 0, buildEmptySubAggregations(), metadata());
    }

    class BufferingNestedLeafBucketCollector extends LeafBucketCollectorBase {

        final BitSet parentDocs;
        final LeafBucketCollector sub;
        final DocIdSetIterator childDocs;
        final LongArrayList bucketBuffer = new LongArrayList();

        Scorable scorer;
        int currentParentDoc = -1;
        final CachedScorable cachedScorer = new CachedScorable();

        BufferingNestedLeafBucketCollector(LeafBucketCollector sub, BitSet parentDocs, DocIdSetIterator childDocs) {
            super(sub, null);
            this.sub = sub;
            this.parentDocs = parentDocs;
            this.childDocs = childDocs;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
            super.setScorer(cachedScorer);
        }

        @Override
        public void collect(int parentDoc, long bucket) throws IOException {
            // if parentDoc is 0 then this means that this parent doesn't have child docs (b/c these appear always before the parent
            // doc), so we can skip:
            if (parentDoc == 0 || parentDocs == null || childDocs == null) {
                return;
            }

            if (currentParentDoc != parentDoc) {
                processBufferedChildBuckets();
                if (scoreMode().needsScores()) {
                    // cache the score of the current parent
                    cachedScorer.score = scorer.score();
                }
                currentParentDoc = parentDoc;

            }
            bucketBuffer.add(bucket);
        }

        void processBufferedChildBuckets() throws IOException {
            if (bucketBuffer.isEmpty()) {
                return;
            }


            final int prevParentDoc = parentDocs.prevSetBit(currentParentDoc - 1);
            int childDocId = childDocs.docID();
            if (childDocId <= prevParentDoc) {
                childDocId = childDocs.advance(prevParentDoc + 1);
            }

            for (; childDocId < currentParentDoc; childDocId = childDocs.nextDoc()) {
                cachedScorer.doc = childDocId;
                final long[] buffer = bucketBuffer.buffer;
                final int size = bucketBuffer.size();
                for (int i = 0; i < size; i++) {
                    collectBucket(sub, childDocId, buffer[i]);
                }
            }
            bucketBuffer.clear();
        }
    }

    private static class CachedScorable extends Scorable {
        int doc;
        float score;

        @Override
        public final float score() { return score; }

        @Override
        public int docID() {
            return doc;
        }

    }

}
