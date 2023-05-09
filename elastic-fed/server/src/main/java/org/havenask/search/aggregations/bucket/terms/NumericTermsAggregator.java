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

package org.havenask.search.aggregations.bucket.terms;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.havenask.common.collect.List;
import org.havenask.common.lease.Releasable;
import org.havenask.common.lease.Releasables;
import org.havenask.common.util.LongArray;
import org.havenask.index.fielddata.FieldData;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.BucketOrder;
import org.havenask.search.aggregations.CardinalityUpperBound;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalMultiBucketAggregation;
import org.havenask.search.aggregations.InternalOrder;
import org.havenask.search.aggregations.LeafBucketCollector;
import org.havenask.search.aggregations.LeafBucketCollectorBase;
import org.havenask.search.aggregations.bucket.terms.IncludeExclude.LongFilter;
import org.havenask.search.aggregations.bucket.terms.LongKeyedBucketOrds.BucketOrdsEnum;
import org.havenask.search.aggregations.bucket.terms.SignificanceLookup.BackgroundFrequencyForLong;
import org.havenask.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.havenask.search.aggregations.support.ValuesSource;
import org.havenask.search.internal.ContextIndexSearcher;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.havenask.search.aggregations.InternalOrder.isKeyOrder;

public class NumericTermsAggregator extends TermsAggregator {
    private final ResultStrategy<?, ?> resultStrategy;
    private final ValuesSource.Numeric valuesSource;
    private final LongKeyedBucketOrds bucketOrds;
    private final LongFilter longFilter;

    public NumericTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<NumericTermsAggregator, ResultStrategy<?, ?>> resultStrategy,
        ValuesSource.Numeric valuesSource,
        DocValueFormat format,
        BucketOrder order,
        BucketCountThresholds bucketCountThresholds,
        SearchContext aggregationContext,
        Aggregator parent,
        SubAggCollectionMode subAggCollectMode,
        IncludeExclude.LongFilter longFilter,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    )
        throws IOException {
        super(name, factories, aggregationContext, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        this.longFilter = longFilter;
        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        SortedNumericDocValues values = resultStrategy.getValues(ctx);
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {
                                long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                                if (bucketOrdinal < 0) { // already seen
                                    bucketOrdinal = -1 - bucketOrdinal;
                                    collectExistingBucket(sub, doc, bucketOrdinal);
                                } else {
                                    collectBucket(sub, doc, bucketOrdinal);
                                }
                            }

                            previous = val;
                        }
                    }
                }
            }
        });
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void doClose() {
        Releasables.close(super::doClose, bucketOrds, resultStrategy);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("total_buckets", bucketOrds.size());
    }

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {
        private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            B[][] topBucketsPerOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCounts = new long[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
                long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);

                int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
                PriorityQueue<B> ordered = buildPriorityQueue(size);
                B spare = null;
                BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
                Supplier<B> emptyBucketBuilder = emptyBucketBuilder(owningBucketOrds[ordIdx]);
                while (ordsEnum.next()) {
                    long docCount = bucketDocCount(ordsEnum.ord());
                    otherDocCounts[ordIdx] += docCount;
                    if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                        continue;
                    }
                    if (spare == null) {
                        spare = emptyBucketBuilder.get();
                    }
                    updateBucket(spare, ordsEnum, docCount);
                    spare = ordered.insertWithOverflow(spare);
                }

                // Get the top buckets
                B[] bucketsForOrd = buildBuckets(ordered.size());
                topBucketsPerOrd[ordIdx] = bucketsForOrd;
                for (int b = ordered.size() - 1; b >= 0; --b) {
                    topBucketsPerOrd[ordIdx][b] = ordered.pop();
                    otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][b].getDocCount();
                }
            }

            buildSubAggs(topBucketsPerOrd);

            InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCounts[ordIdx],
                    topBucketsPerOrd[ordIdx]);
            }
            return result;
        }

        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * Resolve the doc values to collect results of this type.
         */
        abstract SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException;

        /**
         * Wrap the "standard" numeric terms collector to collect any more
         * information that this result type may need.
         */
        abstract LeafBucketCollector wrapCollector(LeafBucketCollector primary);

        /**
         * Build an array to hold the "top" buckets for each ordinal.
         */
        abstract B[][] buildTopBucketsPerOrd(int size);

        /**
         * Build an array of buckets for a particular ordinal. These arrays
         * are asigned to the value returned by {@link #buildTopBucketsPerOrd}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Build a {@linkplain Supplier} that can be used to build "empty"
         * buckets. Those buckets will then be {@link #updateBucket updated}
         * for each collected bucket.
         */
        abstract Supplier<B> emptyBucketBuilder(long owningBucketOrd);

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract void updateBucket(B spare, BucketOrdsEnum ordsEnum, long docCount) throws IOException;

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract PriorityQueue<B> buildPriorityQueue(int size);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[][] topBucketsPerOrd) throws IOException;

        /**
         * Collect extra entries for "zero" hit documents if they were requested
         * and required.
         */
        abstract void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(long owningBucketOrd, long otherDocCounts, B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();
    }

    abstract class StandardTermsResultStrategy<R extends InternalMappedTerms<R, B>, B extends InternalTerms.Bucket<B>> extends
        ResultStrategy<R, B> {
        protected final boolean showTermDocCountError;

        StandardTermsResultStrategy(boolean showTermDocCountError) {
            this.showTermDocCountError = showTermDocCountError;
        }

        @Override
        final LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        final PriorityQueue<B> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        }

        @Override
        final void buildSubAggs(B[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        Supplier<B> emptyBucketBuilder(long owningBucketOrd) {
            return this::buildEmptyBucket;
        }

        abstract B buildEmptyBucket();

        @Override
        final void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {
            if (bucketCountThresholds.getMinDocCount() != 0) {
                return;
            }
            if (InternalOrder.isCountDesc(order) && bucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                SortedNumericDocValues values = getValues(ctx);
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    if (values.advanceExact(docId)) {
                        int valueCount = values.docValueCount();
                        for (int v = 0; v < valueCount; ++v) {
                            long value = values.nextValue();
                            if (longFilter == null || longFilter.accept(value)) {
                                bucketOrds.add(owningBucketOrd, value);
                            }
                        }
                    }
                }
            }
        }

        @Override
        public final void close() {}
    }

    class LongTermsResults extends StandardTermsResultStrategy<LongTerms, LongTerms.Bucket> {
        LongTermsResults(boolean showTermDocCountError) {
            super(showTermDocCountError);
        }

        @Override
        String describe() {
            return "long_terms";
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return valuesSource.longValues(ctx);
        }

        @Override
        LongTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new LongTerms.Bucket[size][];
        }

        @Override
        LongTerms.Bucket[] buildBuckets(int size) {
            return new LongTerms.Bucket[size];
        }

        @Override
        LongTerms.Bucket buildEmptyBucket() {
            return new LongTerms.Bucket(0, 0, null, showTermDocCountError, 0, format);
        }

        @Override
        void updateBucket(LongTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) {
            spare.term = ordsEnum.value();
            spare.docCount = docCount;
            spare.bucketOrd = ordsEnum.ord();
        }

        @Override
        LongTerms buildResult(long owningBucketOrd, long otherDocCount, LongTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new LongTerms(
                name,
                reduceOrder,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                List.of(topBuckets),
                0
            );
        }

        @Override
        LongTerms buildEmptyResult() {
            return new LongTerms(
                name,
                order,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0
            );
        }
    }

    class DoubleTermsResults extends StandardTermsResultStrategy<DoubleTerms, DoubleTerms.Bucket> {

        DoubleTermsResults(boolean showTermDocCountError) {
            super(showTermDocCountError);
        }

        @Override
        String describe() {
            return "double_terms";
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return FieldData.toSortableLongBits(valuesSource.doubleValues(ctx));
        }

        @Override
        DoubleTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new DoubleTerms.Bucket[size][];
        }

        @Override
        DoubleTerms.Bucket[] buildBuckets(int size) {
            return new DoubleTerms.Bucket[size];
        }

        @Override
        DoubleTerms.Bucket buildEmptyBucket() {
            return new DoubleTerms.Bucket(0, 0, null, showTermDocCountError, 0, format);
        }

        @Override
        void updateBucket(DoubleTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) {
            spare.term = NumericUtils.sortableLongToDouble(ordsEnum.value());
            spare.docCount = docCount;
            spare.bucketOrd = ordsEnum.ord();
        }

        @Override
        DoubleTerms buildResult(long owningBucketOrd, long otherDocCount, DoubleTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new DoubleTerms(
                name,
                reduceOrder,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                List.of(topBuckets),
                0
            );
        }

        @Override
        DoubleTerms buildEmptyResult() {
            return new DoubleTerms(
                name,
                order,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0
            );
        }
    }

    class SignificantLongTermsResults extends ResultStrategy<SignificantLongTerms, SignificantLongTerms.Bucket> {
        private final BackgroundFrequencyForLong backgroundFrequencies;
        private final long supersetSize;
        private final SignificanceHeuristic significanceHeuristic;
        private LongArray subsetSizes;

        SignificantLongTermsResults(
            SignificanceLookup significanceLookup,
            SignificanceHeuristic significanceHeuristic,
            CardinalityUpperBound cardinality
        ) {
            backgroundFrequencies = significanceLookup.longLookup(context.bigArrays(), cardinality);
            supersetSize = significanceLookup.supersetSize();
            this.significanceHeuristic = significanceHeuristic;
            subsetSizes = context.bigArrays().newLongArray(1, true);
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return valuesSource.longValues(ctx);
        }

        @Override
        String describe() {
            return "significant_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return new LeafBucketCollectorBase(primary, null) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    super.collect(doc, owningBucketOrd);
                    subsetSizes = context.bigArrays().grow(subsetSizes, owningBucketOrd + 1);
                    subsetSizes.increment(owningBucketOrd, 1);
                }
            };
        }

        @Override
        SignificantLongTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new SignificantLongTerms.Bucket[size][];
        }

        @Override
        SignificantLongTerms.Bucket[] buildBuckets(int size) {
            return new SignificantLongTerms.Bucket[size];
        }

        @Override
        Supplier<SignificantLongTerms.Bucket> emptyBucketBuilder(long owningBucketOrd) {
            long subsetSize = subsetSizes.get(owningBucketOrd);
            return () -> new SignificantLongTerms.Bucket(0, subsetSize, 0, supersetSize, 0, null, format, 0);
        }

        @Override
        void updateBucket(SignificantLongTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) throws IOException {
            spare.term = ordsEnum.value();
            spare.subsetDf = docCount;
            spare.supersetDf = backgroundFrequencies.freq(spare.term);
            spare.bucketOrd = ordsEnum.ord();
            // During shard-local down-selection we use subset/superset stats that are for this shard only
            // Back at the central reducer these properties will be updated with global stats
            spare.updateScore(significanceHeuristic);
        }

        @Override
        PriorityQueue<SignificantLongTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size);
        }

        @Override
        void buildSubAggs(SignificantLongTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {}

        @Override
        SignificantLongTerms buildResult(long owningBucketOrd, long otherDocCoun, SignificantLongTerms.Bucket[] topBuckets) {
            return new SignificantLongTerms(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                subsetSizes.get(owningBucketOrd),
                supersetSize,
                significanceHeuristic,
                List.of(topBuckets)
            );
        }

        @Override
        SignificantLongTerms buildEmptyResult() {
            // We need to account for the significance of a miss in our global stats - provide corpus size as context
            ContextIndexSearcher searcher = context.searcher();
            IndexReader topReader = searcher.getIndexReader();
            int supersetSize = topReader.numDocs();
            return new SignificantLongTerms(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                0,
                supersetSize,
                significanceHeuristic,
                emptyList()
            );
        }

        @Override
        public void close() {
            Releasables.close(backgroundFrequencies, subsetSizes);
        }
    }


}
