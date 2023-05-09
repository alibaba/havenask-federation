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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.havenask.action.OriginalIndices;
import org.havenask.common.breaker.CircuitBreaker;
import org.havenask.common.breaker.NoopCircuitBreaker;
import org.havenask.common.lucene.search.TopDocsAndMaxScore;
import org.havenask.common.util.BigArrays;
import org.havenask.common.util.concurrent.HavenaskExecutors;
import org.havenask.common.util.concurrent.HavenaskThreadPoolExecutor;
import org.havenask.index.shard.ShardId;
import org.havenask.search.DocValueFormat;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.aggregations.pipeline.PipelineAggregator;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class QueryPhaseResultConsumerTests extends HavenaskTestCase {

    private SearchPhaseController searchPhaseController;
    private ThreadPool threadPool;
    private HavenaskThreadPoolExecutor executor;

    @Before
    public void setup() {
        searchPhaseController = new SearchPhaseController(writableRegistry(),
            s -> new InternalAggregation.ReduceContextBuilder() {
                @Override
                public InternalAggregation.ReduceContext forPartialReduction() {
                    return InternalAggregation.ReduceContext.forPartialReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, () -> PipelineAggregator.PipelineTree.EMPTY);
                }

                public InternalAggregation.ReduceContext forFinalReduction() {
                    return InternalAggregation.ReduceContext.forFinalReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, b -> {}, PipelineAggregator.PipelineTree.EMPTY);
                };
            });
        threadPool = new TestThreadPool(SearchPhaseControllerTests.class.getName());
        executor = HavenaskExecutors.newFixed(
            "test", 1, 10, HavenaskExecutors.daemonThreadFactory("test"), threadPool.getThreadContext());
    }

    @After
    public void cleanup() {
        executor.shutdownNow();
        terminate(threadPool);
    }

    public void testProgressListenerExceptionsAreCaught() throws Exception {

        ThrowingSearchProgressListener searchProgressListener = new ThrowingSearchProgressListener();

        List<SearchShard> searchShards = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "uuid", i)));
        }
        searchProgressListener.notifyListShards(searchShards, Collections.emptyList(), SearchResponse.Clusters.EMPTY, false);

        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.setBatchedReduceSize(2);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
        QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(searchRequest, executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), searchPhaseController, searchProgressListener,
            writableRegistry(), 10, e -> onPartialMergeFailure.accumulateAndGet(e, (prev, curr) -> {
                curr.addSuppressed(prev);
                return curr;
            }));

        CountDownLatch partialReduceLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", i),
                null, OriginalIndices.NONE);
            QuerySearchResult querySearchResult = new QuerySearchResult();
            TopDocs topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
            querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), new DocValueFormat[0]);
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(i);
            queryPhaseResultConsumer.consumeResult(querySearchResult, partialReduceLatch::countDown);
        }

        assertEquals(10, searchProgressListener.onQueryResult.get());
        assertTrue(partialReduceLatch.await(10, TimeUnit.SECONDS));
        assertNull(onPartialMergeFailure.get());
        assertEquals(8, searchProgressListener.onPartialReduce.get());

        queryPhaseResultConsumer.reduce();
        assertEquals(1, searchProgressListener.onFinalReduce.get());
    }

    private static class ThrowingSearchProgressListener extends SearchProgressListener {
        private final AtomicInteger onQueryResult = new AtomicInteger(0);
        private final AtomicInteger onPartialReduce = new AtomicInteger(0);
        private final AtomicInteger onFinalReduce = new AtomicInteger(0);

        @Override
        protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters,
                                    boolean fetchPhase) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onQueryResult(int shardIndex) {
            onQueryResult.incrementAndGet();
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                       InternalAggregations aggs, int reducePhase) {
            onPartialReduce.incrementAndGet();
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
            onFinalReduce.incrementAndGet();
            throw new UnsupportedOperationException();
        }
    }
}
