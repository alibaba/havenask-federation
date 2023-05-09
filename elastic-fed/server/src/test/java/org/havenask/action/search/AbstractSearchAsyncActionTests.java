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

package org.havenask.action.search;

import org.havenask.action.ActionListener;
import org.havenask.action.OriginalIndices;
import org.havenask.action.support.IndicesOptions;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.routing.GroupShardsIterator;
import org.havenask.common.UUIDs;
import org.havenask.common.collect.Tuple;
import org.havenask.common.util.set.Sets;
import org.havenask.index.Index;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchPhaseResult;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.internal.AliasFilter;
import org.havenask.search.internal.InternalSearchResponse;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.search.internal.ShardSearchRequest;
import org.havenask.test.HavenaskTestCase;
import org.havenask.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class AbstractSearchAsyncActionTests extends HavenaskTestCase {

    private final List<Tuple<String, String>> resolvedNodes = new ArrayList<>();
    private final Set<ShardSearchContextId> releasedContexts = new CopyOnWriteArraySet<>();

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(SearchRequest request,
                                                                      ArraySearchPhaseResults<SearchPhaseResult> results,
                                                                      ActionListener<SearchResponse> listener,
                                                                      final boolean controlled,
                                                                      final AtomicLong expected) {
        final Runnable runnable;
        final TransportSearchAction.SearchTimeProvider timeProvider;
        if (controlled) {
            runnable = () -> expected.set(randomNonNegativeLong());
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, expected::get);
        } else {
            runnable = () -> {
                long elapsed = spinForAtLeastNMilliseconds(randomIntBetween(1, 10));
                expected.set(elapsed);
            };
            timeProvider = new TransportSearchAction.SearchTimeProvider(
                    0,
                    System.nanoTime(),
                    System::nanoTime);
        }

        BiFunction<String, String, Transport.Connection> nodeIdToConnection = (cluster, node) -> {
            resolvedNodes.add(Tuple.tuple(cluster, node));
            return null;
        };

        return new AbstractSearchAsyncAction<SearchPhaseResult>("test", logger, null, nodeIdToConnection,
                Collections.singletonMap("foo", new AliasFilter(new MatchAllQueryBuilder())), Collections.singletonMap("foo", 2.0f),
                Collections.singletonMap("name", Sets.newHashSet("bar", "baz")), null, request, listener,
                new GroupShardsIterator<>(
                    Collections.singletonList(
                        new SearchShardIterator(null, null, Collections.emptyList(), null)
                    )
                ), timeProvider, ClusterState.EMPTY_STATE, null,
                results, request.getMaxConcurrentShardRequests(),
                SearchResponse.Clusters.EMPTY) {
            @Override
            protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                return null;
            }

            @Override
            protected void executePhaseOnShard(final SearchShardIterator shardIt, final SearchShardTarget shard,
                                               final SearchActionListener<SearchPhaseResult> listener) {
            }

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }

            @Override
            public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection,
                                                 OriginalIndices originalIndices) {
                releasedContexts.add(contextId);
            }
        };
    }

    public void testTookWithControlledClock() {
        runTestTook(true);
    }

    public void testTookWithRealClock() {
        runTestTook(false);
    }

    private void runTestTook(final boolean controlled) {
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(new SearchRequest(),
            new ArraySearchPhaseResults<>(10), null, controlled, expected);
        final long actual = action.buildTookInMillis();
        if (controlled) {
            // with a controlled clock, we can assert the exact took time
            assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        } else {
            // with a real clock, the best we can say is that it took as long as we spun for
            assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        }
    }

    public void testBuildShardSearchTransportRequest() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean()).preference("_shards:1,3");
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new ArraySearchPhaseResults<>(10), null, false, expected);
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardIterator iterator = new SearchShardIterator(clusterAlias, new ShardId(new Index("name", "foo"), 1),
            Collections.emptyList(), new OriginalIndices(new String[] {"name", "name1"}, IndicesOptions.strictExpand()));
        ShardSearchRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator);
        assertEquals(IndicesOptions.strictExpand(), shardSearchTransportRequest.indicesOptions());
        assertArrayEquals(new String[] {"name", "name1"}, shardSearchTransportRequest.indices());
        assertEquals(new MatchAllQueryBuilder(), shardSearchTransportRequest.getAliasFilter().getQueryBuilder());
        assertEquals(2.0f, shardSearchTransportRequest.indexBoost(), 0.0f);
        assertArrayEquals(new String[] {"name", "name1"}, shardSearchTransportRequest.indices());
        assertArrayEquals(new String[] {"bar", "baz"}, shardSearchTransportRequest.indexRoutings());
        assertEquals("_shards:1,3", shardSearchTransportRequest.preference());
        assertEquals(clusterAlias, shardSearchTransportRequest.getClusterAlias());
    }

    public void testBuildSearchResponse() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(10);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            phaseResults, null, false, new AtomicLong());
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testBuildSearchResponseAllowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(10);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, queryResult, null, false, new AtomicLong());
        action.onShardFailure(0, new SearchShardTarget("node", new ShardId("index", "index-uuid", 0), null, OriginalIndices.NONE),
            new IllegalArgumentException());
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testSendSearchResponseDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        int numFailures = randomIntBetween(1, 5);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, numFailures);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        for (int i = 0; i < numFailures; i++) {
            ShardId failureShardId = new ShardId("index", "index-uuid", i);
            String failureClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String failureNodeId = randomAlphaOfLengthBetween(5, 10);
            action.onShardFailure(i, new SearchShardTarget(failureNodeId, failureShardId, failureClusterAlias, OriginalIndices.NONE),
                new IllegalArgumentException());
        }
        action.sendSearchResponse(InternalSearchResponse.empty(), phaseResults.results);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(numFailures, searchPhaseExecutionException.shardFailures().length);
        for (ShardSearchFailure shardSearchFailure : searchPhaseExecutionException.shardFailures()) {
            assertThat(shardSearchFailure.getCause(), instanceOf(IllegalArgumentException.class));
        }
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testOnPhaseFailure() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, 0);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        action.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
        assertEquals("message", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testShardNotAvailableWithDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        int numShards = randomIntBetween(2, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults =
            new ArraySearchPhaseResults<>(numShards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        // skip one to avoid the "all shards failed" failure.
        SearchShardIterator skipIterator = new SearchShardIterator(null, null, Collections.emptyList(), null);
        skipIterator.resetAndSkip();
        action.skipShard(skipIterator);
        // expect at least 2 shards, so onPhaseDone should report failure.
        action.onPhaseDone();
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
        assertEquals("Partial shards failure (" + (numShards - 1) + " shards unavailable)",
            searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
    }

    private static ArraySearchPhaseResults<SearchPhaseResult> phaseResults(Set<ShardSearchContextId> contextIds,
                                                                           List<Tuple<String, String>> nodeLookups,
                                                                           int numFailures) {
        int numResults = randomIntBetween(1, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numResults + numFailures);

        for (int i = 0; i < numResults; i++) {
            ShardSearchContextId contextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            contextIds.add(contextId);
            SearchPhaseResult phaseResult = new PhaseResult(contextId);
            String resultClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String resultNodeId = randomAlphaOfLengthBetween(5, 10);
            ShardId resultShardId = new ShardId("index", "index-uuid", i);
            nodeLookups.add(Tuple.tuple(resultClusterAlias, resultNodeId));
            phaseResult.setSearchShardTarget(new SearchShardTarget(resultNodeId, resultShardId, resultClusterAlias, OriginalIndices.NONE));
            phaseResult.setShardIndex(i);
            phaseResults.consumeResult(phaseResult, () -> {});
        }
        return phaseResults;
    }

    private static final class PhaseResult extends SearchPhaseResult {
        PhaseResult(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }
    }
}
