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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopFieldDocs;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.routing.GroupShardsIterator;
import org.havenask.search.SearchPhaseResult;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.internal.AliasFilter;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.internal.ShardSearchRequest;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.transport.Transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private final SearchPhaseController searchPhaseController;
    private final SearchProgressListener progressListener;

    // informations to track the best bottom top doc globally.
    private final int topDocsSize;
    private final int trackTotalHitsUpTo;
    private volatile BottomSortValuesCollector bottomSortCollector;

    SearchQueryThenFetchAsyncAction(final Logger logger, final SearchTransportService searchTransportService,
                                    final BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                                    final Map<String, AliasFilter> aliasFilter,
                                    final Map<String, Float> concreteIndexBoosts, final Map<String, Set<String>> indexRoutings,
                                    final SearchPhaseController searchPhaseController, final Executor executor,
                                    final QueryPhaseResultConsumer resultConsumer, final SearchRequest request,
                                    final ActionListener<SearchResponse> listener,
                                    final GroupShardsIterator<SearchShardIterator> shardsIts,
                                    final TransportSearchAction.SearchTimeProvider timeProvider,
                                    ClusterState clusterState, SearchTask task, SearchResponse.Clusters clusters) {
        super("query", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings,
                executor, request, listener, shardsIts, timeProvider, clusterState, task,
                resultConsumer, request.getMaxConcurrentShardRequests(), clusters);
        this.topDocsSize = SearchPhaseController.getTopDocsSize(request);
        this.trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        this.searchPhaseController = searchPhaseController;
        this.progressListener = task.getProgressListener();

        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        addReleasable(resultConsumer);

        boolean hasFetchPhase = request.source() == null ? true : request.source().size() > 0;
        progressListener.notifyListShards(SearchProgressListener.buildSearchShards(this.shardsIts),
            SearchProgressListener.buildSearchShards(toSkipShardsIts), clusters, hasFetchPhase);
    }

    protected void executePhaseOnShard(final SearchShardIterator shardIt,
                                       final SearchShardTarget shard,
                                       final SearchActionListener<SearchPhaseResult> listener) {
        ShardSearchRequest request = rewriteShardSearchRequest(super.buildShardSearchRequest(shardIt));
        getSearchTransport().sendExecuteQuery(getConnection(shard.getClusterAlias(), shard.getNodeId()), request, getTask(), listener);
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onShardResult(SearchPhaseResult result, SearchShardIterator shardIt) {
        QuerySearchResult queryResult = result.queryResult();
        if (queryResult.isNull() == false
                // disable sort optims for scroll requests because they keep track of the last bottom doc locally (per shard)
                && getRequest().scroll() == null
                && queryResult.topDocs() != null
                && queryResult.topDocs().topDocs.getClass() == TopFieldDocs.class) {
            TopFieldDocs topDocs = (TopFieldDocs) queryResult.topDocs().topDocs;
            if (bottomSortCollector == null) {
                synchronized (this) {
                    if (bottomSortCollector == null) {
                        bottomSortCollector = new BottomSortValuesCollector(topDocsSize, topDocs.fields);
                    }
                }
            }
            bottomSortCollector.consumeTopDocs(topDocs, queryResult.sortValueFormats());
        }
        super.onShardResult(result, shardIt);
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
        return new FetchSearchPhase(results, searchPhaseController, null, this);
    }

    private ShardSearchRequest rewriteShardSearchRequest(ShardSearchRequest request) {
        if (bottomSortCollector == null) {
            return request;
        }

        // disable tracking total hits if we already reached the required estimation.
        if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE
                && bottomSortCollector.getTotalHits() > trackTotalHitsUpTo) {
            request.source(request.source().shallowCopy().trackTotalHits(false));
        }

        // set the current best bottom field doc
        if (bottomSortCollector.getBottomSortValues() != null) {
            request.setBottomSortValues(bottomSortCollector.getBottomSortValues());
        }
        return request;
    }
}
