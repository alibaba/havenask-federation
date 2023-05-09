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

package org.havenask.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TopDocs;
import org.havenask.LegacyESVersion;
import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionRunnable;
import org.havenask.action.OriginalIndices;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchShardTask;
import org.havenask.action.search.SearchType;
import org.havenask.action.support.TransportActions;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.CheckedSupplier;
import org.havenask.common.UUIDs;
import org.havenask.common.breaker.CircuitBreaker;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.lease.Releasable;
import org.havenask.common.lease.Releasables;
import org.havenask.common.lucene.Lucene;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.BigArrays;
import org.havenask.common.util.CollectionUtils;
import org.havenask.common.util.concurrent.ConcurrentCollections;
import org.havenask.common.util.concurrent.ConcurrentMapLong;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.index.Index;
import org.havenask.index.IndexNotFoundException;
import org.havenask.index.IndexService;
import org.havenask.index.IndexSettings;
import org.havenask.index.engine.Engine;
import org.havenask.index.query.InnerHitContextBuilder;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchNoneQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryRewriteContext;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.query.Rewriteable;
import org.havenask.index.shard.IndexEventListener;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.SearchOperationListener;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.IndicesService;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.havenask.node.ResponseCollectorService;
import org.havenask.script.FieldScript;
import org.havenask.script.ScriptService;
import org.havenask.search.aggregations.AggregationInitializationException;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalAggregation.ReduceContext;
import org.havenask.search.aggregations.MultiBucketConsumerService;
import org.havenask.search.aggregations.SearchContextAggregations;
import org.havenask.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.collapse.CollapseContext;
import org.havenask.search.dfs.DfsPhase;
import org.havenask.search.dfs.DfsSearchResult;
import org.havenask.search.fetch.FetchPhase;
import org.havenask.search.fetch.FetchSearchResult;
import org.havenask.search.fetch.QueryFetchSearchResult;
import org.havenask.search.fetch.ScrollQueryFetchSearchResult;
import org.havenask.search.fetch.ShardFetchRequest;
import org.havenask.search.fetch.subphase.FetchDocValuesContext;
import org.havenask.search.fetch.subphase.FetchFieldsContext;
import org.havenask.search.fetch.subphase.ScriptFieldsContext.ScriptField;
import org.havenask.search.fetch.subphase.highlight.HighlightBuilder;
import org.havenask.search.internal.AliasFilter;
import org.havenask.search.internal.InternalScrollSearchRequest;
import org.havenask.search.internal.LegacyReaderContext;
import org.havenask.search.internal.ReaderContext;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.search.internal.ShardSearchRequest;
import org.havenask.search.lookup.SearchLookup;
import org.havenask.search.profile.Profilers;
import org.havenask.search.query.QueryPhase;
import org.havenask.search.query.QuerySearchRequest;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.search.query.ScrollQuerySearchResult;
import org.havenask.search.rescore.RescorerBuilder;
import org.havenask.search.searchafter.SearchAfterBuilder;
import org.havenask.search.sort.FieldSortBuilder;
import org.havenask.search.sort.MinAndMax;
import org.havenask.search.sort.SortAndFormats;
import org.havenask.search.sort.SortBuilder;
import org.havenask.search.suggest.Suggest;
import org.havenask.search.suggest.completion.CompletionSuggestion;
import org.havenask.threadpool.Scheduler.Cancellable;
import org.havenask.threadpool.ThreadPool;
import org.havenask.threadpool.ThreadPool.Names;
import org.havenask.transport.TransportRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.havenask.common.unit.TimeValue.timeValueHours;
import static org.havenask.common.unit.TimeValue.timeValueMillis;
import static org.havenask.common.unit.TimeValue.timeValueMinutes;

public class SearchService extends AbstractLifecycleComponent implements IndexEventListener {
    private static final Logger logger = LogManager.getLogger(SearchService.class);

    // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING =
        Setting.positiveTimeSetting("search.default_keep_alive", timeValueMinutes(5), Property.NodeScope, Property.Dynamic);
    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING =
        Setting.positiveTimeSetting("search.max_keep_alive", timeValueHours(24), Property.NodeScope, Property.Dynamic);
    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
        Setting.positiveTimeSetting("search.keep_alive_interval", timeValueMinutes(1), Property.NodeScope);
    public static final Setting<Boolean> ALLOW_EXPENSIVE_QUERIES =
        Setting.boolSetting("search.allow_expensive_queries", true, Property.NodeScope, Property.Dynamic);

    /**
     * Enables low-level, frequent search cancellation checks. Enabling low-level checks will make long running searches to react
     * to the cancellation request faster. It will produce more cancellation checks but benchmarking has shown these did not
     * noticeably slow down searches.
     */
    public static final Setting<Boolean> LOW_LEVEL_CANCELLATION_SETTING =
        Setting.boolSetting("search.low_level_cancellation", true, Property.Dynamic, Property.NodeScope);

    public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);
    public static final Setting<TimeValue> DEFAULT_SEARCH_TIMEOUT_SETTING =
        Setting.timeSetting("search.default_search_timeout", NO_TIMEOUT, Property.Dynamic, Property.NodeScope);
    public static final Setting<Boolean> DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS =
            Setting.boolSetting("search.default_allow_partial_results", true, Property.Dynamic, Property.NodeScope);

    public static final Setting<Integer> MAX_OPEN_SCROLL_CONTEXT =
        Setting.intSetting("search.max_open_scroll_context", 500, 0, Property.Dynamic, Property.NodeScope);

    public static final int DEFAULT_SIZE = 10;
    public static final int DEFAULT_FROM = 0;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final ResponseCollectorService responseCollectorService;

    private final BigArrays bigArrays;

    private final DfsPhase dfsPhase = new DfsPhase();

    private final QueryPhase queryPhase;

    private final FetchPhase fetchPhase;

    private volatile long defaultKeepAlive;

    private volatile long maxKeepAlive;

    private volatile TimeValue defaultSearchTimeout;

    private volatile boolean defaultAllowPartialSearchResults;

    private volatile boolean lowLevelCancellation;

    private volatile int maxOpenScrollContext;

    private final Cancellable keepAliveReaper;

    private final AtomicLong idGenerator = new AtomicLong();

    private final ConcurrentMapLong<ReaderContext> activeReaders = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final MultiBucketConsumerService multiBucketConsumerService;

    private final AtomicInteger openScrollContexts = new AtomicInteger();
    private final String sessionId = UUIDs.randomBase64UUID();

    public SearchService(ClusterService clusterService, IndicesService indicesService,
                         ThreadPool threadPool, ScriptService scriptService, BigArrays bigArrays, FetchPhase fetchPhase,
                         ResponseCollectorService responseCollectorService, CircuitBreakerService circuitBreakerService) {
        Settings settings = clusterService.getSettings();
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.bigArrays = bigArrays;
        this.queryPhase = new QueryPhase();
        this.fetchPhase = fetchPhase;
        this.multiBucketConsumerService = new MultiBucketConsumerService(clusterService, settings,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST));

        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_KEEPALIVE_SETTING, MAX_KEEPALIVE_SETTING,
            this::setKeepAlives, this::validateKeepAlives);

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, Names.SAME);

        defaultSearchTimeout = DEFAULT_SEARCH_TIMEOUT_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_SEARCH_TIMEOUT_SETTING, this::setDefaultSearchTimeout);

        defaultAllowPartialSearchResults = DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS,
                this::setDefaultAllowPartialSearchResults);

        maxOpenScrollContext = MAX_OPEN_SCROLL_CONTEXT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_SCROLL_CONTEXT, this::setMaxOpenScrollContext);

        lowLevelCancellation = LOW_LEVEL_CANCELLATION_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LOW_LEVEL_CANCELLATION_SETTING, this::setLowLevelCancellation);
    }

    private void validateKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        if (defaultKeepAlive.millis() > maxKeepAlive.millis()) {
            throw new IllegalArgumentException("Default keep alive setting for request [" + DEFAULT_KEEPALIVE_SETTING.getKey() + "]" +
                " should be smaller than max keep alive [" + MAX_KEEPALIVE_SETTING.getKey() + "], " +
                "was (" + defaultKeepAlive + " > " + maxKeepAlive + ")");
        }
    }

    private void setKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        validateKeepAlives(defaultKeepAlive, maxKeepAlive);
        this.defaultKeepAlive = defaultKeepAlive.millis();
        this.maxKeepAlive = maxKeepAlive.millis();
    }

    private void setDefaultSearchTimeout(TimeValue defaultSearchTimeout) {
        this.defaultSearchTimeout = defaultSearchTimeout;
    }

    private void setDefaultAllowPartialSearchResults(boolean defaultAllowPartialSearchResults) {
        this.defaultAllowPartialSearchResults = defaultAllowPartialSearchResults;
    }

    public boolean defaultAllowPartialSearchResults() {
        return defaultAllowPartialSearchResults;
    }

    private void setMaxOpenScrollContext(int maxOpenScrollContext) {
        this.maxOpenScrollContext = maxOpenScrollContext;
    }

    private void setLowLevelCancellation(Boolean lowLevelCancellation) {
        this.lowLevelCancellation = lowLevelCancellation;
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
        // once an index is removed due to deletion or closing, we can just clean up all the pending search context information
        // if we then close all the contexts we can get some search failures along the way which are not expected.
        // it's fine to keep the contexts open if the index is still "alive"
        // unfortunately we don't have a clear way to signal today why an index is closed.
        // to release memory and let references to the filesystem go etc.
        if (reason == IndexRemovalReason.DELETED || reason == IndexRemovalReason.CLOSED || reason == IndexRemovalReason.REOPENED) {
            freeAllContextForIndex(index);
        }
    }

    protected void putReaderContext(ReaderContext context) {
        final ReaderContext previous = activeReaders.put(context.id().getId(), context);
        assert previous == null;
        // ensure that if we race against afterIndexRemoved, we remove the context from the active list.
        // this is important to ensure store can be cleaned up, in particular if the search is a scroll with a long timeout.
        final Index index = context.indexShard().shardId().getIndex();
        if (indicesService.hasIndex(index) == false) {
            removeReaderContext(context.id().getId());
            throw new IndexNotFoundException(index);
        }
    }

    protected ReaderContext removeReaderContext(long id) {
        return activeReaders.remove(id);
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        for (final ReaderContext context : activeReaders.values()) {
            freeReaderContext(context.id());
        }
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    public void executeDfsPhase(ShardSearchRequest request, boolean keepStatesInContext,
                                SearchShardTask task, ActionListener<SearchPhaseResult> listener) {
        final IndexShard shard = getShard(request);
        rewriteAndFetchShardRequest(shard, request, new ActionListener<ShardSearchRequest>() {
            @Override
            public void onResponse(ShardSearchRequest rewritten) {
                // fork the execution in the search thread pool
                runAsync(getExecutor(shard), () -> executeDfsPhase(request, task, keepStatesInContext), listener);
            }

            @Override
            public void onFailure(Exception exc) {
                listener.onFailure(exc);
            }
        });
    }

    private DfsSearchResult executeDfsPhase(ShardSearchRequest request,
                                            SearchShardTask task,
                                            boolean keepStatesInContext) throws IOException {
        ReaderContext readerContext = createOrGetReaderContext(request, keepStatesInContext);
        try (Releasable ignored = readerContext.markAsUsed(getKeepAlive(request));
                SearchContext context = createContext(readerContext, request, task, true)) {
            dfsPhase.execute(context);
            return context.dfsResult();
        } catch (Exception e) {
            logger.trace("Dfs phase failed", e);
            processFailure(readerContext, e);
            throw e;
        }
    }

    /**
     * Try to load the query results from the cache or execute the query phase directly if the cache cannot be used.
     */
    private void loadOrExecuteQueryPhase(final ShardSearchRequest request, final SearchContext context) throws Exception {
        final boolean canCache = indicesService.canCache(request, context);
        context.getQueryShardContext().freezeContext();
        if (canCache) {
            indicesService.loadIntoContext(request, context, queryPhase);
        } else {
            queryPhase.execute(context);
        }
    }

    public void executeQueryPhase(ShardSearchRequest request, boolean keepStatesInContext,
                                  SearchShardTask task, ActionListener<SearchPhaseResult> listener) {
        assert request.canReturnNullResponseIfMatchNoDocs() == false || request.numberOfShards() > 1
            : "empty responses require more than one shard";
        final IndexShard shard = getShard(request);
        rewriteAndFetchShardRequest(shard, request, new ActionListener<ShardSearchRequest>() {
            @Override
            public void onResponse(ShardSearchRequest orig) {
                // check if we can shortcut the query phase entirely.
                if (orig.canReturnNullResponseIfMatchNoDocs()) {
                    assert orig.scroll() == null;
                    final CanMatchResponse canMatchResp;
                    try {
                        ShardSearchRequest clone = new ShardSearchRequest(orig);
                        canMatchResp = canMatch(clone, false);
                    } catch (Exception exc) {
                        listener.onFailure(exc);
                        return;
                    }
                    if (canMatchResp.canMatch == false) {
                        listener.onResponse(QuerySearchResult.nullInstance());
                        return;
                    }
                }
                // fork the execution in the search thread pool
                runAsync(getExecutor(shard), () -> executeQueryPhase(orig, task, keepStatesInContext), listener);
            }

            @Override
            public void onFailure(Exception exc) {
                listener.onFailure(exc);
            }
        });
    }

    private IndexShard getShard(ShardSearchRequest request) {
        if (request.readerId() != null) {
            return findReaderContext(request.readerId(), request).indexShard();
        } else {
            return indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
        }
    }

    private <T> void runAsync(Executor executor, CheckedSupplier<T, Exception> executable, ActionListener<T> listener) {
        executor.execute(ActionRunnable.supply(listener, executable::get));
    }

    private SearchPhaseResult executeQueryPhase(ShardSearchRequest request,
                                                SearchShardTask task,
                                                boolean keepStatesInContext) throws Exception {
        final ReaderContext readerContext = createOrGetReaderContext(request, keepStatesInContext);
        try (Releasable ignored = readerContext.markAsUsed(getKeepAlive(request));
                SearchContext context = createContext(readerContext, request, task, true)) {
            final long afterQueryTime;
            try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context)) {
                loadOrExecuteQueryPhase(request, context);
                if (context.queryResult().hasSearchContext() == false && readerContext.singleSession()) {
                    freeReaderContext(readerContext.id());
                }
                afterQueryTime = executor.success();
            }
            if (request.numberOfShards() == 1) {
                return executeFetchPhase(readerContext, context, afterQueryTime);
            } else {
                // Pass the rescoreDocIds to the queryResult to send them the coordinating node and receive them back in the fetch phase.
                // We also pass the rescoreDocIds to the LegacyReaderContext in case the search state needs to stay in the data node.
                final RescoreDocIds rescoreDocIds = context.rescoreDocIds();
                context.queryResult().setRescoreDocIds(rescoreDocIds);
                readerContext.setRescoreDocIds(rescoreDocIds);
                return context.queryResult();
            }
        } catch (Exception e) {
            // execution exception can happen while loading the cache, strip it
            if (e instanceof ExecutionException) {
                e = (e.getCause() == null || e.getCause() instanceof Exception) ?
                    (Exception) e.getCause() : new HavenaskException(e.getCause());
            }
            logger.trace("Query phase failed", e);
            processFailure(readerContext, e);
            throw e;
        }
    }

    private QueryFetchSearchResult executeFetchPhase(ReaderContext reader, SearchContext context, long afterQueryTime) {
        try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context, true, afterQueryTime)){
            shortcutDocIdsToLoad(context);
            fetchPhase.execute(context);
            if (reader.singleSession()) {
                freeReaderContext(reader.id());
            }
            executor.success();
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public void executeQueryPhase(InternalScrollSearchRequest request,
                                  SearchShardTask task,
                                  ActionListener<ScrollQuerySearchResult> listener) {
        final LegacyReaderContext readerContext = (LegacyReaderContext) findReaderContext(request.contextId(), request);
        final Releasable markAsUsed;
        try {
            markAsUsed = readerContext.markAsUsed(getScrollKeepAlive(request.scroll()));
        } catch (Exception e) {
            // We need to release the reader context of the scroll when we hit any exception (here the keep_alive can be too large)
            freeReaderContext(readerContext.id());
            throw e;
        }
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(null);
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, false);
                 SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(searchContext)) {
                searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(null));
                processScroll(request, readerContext, searchContext);
                queryPhase.execute(searchContext);
                executor.success();
                readerContext.setRescoreDocIds(searchContext.rescoreDocIds());
                return new ScrollQuerySearchResult(searchContext.queryResult(), searchContext.shardTarget());
            } catch (Exception e) {
                logger.trace("Query phase failed", e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    public void executeQueryPhase(QuerySearchRequest request, SearchShardTask task, ActionListener<QuerySearchResult> listener) {
        final ReaderContext readerContext = findReaderContext(request.contextId(), request.shardSearchRequest());
        final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(request.shardSearchRequest());
        final Releasable markAsUsed = readerContext.markAsUsed(getKeepAlive(shardSearchRequest));
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            readerContext.setAggregatedDfs(request.dfs());
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, true);
                 SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(searchContext)) {
                searchContext.searcher().setAggregatedDfs(request.dfs());
                queryPhase.execute(searchContext);
                if (searchContext.queryResult().hasSearchContext() == false && readerContext.singleSession()) {
                    // no hits, we can release the context since there will be no fetch phase
                    freeReaderContext(readerContext.id());
                }
                executor.success();
                // Pass the rescoreDocIds to the queryResult to send them the coordinating node and receive them back in the fetch phase.
                // We also pass the rescoreDocIds to the LegacyReaderContext in case the search state needs to stay in the data node.
                final RescoreDocIds rescoreDocIds = searchContext.rescoreDocIds();
                searchContext.queryResult().setRescoreDocIds(rescoreDocIds);
                readerContext.setRescoreDocIds(rescoreDocIds);
                return searchContext.queryResult();
            } catch (Exception e) {
                assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                logger.trace("Query phase failed", e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    private Executor getExecutor(IndexShard indexShard) {
        assert indexShard != null;
        final String executorName;
        if (indexShard.isSystem()) {
            executorName = Names.SYSTEM_READ;
        } else if (indexShard.indexSettings().isSearchThrottled()) {
            executorName = Names.SEARCH_THROTTLED;
        } else {
            executorName = Names.SEARCH;
        }
        return threadPool.executor(executorName);
    }

    public void executeFetchPhase(InternalScrollSearchRequest request, SearchShardTask task,
                                  ActionListener<ScrollQueryFetchSearchResult> listener) {
        final LegacyReaderContext readerContext = (LegacyReaderContext) findReaderContext(request.contextId(), request);
        final Releasable markAsUsed;
        try {
            markAsUsed = readerContext.markAsUsed(getScrollKeepAlive(request.scroll()));
        } catch (Exception e) {
            // We need to release the reader context of the scroll when we hit any exception (here the keep_alive can be too large)
            freeReaderContext(readerContext.id());
            throw e;
        }
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(null);
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, false);
                 SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(searchContext)) {
                searchContext.assignRescoreDocIds(readerContext.getRescoreDocIds(null));
                searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(null));
                processScroll(request, readerContext, searchContext);
                queryPhase.execute(searchContext);
                final long afterQueryTime = executor.success();
                QueryFetchSearchResult fetchSearchResult = executeFetchPhase(readerContext, searchContext, afterQueryTime);
                return new ScrollQueryFetchSearchResult(fetchSearchResult, searchContext.shardTarget());
            } catch (Exception e) {
                assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                logger.trace("Fetch phase failed", e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    public void executeFetchPhase(ShardFetchRequest request, SearchShardTask task, ActionListener<FetchSearchResult> listener) {
        final ReaderContext readerContext = findReaderContext(request.contextId(), request);
        final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(request.getShardSearchRequest());
        final Releasable markAsUsed = readerContext.markAsUsed(getKeepAlive(shardSearchRequest));
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, false)) {
                if (request.lastEmittedDoc() != null) {
                    searchContext.scrollContext().lastEmittedDoc = request.lastEmittedDoc();
                }
                searchContext.assignRescoreDocIds(readerContext.getRescoreDocIds(request.getRescoreDocIds()));
                searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(request.getAggregatedDfs()));
                searchContext.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
                try (SearchOperationListenerExecutor executor =
                         new SearchOperationListenerExecutor(searchContext, true, System.nanoTime())) {
                    fetchPhase.execute(searchContext);
                    if (readerContext.singleSession()) {
                        freeReaderContext(request.contextId());
                    }
                    executor.success();
                }
                return searchContext.fetchResult();
            } catch (Exception e) {
                assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    private ReaderContext getReaderContext(ShardSearchContextId id) {
        if (sessionId.equals(id.getSessionId()) == false && id.getSessionId().isEmpty() == false) {
            throw new SearchContextMissingException(id);
        }
        return activeReaders.get(id.getId());
    }

    private ReaderContext findReaderContext(ShardSearchContextId id, TransportRequest request) throws SearchContextMissingException {
        final ReaderContext reader = getReaderContext(id);
        if (reader == null) {
            throw new SearchContextMissingException(id);
        }
        try {
            reader.validate(request);
        } catch (Exception exc) {
            processFailure(reader, exc);
            throw exc;
        }
        return reader;
    }

    final ReaderContext createOrGetReaderContext(ShardSearchRequest request, boolean keepStatesInContext) {
        if (request.readerId() != null) {
            assert keepStatesInContext == false;
            return findReaderContext(request.readerId(), request);
        }
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard shard = indexService.getShard(request.shardId().id());
        Engine.SearcherSupplier reader = shard.acquireSearcherSupplier();
        return createAndPutReaderContext(request, indexService, shard, reader, keepStatesInContext);
    }

    final ReaderContext createAndPutReaderContext(ShardSearchRequest request, IndexService indexService, IndexShard shard,
                                                  Engine.SearcherSupplier reader, boolean keepStatesInContext) {
        assert request.readerId() == null;
        assert request.keepAlive() == null;
        ReaderContext readerContext = null;
        Releasable decreaseScrollContexts = null;
        try {
            if (request.scroll() != null) {
                decreaseScrollContexts = openScrollContexts::decrementAndGet;
                if (openScrollContexts.incrementAndGet() > maxOpenScrollContext) {
                    throw new HavenaskException(
                        "Trying to create too many scroll contexts. Must be less than or equal to: [" +
                            maxOpenScrollContext + "]. " + "This limit can be set by changing the ["
                            + MAX_OPEN_SCROLL_CONTEXT.getKey() + "] setting.");
                }
            }
            final long keepAlive = getKeepAlive(request);
            final ShardSearchContextId id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
            if (keepStatesInContext || request.scroll() != null) {
                readerContext = new LegacyReaderContext(id, indexService, shard, reader, request, keepAlive);
                if (request.scroll() != null) {
                    readerContext.addOnClose(decreaseScrollContexts);
                    decreaseScrollContexts = null;
                }
            } else {
                readerContext = new ReaderContext(id, indexService, shard, reader, keepAlive, request.keepAlive() == null);
            }
            reader = null;
            final ReaderContext finalReaderContext = readerContext;
            final SearchOperationListener searchOperationListener = shard.getSearchOperationListener();
            searchOperationListener.onNewReaderContext(finalReaderContext);
            if (finalReaderContext.scrollContext() != null) {
                searchOperationListener.onNewScrollContext(finalReaderContext);
            }
            readerContext.addOnClose(() -> {
                try {
                    if (finalReaderContext.scrollContext() != null) {
                        searchOperationListener.onFreeScrollContext(finalReaderContext);
                    }
                } finally {
                    searchOperationListener.onFreeReaderContext(finalReaderContext);
                }
            });
            putReaderContext(finalReaderContext);
            readerContext = null;
            return finalReaderContext;
        } finally {
            Releasables.close(reader, readerContext, decreaseScrollContexts);
        }
    }

    /**
     * Opens the reader context for given shardId. The newly opened reader context will be keep
     * until the {@code keepAlive} elapsed unless it is manually released.
     */
    public void openReaderContext(ShardId shardId, TimeValue keepAlive, ActionListener<ShardSearchContextId> listener) {
        checkKeepAliveLimit(keepAlive.millis());
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard shard = indexService.getShard(shardId.id());
        final SearchOperationListener searchOperationListener = shard.getSearchOperationListener();
        shard.awaitShardSearchActive(ignored -> {
            Engine.SearcherSupplier searcherSupplier = null;
            ReaderContext readerContext = null;
            try {
                searcherSupplier = shard.acquireSearcherSupplier();
                final ShardSearchContextId id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
                readerContext = new ReaderContext(id, indexService, shard, searcherSupplier, keepAlive.millis(), false);
                final ReaderContext finalReaderContext = readerContext;
                searcherSupplier = null; // transfer ownership to reader context
                searchOperationListener.onNewReaderContext(readerContext);
                readerContext.addOnClose(() -> searchOperationListener.onFreeReaderContext(finalReaderContext));
                putReaderContext(readerContext);
                readerContext = null;
                listener.onResponse(finalReaderContext.id());
            } catch (Exception exc) {
                Releasables.closeWhileHandlingException(searcherSupplier, readerContext);
                listener.onFailure(exc);
            }
        });
    }

    final SearchContext createContext(ReaderContext readerContext,
                                      ShardSearchRequest request,
                                      SearchShardTask task,
                                      boolean includeAggregations) throws IOException {
        final DefaultSearchContext context = createSearchContext(readerContext, request, defaultSearchTimeout);
        try {
            if (request.scroll() != null) {
                context.scrollContext().scroll = request.scroll();
            }
            parseSource(context, request.source(), includeAggregations);

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(DEFAULT_FROM);
            }
            if (context.size() == -1) {
                context.size(DEFAULT_SIZE);
            }
            context.setTask(task);

            // pre process
            queryPhase.preProcess(context);
        } catch (Exception e) {
            context.close();
            throw e;
        }

        return context;
    }

    public DefaultSearchContext createSearchContext(ShardSearchRequest request, TimeValue timeout) throws IOException {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard indexShard = indexService.getShard(request.shardId().getId());
        final Engine.SearcherSupplier reader = indexShard.acquireSearcherSupplier();
        final ShardSearchContextId id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
        try (ReaderContext readerContext = new ReaderContext(id, indexService, indexShard, reader, -1L, true)) {
            DefaultSearchContext searchContext = createSearchContext(readerContext, request, timeout);
            searchContext.addReleasable(readerContext.markAsUsed(0L));
            return searchContext;
        }
    }

    private DefaultSearchContext createSearchContext(ReaderContext reader, ShardSearchRequest request, TimeValue timeout)
        throws IOException {
        boolean success = false;
        DefaultSearchContext searchContext = null;
        try {
            SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().getId(),
                reader.indexShard().shardId(), request.getClusterAlias(), OriginalIndices.NONE);
            searchContext = new DefaultSearchContext(reader, request, shardTarget, clusterService, bigArrays,
                threadPool::relativeTimeInMillis, timeout, fetchPhase, lowLevelCancellation,
                clusterService.state().nodes().getMinNodeVersion());
            // we clone the query shard context here just for rewriting otherwise we
            // might end up with incorrect state since we are using now() or script services
            // during rewrite and normalized / evaluate templates etc.
            QueryShardContext context = new QueryShardContext(searchContext.getQueryShardContext());
            Rewriteable.rewrite(request.getRewriteable(), context, true);
            assert searchContext.getQueryShardContext().isCacheable();
            success = true;
        } finally {
            if (success == false) {
                // we handle the case where `IndicesService#indexServiceSafe`or `IndexService#getShard`, or the DefaultSearchContext
                // constructor throws an exception since we would otherwise leak a searcher and this can have severe implications
                // (unable to obtain shard lock exceptions).
                IOUtils.closeWhileHandlingException(searchContext);
            }
        }
        return searchContext;
    }

    private void freeAllContextForIndex(Index index) {
        assert index != null;
        for (ReaderContext ctx : activeReaders.values()) {
            if (index.equals(ctx.indexShard().shardId().getIndex())) {
                freeReaderContext(ctx.id());
            }
        }
    }

    public boolean freeReaderContext(ShardSearchContextId contextId) {
        if (getReaderContext(contextId) != null) {
            try (ReaderContext context = removeReaderContext(contextId.getId())) {
                return context != null;
            }
        }
        return false;
    }

    public void freeAllScrollContexts() {
        for (ReaderContext readerContext : activeReaders.values()) {
            if (readerContext.scrollContext() != null) {
                freeReaderContext(readerContext.id());
            }
        }
    }

    private long getKeepAlive(ShardSearchRequest request) {
        if (request.scroll() != null) {
            return getScrollKeepAlive(request.scroll());
        } else if (request.keepAlive() != null) {
            checkKeepAliveLimit(request.keepAlive().millis());
            return request.keepAlive().getMillis();
        } else {
            return request.readerId() == null ? defaultKeepAlive : -1;
        }
    }

    private long getScrollKeepAlive(Scroll scroll) {
        if (scroll != null && scroll.keepAlive() != null) {
            checkKeepAliveLimit(scroll.keepAlive().millis());
            return scroll.keepAlive().getMillis();
        }
        return defaultKeepAlive;
    }

    private void checkKeepAliveLimit(long keepAlive) {
        if (keepAlive > maxKeepAlive) {
            throw new IllegalArgumentException(
                "Keep alive for request (" + TimeValue.timeValueMillis(keepAlive) + ") is too large. " +
                    "It must be less than (" + TimeValue.timeValueMillis(maxKeepAlive) + "). " +
                    "This limit can be set by changing the [" + MAX_KEEPALIVE_SETTING.getKey() + "] cluster level setting.");
        }
    }

    private <T> ActionListener<T> wrapFailureListener(ActionListener<T> listener, ReaderContext context, Releasable releasable) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T resp) {
                Releasables.close(releasable);
                listener.onResponse(resp);
            }

            @Override
            public void onFailure(Exception exc) {
                processFailure(context, exc);
                Releasables.close(releasable);
                listener.onFailure(exc);
            }
        };
    }

    private boolean isScrollContext(ReaderContext context) {
        return context instanceof LegacyReaderContext && context.singleSession() == false;
    }

    private void processFailure(ReaderContext context, Exception exc) {
        if (context.singleSession() || isScrollContext(context)) {
            // we release the reader on failure if the request is a normal search or a scroll
            freeReaderContext(context.id());
        }
        try {
            if (Lucene.isCorruptionException(exc)) {
                context.indexShard().failShard("search execution corruption failure", exc);
            }
        } catch (Exception inner) {
            inner.addSuppressed(exc);
            logger.warn("failed to process shard failure to (potentially) send back shard failure on corruption", inner);
        }
    }

    private void parseSource(DefaultSearchContext context, SearchSourceBuilder source, boolean includeAggregations) {
        // nothing to parse...
        if (source == null) {
            return;
        }
        SearchShardTarget shardTarget = context.shardTarget();
        QueryShardContext queryShardContext = context.getQueryShardContext();
        context.from(source.from());
        context.size(source.size());
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        if (source.query() != null) {
            InnerHitContextBuilder.extractInnerHits(source.query(), innerHitBuilders);
            context.parsedQuery(queryShardContext.toQuery(source.query()));
        }
        if (source.postFilter() != null) {
            InnerHitContextBuilder.extractInnerHits(source.postFilter(), innerHitBuilders);
            context.parsedPostFilter(queryShardContext.toQuery(source.postFilter()));
        }
        if (innerHitBuilders.size() > 0) {
            for (Map.Entry<String, InnerHitContextBuilder> entry : innerHitBuilders.entrySet()) {
                try {
                    entry.getValue().build(context, context.innerHits());
                } catch (IOException e) {
                    throw new SearchException(shardTarget, "failed to build inner_hits", e);
                }
            }
        }
        if (source.sorts() != null) {
            try {
                Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(source.sorts(), context.getQueryShardContext());
                if (optionalSort.isPresent()) {
                    context.sort(optionalSort.get());
                }
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create sort elements", e);
            }
        }
        context.trackScores(source.trackScores());
        if (source.trackTotalHitsUpTo() != null
                && source.trackTotalHitsUpTo() != SearchContext.TRACK_TOTAL_HITS_ACCURATE
                && context.scrollContext() != null) {
            throw new SearchException(shardTarget, "disabling [track_total_hits] is not allowed in a scroll context");
        }
        if (source.trackTotalHitsUpTo() != null) {
            context.trackTotalHitsUpTo(source.trackTotalHitsUpTo());
        }
        if (source.minScore() != null) {
            context.minimumScore(source.minScore());
        }
        if (source.profile()) {
            context.setProfilers(new Profilers(context.searcher()));
        }
        if (source.timeout() != null) {
            context.timeout(source.timeout());
        }
        context.terminateAfter(source.terminateAfter());
        if (source.aggregations() != null && includeAggregations) {
            try {
                AggregatorFactories factories = source.aggregations().build(queryShardContext, null);
                context.aggregations(new SearchContextAggregations(factories, multiBucketConsumerService.create()));
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to create aggregators", e);
            }
        }
        if (source.suggest() != null) {
            try {
                context.suggest(source.suggest().build(queryShardContext));
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SuggestionSearchContext", e);
            }
        }
        if (source.rescores() != null) {
            try {
                for (RescorerBuilder<?> rescore : source.rescores()) {
                    context.addRescore(rescore.buildContext(queryShardContext));
                }
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create RescoreSearchContext", e);
            }
        }
        if (source.explain() != null) {
            context.explain(source.explain());
        }
        if (source.fetchSource() != null) {
            context.fetchSourceContext(source.fetchSource());
        }
        if (source.docValueFields() != null) {
            FetchDocValuesContext docValuesContext = FetchDocValuesContext.create(context.mapperService(), source.docValueFields());
            context.docValuesContext(docValuesContext);
        }
        if (source.fetchFields() != null) {
            FetchFieldsContext fetchFieldsContext = new FetchFieldsContext(source.fetchFields());
            context.fetchFieldsContext(fetchFieldsContext);
        }
        if (source.highlighter() != null) {
            HighlightBuilder highlightBuilder = source.highlighter();
            try {
                context.highlight(highlightBuilder.build(queryShardContext));
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SearchContextHighlighter", e);
            }
        }
        if (source.scriptFields() != null && source.size() != 0) {
            int maxAllowedScriptFields = context.mapperService().getIndexSettings().getMaxScriptFields();
            if (source.scriptFields().size() > maxAllowedScriptFields) {
                throw new IllegalArgumentException(
                        "Trying to retrieve too many script_fields. Must be less than or equal to: [" + maxAllowedScriptFields
                                + "] but was [" + source.scriptFields().size() + "]. This limit can be set by changing the ["
                                + IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey() + "] index level setting.");
            }
            for (org.havenask.search.builder.SearchSourceBuilder.ScriptField field : source.scriptFields()) {
                FieldScript.Factory factory = scriptService.compile(field.script(), FieldScript.CONTEXT);
                SearchLookup lookup = context.getQueryShardContext().lookup();
                FieldScript.LeafFactory searchScript = factory.newFactory(field.script().getParams(), lookup);
                context.scriptFields().add(new ScriptField(field.fieldName(), searchScript, field.ignoreFailure()));
            }
        }
        if (source.ext() != null) {
            for (SearchExtBuilder searchExtBuilder : source.ext()) {
                context.addSearchExt(searchExtBuilder);
            }
        }
        if (source.version() != null) {
            context.version(source.version());
        }

        if (source.seqNoAndPrimaryTerm() != null) {
            context.seqNoAndPrimaryTerm(source.seqNoAndPrimaryTerm());
        }

        if (source.stats() != null) {
            context.groupStats(source.stats());
        }
        if (CollectionUtils.isEmpty(source.searchAfter()) == false) {
            if (context.scrollContext() != null) {
                throw new SearchException(shardTarget, "`search_after` cannot be used in a scroll context.");
            }
            if (context.from() > 0) {
                throw new SearchException(shardTarget, "`from` parameter must be set to 0 when `search_after` is used.");
            }
            FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(context.sort(), source.searchAfter());
            context.searchAfter(fieldDoc);
        }

        if (source.slice() != null) {
            if (context.scrollContext() == null) {
                throw new SearchException(shardTarget, "`slice` cannot be used outside of a scroll context");
            }
            context.sliceBuilder(source.slice());
        }

        if (source.storedFields() != null) {
            if (source.storedFields().fetchFields() == false) {
                if (context.sourceRequested()) {
                    throw new SearchException(shardTarget, "[stored_fields] cannot be disabled if [_source] is requested");
                }
                if (context.fetchFieldsContext() != null) {
                    throw new SearchException(shardTarget, "[stored_fields] cannot be disabled when using the [fields] option");
                }
            }
            context.storedFieldsContext(source.storedFields());
        }

        if (source.collapse() != null) {
            if (context.scrollContext() != null) {
                throw new SearchException(shardTarget, "cannot use `collapse` in a scroll context");
            }
            if (context.searchAfter() != null) {
                throw new SearchException(shardTarget, "cannot use `collapse` in conjunction with `search_after`");
            }
            if (context.rescore() != null && context.rescore().isEmpty() == false) {
                throw new SearchException(shardTarget, "cannot use `collapse` in conjunction with `rescore`");
            }
            final CollapseContext collapseContext = source.collapse().build(queryShardContext);
            context.collapse(collapseContext);
        }
    }

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_T_F
     */
    private void shortcutDocIdsToLoad(SearchContext context) {
        final int[] docIdsToLoad;
        int docsOffset = 0;
        final Suggest suggest = context.queryResult().suggest();
        int numSuggestDocs = 0;
        final List<CompletionSuggestion> completionSuggestions;
        if (suggest != null && suggest.hasScoreDocs()) {
            completionSuggestions = suggest.filter(CompletionSuggestion.class);
            for (CompletionSuggestion completionSuggestion : completionSuggestions) {
                numSuggestDocs += completionSuggestion.getOptions().size();
            }
        } else {
            completionSuggestions = Collections.emptyList();
        }
        if (context.request().scroll() != null) {
            TopDocs topDocs = context.queryResult().topDocs().topDocs;
            docIdsToLoad = new int[topDocs.scoreDocs.length + numSuggestDocs];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[docsOffset++] = topDocs.scoreDocs[i].doc;
            }
        } else {
            TopDocs topDocs = context.queryResult().topDocs().topDocs;
            if (topDocs.scoreDocs.length < context.from()) {
                // no more docs...
                docIdsToLoad = new int[numSuggestDocs];
            } else {
                int totalSize = context.from() + context.size();
                docIdsToLoad = new int[Math.min(topDocs.scoreDocs.length - context.from(), context.size()) +
                    numSuggestDocs];
                for (int i = context.from(); i < Math.min(totalSize, topDocs.scoreDocs.length); i++) {
                    docIdsToLoad[docsOffset++] = topDocs.scoreDocs[i].doc;
                }
            }
        }
        for (CompletionSuggestion completionSuggestion : completionSuggestions) {
            for (CompletionSuggestion.Entry.Option option : completionSuggestion.getOptions()) {
                docIdsToLoad[docsOffset++] = option.getDoc().doc;
            }
        }
        context.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
    }

    private void processScroll(InternalScrollSearchRequest request, ReaderContext reader, SearchContext context) {
        // process scroll
        context.from(context.from() + context.size());
        context.scrollContext().scroll = request.scroll();
    }

    /**
     * Returns the number of active contexts in this
     * SearchService
     */
    public int getActiveContexts() {
        return this.activeReaders.size();
    }

    public ResponseCollectorService getResponseCollectorService() {
        return this.responseCollectorService;
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            for (ReaderContext context : activeReaders.values()) {
                if (context.isExpired()) {
                    logger.debug("freeing search context [{}]", context.id());
                    freeReaderContext(context.id());
                }
            }
        }
    }

    public AliasFilter buildAliasFilter(ClusterState state, String index, Set<String> resolvedExpressions) {
        return indicesService.buildAliasFilter(state, index, resolvedExpressions);
    }

    public void canMatch(ShardSearchRequest request, ActionListener<CanMatchResponse> listener) {
        try {
            listener.onResponse(canMatch(request));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    /**
     * This method uses a lightweight searcher without wrapping (i.e., not open a full reader on frozen indices) to rewrite the query
     * to check if the query can match any documents. This method can have false positives while if it returns {@code false} the query
     * won't match any documents on the current shard.
     */
    public CanMatchResponse canMatch(ShardSearchRequest request) throws IOException {
        return canMatch(request, true);
    }

    private CanMatchResponse canMatch(ShardSearchRequest request, boolean checkRefreshPending) throws IOException {
        assert request.searchType() == SearchType.QUERY_THEN_FETCH : "unexpected search type: " + request.searchType();
        final ReaderContext readerContext = request.readerId() != null ? findReaderContext(request.readerId(), request) : null;
        final Releasable markAsUsed = readerContext != null ? readerContext.markAsUsed(getKeepAlive(request)) : () -> {};
        try (Releasable ignored = markAsUsed) {
            final IndexService indexService;
            final Engine.Searcher canMatchSearcher;
            final boolean hasRefreshPending;
            if (readerContext != null) {
                indexService = readerContext.indexService();
                canMatchSearcher = readerContext.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
                hasRefreshPending = false;
            } else {
                indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
                IndexShard indexShard = indexService.getShard(request.shardId().getId());
                hasRefreshPending = indexShard.hasRefreshPending() && checkRefreshPending;
                canMatchSearcher = indexShard.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
            }

            try (Releasable ignored2 = canMatchSearcher) {
                QueryShardContext context = indexService.newQueryShardContext(request.shardId().id(), canMatchSearcher,
                    request::nowInMillis, request.getClusterAlias());
                Rewriteable.rewrite(request.getRewriteable(), context, false);
                final boolean aliasFilterCanMatch = request.getAliasFilter()
                    .getQueryBuilder() instanceof MatchNoneQueryBuilder == false;
                FieldSortBuilder sortBuilder = FieldSortBuilder.getPrimaryFieldSortOrNull(request.source());
                MinAndMax<?> minMax = sortBuilder != null ? FieldSortBuilder.getMinMaxOrNull(context, sortBuilder) : null;
                final boolean canMatch;
                if (canRewriteToMatchNone(request.source())) {
                    QueryBuilder queryBuilder = request.source().query();
                    canMatch = aliasFilterCanMatch && queryBuilder instanceof MatchNoneQueryBuilder == false;
                } else {
                    // null query means match_all
                    canMatch = aliasFilterCanMatch;
                }
                return new CanMatchResponse(canMatch || hasRefreshPending, minMax);
            }
        }
    }

    /**
     * Returns true iff the given search source builder can be early terminated by rewriting to a match none query. Or in other words
     * if the execution of the search request can be early terminated without executing it. This is for instance not possible if
     * a global aggregation is part of this request or if there is a suggest builder present.
     */
    public static boolean canRewriteToMatchNone(SearchSourceBuilder source) {
        if (source == null || source.query() == null || source.query() instanceof MatchAllQueryBuilder || source.suggest() != null) {
            return false;
        }
        AggregatorFactories.Builder aggregations = source.aggregations();
        return aggregations == null || aggregations.mustVisitAllDocs() == false;
    }

    private void rewriteAndFetchShardRequest(IndexShard shard, ShardSearchRequest request, ActionListener<ShardSearchRequest> listener) {
        ActionListener<Rewriteable> actionListener = ActionListener.wrap(r -> {
            if (request.readerId() != null) {
                listener.onResponse(request);
            } else {
                // now we need to check if there is a pending refresh and register
                shard.awaitShardSearchActive(b -> listener.onResponse(request));
            }
        }, listener::onFailure);
        // we also do rewrite on the coordinating node (TransportSearchService) but we also need to do it here for BWC as well as
        // AliasFilters that might need to be rewritten. These are edge-cases but we are every efficient doing the rewrite here so it's not
        // adding a lot of overhead
        Rewriteable.rewriteAndFetch(request.getRewriteable(), indicesService.getRewriteContext(request::nowInMillis), actionListener);
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given {@code now} provider
     */
    public QueryRewriteContext getRewriteContext(LongSupplier nowInMillis) {
        return indicesService.getRewriteContext(nowInMillis);
    }

    public IndicesService getIndicesService() {
        return indicesService;
    }

    /**
     * Returns a builder for {@link InternalAggregation.ReduceContext}. This
     * builder retains a reference to the provided {@link SearchRequest}.
     */
    public InternalAggregation.ReduceContextBuilder aggReduceContextBuilder(SearchRequest request) {
        return new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(bigArrays, scriptService,
                        () -> requestToPipelineTree(request));
            }

            @Override
            public ReduceContext forFinalReduction() {
                PipelineTree pipelineTree = requestToPipelineTree(request);
                return InternalAggregation.ReduceContext.forFinalReduction(
                        bigArrays, scriptService, multiBucketConsumerService.create(), pipelineTree);
            }
        };
    }

    private static PipelineTree requestToPipelineTree(SearchRequest request) {
        if (request.source() == null || request.source().aggregations() == null) {
            return PipelineTree.EMPTY;
        }
        return request.source().aggregations().buildPipelineTree();
    }

    public static final class CanMatchResponse extends SearchPhaseResult {
        private final boolean canMatch;
        private final MinAndMax<?> estimatedMinAndMax;

        public CanMatchResponse(StreamInput in) throws IOException {
            super(in);
            this.canMatch = in.readBoolean();
            if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                estimatedMinAndMax = in.readOptionalWriteable(MinAndMax::new);
            } else {
                estimatedMinAndMax = null;
            }
        }

        public CanMatchResponse(boolean canMatch, MinAndMax<?> estimatedMinAndMax) {
            this.canMatch = canMatch;
            this.estimatedMinAndMax = estimatedMinAndMax;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(canMatch);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                out.writeOptionalWriteable(estimatedMinAndMax);
            }
        }

        public boolean canMatch() {
            return canMatch;
        }

        public MinAndMax<?> estimatedMinAndMax() {
            return estimatedMinAndMax;
        }
    }

    /**
     * This helper class ensures we only execute either the success or the failure path for {@link SearchOperationListener}.
     * This is crucial for some implementations like {@link org.havenask.index.search.stats.ShardSearchStats}.
     */
    private static final class SearchOperationListenerExecutor implements AutoCloseable {
        private final SearchOperationListener listener;
        private final SearchContext context;
        private final long time;
        private final boolean fetch;
        private long afterQueryTime = -1;
        private boolean closed = false;

        SearchOperationListenerExecutor(SearchContext context) {
            this(context, false, System.nanoTime());
        }

        SearchOperationListenerExecutor(SearchContext context, boolean fetch, long startTime) {
            this.listener = context.indexShard().getSearchOperationListener();
            this.context = context;
            time = startTime;
            this.fetch = fetch;
            if (fetch) {
                listener.onPreFetchPhase(context);
            } else {
                listener.onPreQueryPhase(context);
            }
        }

        long success() {
            return afterQueryTime = System.nanoTime();
        }

        @Override
        public void close() {
            assert closed == false : "already closed - while technically ok double closing is a likely a bug in this case";
            if (closed == false) {
                closed = true;
                if (afterQueryTime != -1) {
                    if (fetch) {
                        listener.onFetchPhase(context, afterQueryTime - time);
                    } else {
                        listener.onQueryPhase(context, afterQueryTime - time);
                    }
                } else {
                    if (fetch) {
                        listener.onFailedFetchPhase(context);
                    } else {
                        listener.onFailedQueryPhase(context);
                    }
                }
            }
        }
    }
}
