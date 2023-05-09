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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.havenask.Version;
import org.havenask.action.search.SearchShardTask;
import org.havenask.action.search.SearchType;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.lease.Releasables;
import org.havenask.common.lucene.search.Queries;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.BigArrays;
import org.havenask.index.IndexService;
import org.havenask.index.IndexSettings;
import org.havenask.index.cache.bitset.BitsetFilterCache;
import org.havenask.index.engine.Engine;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.ObjectMapper;
import org.havenask.index.mapper.TypeFieldMapper;
import org.havenask.index.query.AbstractQueryBuilder;
import org.havenask.index.query.ParsedQuery;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.search.NestedHelper;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.similarity.SimilarityService;
import org.havenask.search.aggregations.SearchContextAggregations;
import org.havenask.search.collapse.CollapseContext;
import org.havenask.search.dfs.DfsSearchResult;
import org.havenask.search.fetch.FetchPhase;
import org.havenask.search.fetch.FetchSearchResult;
import org.havenask.search.fetch.StoredFieldsContext;
import org.havenask.search.fetch.subphase.FetchDocValuesContext;
import org.havenask.search.fetch.subphase.FetchFieldsContext;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.search.fetch.subphase.ScriptFieldsContext;
import org.havenask.search.fetch.subphase.highlight.SearchHighlightContext;
import org.havenask.search.internal.ContextIndexSearcher;
import org.havenask.search.internal.ReaderContext;
import org.havenask.search.internal.ScrollContext;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.search.internal.ShardSearchRequest;
import org.havenask.search.profile.Profilers;
import org.havenask.search.query.QueryPhaseExecutionException;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.search.rescore.RescoreContext;
import org.havenask.search.slice.SliceBuilder;
import org.havenask.search.sort.SortAndFormats;
import org.havenask.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

final class DefaultSearchContext extends SearchContext {

    private final ReaderContext readerContext;
    private final Engine.Searcher engineSearcher;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final LongSupplier relativeTimeSupplier;
    private SearchType searchType;
    private final BigArrays bigArrays;
    private final IndexShard indexShard;
    private final ClusterService clusterService;
    private final IndexService indexService;
    private final ContextIndexSearcher searcher;
    private final DfsSearchResult dfsResult;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;
    private final float queryBoost;
    private final boolean lowLevelCancellation;
    private TimeValue timeout;
    // terminate after count
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private List<String> groupStats;
    private boolean explain;
    private boolean version = false; // by default, we don't return versions
    private boolean seqAndPrimaryTerm = false;
    private StoredFieldsContext storedFields;
    private ScriptFieldsContext scriptFields;
    private FetchSourceContext fetchSourceContext;
    private FetchDocValuesContext docValuesContext;
    private FetchFieldsContext fetchFieldsContext;
    private int from = -1;
    private int size = -1;
    private SortAndFormats sort;
    private Float minimumScore;
    private boolean trackScores = false; // when sorting, track scores as well...
    private int trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
    private FieldDoc searchAfter;
    private CollapseContext collapse;
    // filter for sliced scroll
    private SliceBuilder sliceBuilder;
    private SearchShardTask task;
    private final Version minNodeVersion;

    /**
     * The original query as sent by the user without the types and aliases
     * applied. Putting things in here leaks them into highlighting so don't add
     * things like the type filter or alias filters.
     */
    private ParsedQuery originalQuery;

    /**
     * The query to actually execute.
     */
    private Query query;
    private ParsedQuery postFilter;
    private Query aliasFilter;
    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;
    private SearchContextAggregations aggregations;
    private SearchHighlightContext highlight;
    private SuggestionSearchContext suggest;
    private List<RescoreContext> rescore;
    private Profilers profilers;

    private final Map<String, SearchExtBuilder> searchExtBuilders = new HashMap<>();
    private final Map<Class<?>, Collector> queryCollectors = new HashMap<>();
    private final QueryShardContext queryShardContext;
    private final FetchPhase fetchPhase;

    DefaultSearchContext(ReaderContext readerContext,
                         ShardSearchRequest request,
                         SearchShardTarget shardTarget,
                         ClusterService clusterService,
                         BigArrays bigArrays,
                         LongSupplier relativeTimeSupplier,
                         TimeValue timeout,
                         FetchPhase fetchPhase,
                         boolean lowLevelCancellation,
                         Version minNodeVersion) throws IOException {
        this.readerContext = readerContext;
        this.request = request;
        this.fetchPhase = fetchPhase;
        this.searchType = request.searchType();
        this.shardTarget = shardTarget;
        // SearchContexts use a BigArrays that can circuit break
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.dfsResult = new DfsSearchResult(readerContext.id(), shardTarget, request);
        this.queryResult = new QuerySearchResult(readerContext.id(), shardTarget, request);
        this.fetchResult = new FetchSearchResult(readerContext.id(), shardTarget);
        this.indexService = readerContext.indexService();
        this.indexShard = readerContext.indexShard();
        this.clusterService = clusterService;
        this.engineSearcher = readerContext.acquireSearcher("search");
        this.searcher = new ContextIndexSearcher(engineSearcher.getIndexReader(), engineSearcher.getSimilarity(),
            engineSearcher.getQueryCache(), engineSearcher.getQueryCachingPolicy(), lowLevelCancellation);
        this.relativeTimeSupplier = relativeTimeSupplier;
        this.timeout = timeout;
        this.minNodeVersion = minNodeVersion;
        queryShardContext = indexService.newQueryShardContext(request.shardId().id(), this.searcher,
            request::nowInMillis, shardTarget.getClusterAlias());
        queryShardContext.setTypes(request.types());
        queryBoost = request.indexBoost();
        this.lowLevelCancellation = lowLevelCancellation;
    }

    @Override
    public void doClose() {
        Releasables.close(engineSearcher, searcher);
    }

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    @Override
    public void preProcess(boolean rewrite) {
        if (hasOnlySuggest() ) {
            return;
        }
        long from = from() == -1 ? 0 : from();
        long size = size() == -1 ? 10 : size();
        long resultWindow = from + size;
        int maxResultWindow = indexService.getIndexSettings().getMaxResultWindow();

        if (resultWindow > maxResultWindow) {
            if (scrollContext() == null) {
                throw new IllegalArgumentException(
                        "Result window is too large, from + size must be less than or equal to: [" + maxResultWindow + "] but was ["
                                + resultWindow + "]. See the scroll api for a more efficient way to request large data sets. "
                                + "This limit can be set by changing the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                                + "] index level setting.");
            }
            throw new IllegalArgumentException(
                    "Batch size is too large, size must be less than or equal to: [" + maxResultWindow + "] but was [" + resultWindow
                            + "]. Scroll batch sizes cost as much memory as result windows so they are controlled by the ["
                            + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey() + "] index level setting.");
        }
        if (rescore != null) {
            if (sort != null) {
                throw new IllegalArgumentException("Cannot use [sort] option in conjunction with [rescore].");
            }
            int maxWindow = indexService.getIndexSettings().getMaxRescoreWindow();
            for (RescoreContext rescoreContext: rescore()) {
                if (rescoreContext.getWindowSize() > maxWindow) {
                    throw new IllegalArgumentException("Rescore window [" + rescoreContext.getWindowSize() + "] is too large. "
                            + "It must be less than [" + maxWindow + "]. This prevents allocating massive heaps for storing the results "
                            + "to be rescored. This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                            + "] index level setting.");
                }
            }
        }

        if (sliceBuilder != null) {
            int sliceLimit = indexService.getIndexSettings().getMaxSlicesPerScroll();
            int numSlices = sliceBuilder.getMax();
            if (numSlices > sliceLimit) {
                throw new IllegalArgumentException("The number of slices [" + numSlices + "] is too large. It must "
                    + "be less than [" + sliceLimit + "]. This limit can be set by changing the [" +
                    IndexSettings.MAX_SLICES_PER_SCROLL.getKey() + "] index level setting.");
            }
        }

        // initialize the filtering alias based on the provided filters
        try {
            final QueryBuilder queryBuilder = request.getAliasFilter().getQueryBuilder();
            aliasFilter = queryBuilder == null ? null : queryBuilder.toQuery(queryShardContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (query() == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            parsedQuery(new ParsedQuery(new BoostQuery(query(), queryBoost), parsedQuery()));
        }
        this.query = buildFilteredQuery(query);
        if (rewrite) {
            try {
                this.query = searcher.rewrite(query);
            } catch (IOException e) {
                throw new QueryPhaseExecutionException(shardTarget, "Failed to rewrite main query", e);
            }
        }
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        List<Query> filters = new ArrayList<>();
        Query typeFilter = createTypeFilter(queryShardContext.getTypes());
        if (typeFilter != null) {
            filters.add(typeFilter);
        }

        if (mapperService().hasNested()
                && new NestedHelper(mapperService()).mightMatchNestedDocs(query)
                && (aliasFilter == null || new NestedHelper(mapperService()).mightMatchNestedDocs(aliasFilter))) {
            filters.add(Queries.newNonNestedFilter(mapperService().getIndexSettings().getIndexVersionCreated()));
        }

        if (aliasFilter != null) {
            filters.add(aliasFilter);
        }

        if (sliceBuilder != null) {
            Query slicedQuery = sliceBuilder.toFilter(clusterService, request, queryShardContext, minNodeVersion);
            if (slicedQuery instanceof MatchNoDocsQuery) {
                return slicedQuery;
            } else {
                filters.add(slicedQuery);
            }
        }

        if (filters.isEmpty()) {
            return query;
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, Occur.MUST);
            for (Query filter : filters) {
                builder.add(filter, Occur.FILTER);
            }
            return builder.build();
        }
    }

    private Query createTypeFilter(String[] types) {
        if (types != null && types.length >= 1) {
            if (mapperService().documentMapper() == null) {
                return null;
            }
            TypeFieldMapper.TypeFieldType ft = new TypeFieldMapper.TypeFieldType(mapperService().documentMapper().type());
            return ft.typeFilter(types);
        }
        return null;
    }

    @Override
    public ShardSearchContextId id() {
        return readerContext.id();
    }

    @Override
    public String source() {
        return "search";
    }

    @Override
    public ShardSearchRequest request() {
        return this.request;
    }

    @Override
    public SearchType searchType() {
        return this.searchType;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return this.shardTarget;
    }

    @Override
    public int numberOfShards() {
        return request.numberOfShards();
    }

    @Override
    public float queryBoost() {
        return queryBoost;
    }

    @Override
    public ScrollContext scrollContext() {
        return readerContext.scrollContext();
    }

    @Override
    public SearchContextAggregations aggregations() {
        return aggregations;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        //it's ok to use the writeable name here given that we enforce it to be the same as the name of the element that gets
        //parsed by the corresponding parser. There is one single name and one single way to retrieve the parsed object from the context.
        searchExtBuilders.put(searchExtBuilder.getWriteableName(), searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return searchExtBuilders.get(name);
    }

    @Override
    public SearchHighlightContext highlight() {
        return highlight;
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {
        this.highlight = highlight;
    }

    @Override
    public SuggestionSearchContext suggest() {
        return suggest;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        this.suggest = suggest;
    }

    @Override
    public List<RescoreContext> rescore() {
        if (rescore == null) {
            return Collections.emptyList();
        }
        return rescore;
    }

    @Override
    public void addRescore(RescoreContext rescore) {
        if (this.rescore == null) {
            this.rescore = new ArrayList<>();
        }
        this.rescore.add(rescore);
    }

    @Override
    public boolean hasScriptFields() {
        return scriptFields != null;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        if (scriptFields == null) {
            scriptFields = new ScriptFieldsContext();
        }
        return this.scriptFields;
    }

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
    @Override
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return this.fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        return docValuesContext;
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        this.docValuesContext = docValuesContext;
        return this;
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        return fetchFieldsContext;
    }

    @Override
    public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        this.fetchFieldsContext = fetchFieldsContext;
        return this;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    @Override
    public IndexShard indexShard() {
        return this.indexShard;
    }

    @Override
    public MapperService mapperService() {
        return indexService.mapperService();
    }

    @Override
    public SimilarityService similarityService() {
        return indexService.similarityService();
    }

    @Override
    public BigArrays bigArrays() {
        return bigArrays;
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return indexService.cache().bitsetFilterCache();
    }

    @Override
    public TimeValue timeout() {
        return timeout;
    }

    @Override
    public void timeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    @Override
    public int terminateAfter() {
        return terminateAfter;
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        this.terminateAfter = terminateAfter;
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        this.minimumScore = minimumScore;
        return this;
    }

    @Override
    public Float minimumScore() {
        return this.minimumScore;
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public SortAndFormats sort() {
        return this.sort;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    @Override
    public boolean trackScores() {
        return this.trackScores;
    }

    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHitsUpTo) {
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        return this;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return trackTotalHitsUpTo;
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        this.searchAfter = searchAfter;
        return this;
    }

    @Override
    public boolean lowLevelCancellation() {
        return lowLevelCancellation;
    }

    @Override
    public FieldDoc searchAfter() {
        return searchAfter;
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        this.collapse = collapse;
        return this;
    }

    @Override
    public CollapseContext collapse() {
        return collapse;
    }

    public SearchContext sliceBuilder(SliceBuilder sliceBuilder) {
        this.sliceBuilder = sliceBuilder;
        return this;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return this.postFilter;
    }

    @Override
    public Query aliasFilter() {
        return aliasFilter;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

    /**
     * The query to execute, in its rewritten form.
     */
    @Override
    public Query query() {
        return this.query;
    }

    @Override
    public int from() {
        return from;
    }

    @Override
    public SearchContext from(int from) {
        this.from = from;
        return this;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public boolean hasStoredFields() {
        return storedFields != null && storedFields.fieldNames() != null;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return storedFields != null;
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return storedFields;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        this.storedFields = storedFieldsContext;
        return this;
    }

    @Override
    public boolean storedFieldsRequested() {
        return storedFields == null || storedFields.fetchFields();
    }

    @Override
    public boolean explain() {
        return explain;
    }

    @Override
    public void explain(boolean explain) {
        this.explain = explain;
    }

    @Override
    @Nullable
    public List<String> groupStats() {
        return this.groupStats;
    }

    @Override
    public void groupStats(List<String> groupStats) {
        this.groupStats = groupStats;
    }

    @Override
    public boolean version() {
        return version;
    }

    @Override
    public void version(boolean version) {
        this.version = version;
    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return seqAndPrimaryTerm;
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        this.seqAndPrimaryTerm = seqNoAndPrimaryTerm;
    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    @Override
    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    @Override
    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchPhase fetchPhase() {
        return fetchPhase;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    @Override
    public MappedFieldType fieldType(String name) {
        return mapperService().fieldType(name);
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        return mapperService().getObjectMapper(name);
    }

    @Override
    public long getRelativeTimeInMillis() {
        return relativeTimeSupplier.getAsLong();
    }

    @Override
    public Map<Class<?>, Collector> queryCollectors() {
        return queryCollectors;
    }

    @Override
    public QueryShardContext getQueryShardContext() {
        return queryShardContext;
    }

    @Override
    public Profilers getProfilers() {
        return profilers;
    }

    public void setProfilers(Profilers profilers) {
        this.profilers = profilers;
    }

    @Override
    public void setTask(SearchShardTask task) {
        this.task = task;
    }

    @Override
    public SearchShardTask getTask() {
        return task;
    }

    @Override
    public boolean isCancelled() {
        return task.isCancelled();
    }

    @Override
    public ReaderContext readerContext() {
        return readerContext;
    }
}
