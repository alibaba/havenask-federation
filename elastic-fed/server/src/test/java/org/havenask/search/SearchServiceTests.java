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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.OriginalIndices;
import org.havenask.action.index.IndexResponse;
import org.havenask.action.search.ClearScrollRequest;
import org.havenask.action.search.SearchPhaseExecutionException;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.search.SearchShardTask;
import org.havenask.action.search.SearchType;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.WriteRequest;
import org.havenask.common.Strings;
import org.havenask.common.UUIDs;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.Index;
import org.havenask.index.IndexModule;
import org.havenask.index.IndexNotFoundException;
import org.havenask.index.IndexService;
import org.havenask.index.IndexSettings;
import org.havenask.index.engine.Engine;
import org.havenask.index.query.AbstractQueryBuilder;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchNoneQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryRewriteContext;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.index.search.stats.SearchStats;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.SearchOperationListener;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.IndicesService;
import org.havenask.indices.settings.InternalOrPrivateSettingsPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.SearchPlugin;
import org.havenask.rest.RestStatus;
import org.havenask.script.MockScriptEngine;
import org.havenask.script.MockScriptPlugin;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.search.aggregations.AggregationBuilders;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.MultiBucketConsumerService;
import org.havenask.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.havenask.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.havenask.search.aggregations.support.ValueType;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.fetch.FetchSearchResult;
import org.havenask.search.fetch.ShardFetchRequest;
import org.havenask.search.internal.AliasFilter;
import org.havenask.search.internal.ReaderContext;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.search.internal.ShardSearchRequest;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.search.suggest.SuggestBuilder;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.havenask.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.havenask.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.havenask.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertSearchHits;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;

public class SearchServiceTests extends HavenaskSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FailOnRewriteQueryPlugin.class, CustomScriptPlugin.class,
            ReaderWrapperCountPlugin.class, InternalOrPrivateSettingsPlugin.class, MockSearchService.TestPlugin.class);
    }

    public static class ReaderWrapperCountPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.setReaderWrapper(service -> SearchServiceTests::apply);
        }
    }

    @Before
    private void resetCount() {
        numWrapInvocations = new AtomicInteger(0);
    }

    private static AtomicInteger numWrapInvocations = new AtomicInteger(0);
    private static DirectoryReader apply(DirectoryReader directoryReader) throws IOException {
        numWrapInvocations.incrementAndGet();
        return new FilterDirectoryReader(directoryReader,
            new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return reader;
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return directoryReader.getReaderCacheHelper();
            }
        };
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        static final String DUMMY_SCRIPT = "dummyScript";

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(DUMMY_SCRIPT, vars -> "dummy");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onFetchPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("havenask[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("havenask[node_s_0][search]"));
                    }
                }

                @Override
                public void onQueryPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("havenask[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("havenask[node_s_0][search]"));
                    }
                }
            });
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("search.default_search_timeout", "5s").build();
    }

    public void testClearOnClose() {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearOnStop() {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doStop();
        assertEquals(0, service.getActiveContexts());
    }

    public void testClearIndexDelete() {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        assertAcked(client().admin().indices().prepareDelete("index"));
        assertEquals(0, service.getActiveContexts());
    }

    public void testCloseSearchContextOnRewriteException() {
        // if refresh happens while checking the exception, the subsequent reference count might not match, so we switch it off
        createIndex("index", Settings.builder().put("index.refresh_interval", -1).build());
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        final int activeContexts = service.getActiveContexts();
        final int activeRefs = indexShard.store().refCount();
        expectThrows(SearchPhaseExecutionException.class, () ->
                client().prepareSearch("index").setQuery(new FailOnRewriteQueryBuilder()).get());
        assertEquals(activeContexts, service.getActiveContexts());
        assertEquals(activeRefs, indexShard.store().refCount());
    }

    @AwaitsFix(bugUrl = "https://github.com/alibaba/havenask-federation/issues/578")
    public void testSearchWhileIndexDeleted() throws InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startGun = new CountDownLatch(1);
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);

        final Thread thread = new Thread() {
            @Override
            public void run() {
                startGun.countDown();
                while(running.get()) {
                    service.afterIndexRemoved(indexService.index(), indexService.getIndexSettings(), DELETED);
                    if (randomBoolean()) {
                        // here we trigger some refreshes to ensure the IR go out of scope such that we hit ACE if we access a search
                        // context in a non-sane way.
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        client().prepareIndex("index", "type").setSource("field", "value")
                            .setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values())).execute(new ActionListener<IndexResponse>() {
                            @Override
                            public void onResponse(IndexResponse indexResponse) {
                                semaphore.release();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                semaphore.release();
                            }
                        });
                    }
                }
            }
        };
        thread.start();
        startGun.await();
        try {
            final int rounds = scaledRandomIntBetween(100, 10000);
            SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
            SearchRequest scrollSearchRequest = new SearchRequest().allowPartialSearchResults(true)
                .scroll(new Scroll(TimeValue.timeValueMinutes(1)));
            for (int i = 0; i < rounds; i++) {
                try {
                    try {
                        PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
                        final boolean useScroll = randomBoolean();
                        service.executeQueryPhase(
                            new ShardSearchRequest(OriginalIndices.NONE, useScroll ? scrollSearchRequest : searchRequest,
                                indexShard.shardId(), 1,
                                new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null),
                            true,
                            new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()), result);
                        SearchPhaseResult searchPhaseResult = result.get();
                        IntArrayList intCursors = new IntArrayList(1);
                        intCursors.add(0);
                        ShardFetchRequest req = new ShardFetchRequest(searchPhaseResult.getContextId(), intCursors, null/* not a scroll */);
                        PlainActionFuture<FetchSearchResult> listener = new PlainActionFuture<>();
                        service.executeFetchPhase(req, new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()), listener);
                        listener.get();
                        if (useScroll) {
                            // have to free context since this test does not remove the index from IndicesService.
                            service.freeReaderContext(searchPhaseResult.getContextId());
                        }
                    } catch (ExecutionException ex) {
                        assertThat(ex.getCause(), instanceOf(RuntimeException.class));
                        throw ((RuntimeException)ex.getCause());
                    }
                } catch (AlreadyClosedException ex) {
                    throw ex;
                } catch (IllegalStateException ex) {
                    assertEquals("reader_context is already closed can't increment refCount current count [0]", ex.getMessage());
                } catch (SearchContextMissingException ex) {
                    // that's fine
                }
            }
        } finally {
            running.set(false);
            thread.join();
            semaphore.acquire(Integer.MAX_VALUE);
        }

        assertEquals(0, service.getActiveContexts());

        SearchStats.Stats totalStats = indexShard.searchStats().getTotal();
        assertEquals(0, totalStats.getQueryCurrent());
        assertEquals(0, totalStats.getScrollCurrent());
        assertEquals(0, totalStats.getFetchCurrent());
    }

    public void testSearchWhileIndexDeletedDoesNotLeakSearchContext() throws ExecutionException, InterruptedException {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);

        MockSearchService service = (MockSearchService) getInstanceFromNode(SearchService.class);
        service.setOnPutContext(
            context -> {
                if (context.indexShard() == indexShard) {
                    assertAcked(client().admin().indices().prepareDelete("index"));
                }
            }
        );

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchRequest scrollSearchRequest = new SearchRequest().allowPartialSearchResults(true)
            .scroll(new Scroll(TimeValue.timeValueMinutes(1)));

        // the scrolls are not explicitly freed, but should all be gone when the test finished.
        // for completeness, we also randomly test the regular search path.
        final boolean useScroll = randomBoolean();
        PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
        service.executeQueryPhase(
            new ShardSearchRequest(OriginalIndices.NONE, useScroll ? scrollSearchRequest : searchRequest,
                new ShardId(resolveIndex("index"), 0), 1,
                new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null),
            randomBoolean(), new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()), result);

        try {
            result.get();
        } catch (Exception e) {
            // ok
        }

        expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex().setIndices("index").get());

        assertEquals(0, service.getActiveContexts());

        SearchStats.Stats totalStats = indexShard.searchStats().getTotal();
        assertEquals(0, totalStats.getQueryCurrent());
        assertEquals(0, totalStats.getScrollCurrent());
        assertEquals(0, totalStats.getFetchCurrent());
    }

    public void testTimeout() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final ShardSearchRequest requestWithDefaultTimeout = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f, -1, null, null);

        try (ReaderContext reader = createReaderContext(indexService, indexShard);
             SearchContext contextWithDefaultTimeout = service.createContext(reader, requestWithDefaultTimeout, null, randomBoolean())) {
            // the search context should inherit the default timeout
            assertThat(contextWithDefaultTimeout.timeout(), equalTo(TimeValue.timeValueSeconds(5)));
        }

        final long seconds = randomIntBetween(6, 10);
        searchRequest.source(new SearchSourceBuilder().timeout(TimeValue.timeValueSeconds(seconds)));
        final ShardSearchRequest requestWithCustomTimeout = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f, -1, null, null);
        try (ReaderContext reader = createReaderContext(indexService, indexShard);
             SearchContext context = service.createContext(reader, requestWithCustomTimeout, null, randomBoolean())) {
            // the search context should inherit the query timeout
            assertThat(context.timeout(), equalTo(TimeValue.timeValueSeconds(seconds)));
        }
    }

    /**
     * test that getting more than the allowed number of docvalue_fields throws an exception
     */
    public void testMaxDocvalueFieldsSearch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        // adding the maximum allowed number of docvalue_fields to retrieve
        for (int i = 0; i < indexService.getIndexSettings().getMaxDocvalueFields(); i++) {
            searchSourceBuilder.docValueField("field" + i);
        }
        final ShardSearchRequest request = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null);
        try (ReaderContext reader = createReaderContext(indexService, indexShard);
             SearchContext context = service.createContext(reader, request, null, randomBoolean())) {
            assertNotNull(context);
        }
        searchSourceBuilder.docValueField("one_field_too_much");
        try (ReaderContext reader = createReaderContext(indexService, indexShard)) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> service.createContext(reader, request, null, randomBoolean()));
            assertEquals(
                "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [100] but was [101]. "
                    + "This limit can be set by changing the [index.max_docvalue_fields_search] index level setting.",
                ex.getMessage());
        }
    }

    /**
     * test that getting more than the allowed number of script_fields throws an exception
     */
    public void testMaxScriptFieldsSearch() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        // adding the maximum allowed number of script_fields to retrieve
        int maxScriptFields = indexService.getIndexSettings().getMaxScriptFields();
        for (int i = 0; i < maxScriptFields; i++) {
            searchSourceBuilder.scriptField("field" + i,
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
        }
        final ShardSearchRequest request = new ShardSearchRequest(OriginalIndices.NONE, searchRequest,
            indexShard.shardId(), 1, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null);

        try(ReaderContext reader = createReaderContext(indexService, indexShard)) {
            try (SearchContext context = service.createContext(reader, request, null, randomBoolean())) {
                assertNotNull(context);
            }
            searchSourceBuilder.scriptField("anotherScriptField",
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> service.createContext(reader, request, null, randomBoolean()));
            assertEquals(
                    "Trying to retrieve too many script_fields. Must be less than or equal to: [" + maxScriptFields + "] but was ["
                            + (maxScriptFields + 1)
                            + "]. This limit can be set by changing the [index.max_script_fields] index level setting.",
                    ex.getMessage());
        }
    }

    public void testIgnoreScriptfieldIfSizeZero() throws IOException {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.scriptField("field" + 0,
                new Script(ScriptType.INLINE, MockScriptEngine.NAME, CustomScriptPlugin.DUMMY_SCRIPT, Collections.emptyMap()));
        searchSourceBuilder.size(0);
        final ShardSearchRequest request = new ShardSearchRequest(OriginalIndices.NONE,
            searchRequest, indexShard.shardId(), 1, new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f, -1, null, null);
        try (ReaderContext reader = createReaderContext(indexService, indexShard);
             SearchContext context = service.createContext(reader, request, null, randomBoolean())) {
            assertEquals(0, context.scriptFields().fields().size());
        }
    }

    /**
     * test that creating more than the allowed number of scroll contexts throws an exception
     */
    public void testMaxOpenScrollContexts() throws Exception {
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        // Open all possible scrolls, clear some of them, then open more until the limit is reached
        LinkedList<String> clearScrollIds = new LinkedList<>();

        for (int i = 0; i < SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY); i++) {
            SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();

            if (randomInt(4) == 0) clearScrollIds.addLast(searchResponse.getScrollId());
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.setScrollIds(clearScrollIds);
        client().clearScroll(clearScrollRequest);

        for (int i = 0; i < clearScrollIds.size(); i++) {
            client().prepareSearch("index").setSize(1).setScroll("1m").get();
        }

        final ShardScrollRequestTest request = new ShardScrollRequestTest(indexShard.shardId());
        HavenaskException ex = expectThrows(HavenaskException.class,
            () -> service.createAndPutReaderContext(
                request, indexService, indexShard, indexShard.acquireSearcherSupplier(), randomBoolean()));
        assertEquals(
            "Trying to create too many scroll contexts. Must be less than or equal to: [" +
                SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY) + "]. " +
                "This limit can be set by changing the [search.max_open_scroll_context] setting.",
            ex.getMessage());

        service.freeAllScrollContexts();
    }

    public void testOpenScrollContextsConcurrently() throws Exception {
        createIndex("index");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);

        final int maxScrollContexts = SearchService.MAX_OPEN_SCROLL_CONTEXT.get(Settings.EMPTY);
        final SearchService searchService = getInstanceFromNode(SearchService.class);
        Thread[] threads = new Thread[randomIntBetween(2, 8)];
        CountDownLatch latch = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                    for (; ; ) {
                        final Engine.SearcherSupplier reader = indexShard.acquireSearcherSupplier();
                        try {
                            searchService.createAndPutReaderContext(
                                new ShardScrollRequestTest(indexShard.shardId()), indexService, indexShard, reader, true);
                        } catch (HavenaskException e) {
                            assertThat(e.getMessage(), equalTo(
                                "Trying to create too many scroll contexts. Must be less than or equal to: " +
                                    "[" + maxScrollContexts + "]. " +
                                    "This limit can be set by changing the [search.max_open_scroll_context] setting."));
                            return;
                        }
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].setName("havenask[node_s_0][search]");
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(searchService.getActiveContexts(), equalTo(maxScrollContexts));
        searchService.freeAllScrollContexts();
    }

    public static class FailOnRewriteQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return singletonList(new QuerySpec<>("fail_on_rewrite_query", FailOnRewriteQueryBuilder::new, parseContext -> {
                throw new UnsupportedOperationException("No query parser for this plugin");
            }));
        }
    }

    public static class FailOnRewriteQueryBuilder extends AbstractQueryBuilder<FailOnRewriteQueryBuilder> {

        public FailOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        public FailOnRewriteQueryBuilder() {
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
            if (queryRewriteContext.convertToShardContext() != null) {
                throw new IllegalStateException("Fail on rewrite phase");
            }
            return this;
        }

        @Override
        protected void doWriteTo(StreamOutput out) {
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {
        }

        @Override
        protected Query doToQuery(QueryShardContext context) {
            return null;
        }

        @Override
        protected boolean doEquals(FailOnRewriteQueryBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return null;
        }
    }

    private static class ShardScrollRequestTest extends ShardSearchRequest {
        private Scroll scroll;

        ShardScrollRequestTest(ShardId shardId) {
            super(OriginalIndices.NONE, new SearchRequest().allowPartialSearchResults(true),
                shardId, 1, new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null);
            this.scroll = new Scroll(TimeValue.timeValueMinutes(1));
        }

        @Override
        public Scroll scroll() {
            return this.scroll;
        }
    }

    public void testCanMatch() throws Exception {
        createIndex("index");
        final SearchService service = getInstanceFromNode(SearchService.class);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder());
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(0)));
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1,  null, null)).canMatch());
        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new GlobalAggregationBuilder("test")));
        assertTrue(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());

        searchRequest.source(new SearchSourceBuilder().query(new MatchNoneQueryBuilder()));
        assertFalse(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null)).canMatch());
        assertEquals(0, numWrapInvocations.get());

        ShardSearchRequest request = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null);

        /*
         * Checks that canMatch takes into account the alias filter
         */
        // the source cannot be rewritten to a match_none
        searchRequest.indices("alias").source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
        assertFalse(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(new TermQueryBuilder("foo", "bar"), "alias"), 1f, -1, null, null)).canMatch());
        // the source can match and can be rewritten to a match_none, but not the alias filter
        final IndexResponse response = client().prepareIndex("index", "_doc", "1").setSource("id", "1").get();
        assertEquals(RestStatus.CREATED, response.status());
        searchRequest.indices("alias").source(new SearchSourceBuilder().query(new TermQueryBuilder("id", "1")));
        assertFalse(service.canMatch(new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(), 1,
            new AliasFilter(new TermQueryBuilder("foo", "bar"), "alias"), 1f, -1, null, null)).canMatch());

        CountDownLatch latch = new CountDownLatch(1);
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
        service.executeQueryPhase(request, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
            @Override
            public void onResponse(SearchPhaseResult searchPhaseResult) {
                try {
                    // make sure that the wrapper is called when the query is actually executed
                    assertEquals(1, numWrapInvocations.get());
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    throw new AssertionError(e);
                } finally {
                    latch.countDown();
                }
            }
        });
        latch.await();
    }

    public void testCanRewriteToMatchNone() {
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new GlobalAggregationBuilder("test"))));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder()));
        assertFalse(SearchService.canRewriteToMatchNone(null));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(0))));
        assertTrue(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar"))));
        assertTrue(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(1))));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new MatchNoneQueryBuilder())
            .aggregation(new TermsAggregationBuilder("test").userValueTypeHint(ValueType.STRING).minDocCount(1))
            .suggest(new SuggestBuilder())));
        assertFalse(SearchService.canRewriteToMatchNone(new SearchSourceBuilder().query(new TermQueryBuilder("foo", "bar"))
            .suggest(new SuggestBuilder())));
    }

    public void testSetSearchThrottled() {
        createIndex("throttled_threadpool_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), "true"))
            .actionGet();
        final SearchService service = getInstanceFromNode(SearchService.class);
        Index index = resolveIndex("throttled_threadpool_index");
        assertTrue(service.getIndicesService().indexServiceSafe(index).getIndexSettings().isSearchThrottled());
        client().prepareIndex("throttled_threadpool_index", "_doc", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("throttled_threadpool_index")
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).setSize(1).get();
        assertSearchHits(searchResponse, "1");
        // we add a search action listener in a plugin above to assert that this is actually used
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), "false"))
            .actionGet();

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
            client().admin().indices().prepareUpdateSettings("throttled_threadpool_index").setSettings(Settings.builder().put(IndexSettings
                .INDEX_SEARCH_THROTTLED.getKey(), false)).get());
        assertEquals("can not update private setting [index.search.throttled]; this setting is managed by Havenask",
            iae.getMessage());
        assertFalse(service.getIndicesService().indexServiceSafe(index).getIndexSettings().isSearchThrottled());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        ShardSearchRequest req = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, new ShardId(index, 0), 1,
            new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null);
        Thread currentThread = Thread.currentThread();
        // we still make sure can match is executed on the network thread
        service.canMatch(req, ActionListener.wrap(r -> assertSame(Thread.currentThread(), currentThread), e -> fail("unexpected")));
    }

    public void testExpandSearchThrottled() {
        createIndex("throttled_threadpool_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("throttled_threadpool_index",
                IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), "true"))
            .actionGet();

        client().prepareIndex("throttled_threadpool_index", "_doc", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch().get(), 1L);
        assertHitCount(client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).get(), 1L);
    }

    public void testExpandSearchFrozen() {
        createIndex("frozen_index");
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("frozen_index",
                "index.frozen", "true"))
            .actionGet();

        client().prepareIndex("frozen_index", "_doc", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch().get(), 0L);
        assertHitCount(client().prepareSearch().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).get(), 1L);
    }

    public void testCreateReduceContext() {
        SearchService service = getInstanceFromNode(SearchService.class);
        InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(new SearchRequest());
        {
            InternalAggregation.ReduceContext reduceContext = reduceContextBuilder.forFinalReduction();
            expectThrows(MultiBucketConsumerService.TooManyBucketsException.class,
                () -> reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1));
        }
        {
            InternalAggregation.ReduceContext reduceContext = reduceContextBuilder.forPartialReduction();
            reduceContext.consumeBucketsAndMaybeBreak(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1);
        }
    }

    public void testCreateSearchContext() throws IOException {
        String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        ShardId shardId = new ShardId(indexService.index(), 0);
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        ShardSearchRequest request = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, shardId,
            indexService.numberOfShards(), AliasFilter.EMPTY, 1f, nowInMillis, clusterAlias, Strings.EMPTY_ARRAY);
        try (DefaultSearchContext searchContext = service.createSearchContext(request, new TimeValue(System.currentTimeMillis()))) {
            SearchShardTarget searchShardTarget = searchContext.shardTarget();
            QueryShardContext queryShardContext = searchContext.getQueryShardContext();
            String expectedIndexName = clusterAlias == null ? index : clusterAlias + ":" + index;
            assertEquals(expectedIndexName, queryShardContext.getFullyQualifiedIndex().getName());
            assertEquals(expectedIndexName, searchShardTarget.getFullyQualifiedIndexName());
            assertEquals(clusterAlias, searchShardTarget.getClusterAlias());
            assertEquals(shardId, searchShardTarget.getShardId());
            assertSame(searchShardTarget, searchContext.dfsResult().getSearchShardTarget());
            assertSame(searchShardTarget, searchContext.queryResult().getSearchShardTarget());
            assertSame(searchShardTarget, searchContext.fetchResult().getSearchShardTarget());
        }
    }

    /**
     * While we have no NPE in DefaultContext constructor anymore, we still want to guard against it (or other failures) in the future to
     * avoid leaking searchers.
     */
    public void testCreateSearchContextFailure() throws Exception {
        final String index = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        final IndexService indexService = createIndex(index);
        final SearchService service = getInstanceFromNode(SearchService.class);
        final ShardId shardId = new ShardId(indexService.index(), 0);
        final ShardSearchRequest request = new ShardSearchRequest(shardId, new String[0], 0, null) {
            @Override
            public SearchType searchType() {
                // induce an artificial NPE
                throw new NullPointerException("expected");
            }
        };
        try (ReaderContext reader = createReaderContext(indexService, indexService.getShard(shardId.id()))) {
            NullPointerException e = expectThrows(NullPointerException.class,
                () -> service.createContext(reader, request, null, randomBoolean()));
            assertEquals("expected", e.getMessage());
        }
        // Needs to busily assert because Engine#refreshNeeded can increase the refCount.
        assertBusy(() ->
            assertEquals("should have 2 store refs (IndexService + InternalEngine)", 2, indexService.getShard(0).store().refCount()));
    }

    public void testMatchNoDocsEmptyResponse() throws InterruptedException {
        createIndex("index");
        Thread currentThread = Thread.currentThread();
        SearchService service = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        SearchRequest searchRequest = new SearchRequest()
            .allowPartialSearchResults(false)
            .source(new SearchSourceBuilder()
                .aggregation(AggregationBuilders.count("count").field("value")));
        ShardSearchRequest shardRequest = new ShardSearchRequest(OriginalIndices.NONE, searchRequest, indexShard.shardId(),
            5, AliasFilter.EMPTY, 1.0f, 0, null, null);
        SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.source().query(new MatchAllQueryBuilder());
            service.executeQueryPhase(shardRequest, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        assertNotSame(Thread.currentThread(), currentThread);
                        assertThat(Thread.currentThread().getName(), startsWith("havenask[node_s_0][search]"));
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertFalse(result.queryResult().isNull());
                        assertNotNull(result.queryResult().topDocs());
                        assertNotNull(result.queryResult().aggregations());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        throw new AssertionError(exc);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.source().query(new MatchNoneQueryBuilder());
            service.executeQueryPhase(shardRequest, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        assertNotSame(Thread.currentThread(), currentThread);
                        assertThat(Thread.currentThread().getName(), startsWith("havenask[node_s_0][search]"));
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertFalse(result.queryResult().isNull());
                        assertNotNull(result.queryResult().topDocs());
                        assertNotNull(result.queryResult().aggregations());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    try {
                        throw new AssertionError(exc);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }

        {
            CountDownLatch latch = new CountDownLatch(1);
            shardRequest.canReturnNullResponseIfMatchNoDocs(true);
            service.executeQueryPhase(shardRequest, randomBoolean(), task, new ActionListener<SearchPhaseResult>() {
                @Override
                public void onResponse(SearchPhaseResult result) {
                    try {
                        // make sure we don't use the search threadpool
                        assertSame(Thread.currentThread(), currentThread);
                        assertThat(result, instanceOf(QuerySearchResult.class));
                        assertTrue(result.queryResult().isNull());
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        throw new AssertionError(e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        }
    }

    public void testDeleteIndexWhileSearch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(1, 20);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "_doc").setSource("f", "v").get();
        }
        client().admin().indices().prepareRefresh("test").get();
        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread[] searchers = new Thread[randomIntBetween(1, 4)];
        CountDownLatch latch = new CountDownLatch(searchers.length);
        for (int i = 0; i < searchers.length; i++) {
            searchers[i] = new Thread(() -> {
                latch.countDown();
                while (stopped.get() == false) {
                    try {
                        client().prepareSearch("test").setRequestCache(false).get();
                    } catch (Exception ignored) {
                        return;
                    }
                }
            });
            searchers[i].start();
        }
        latch.await();
        client().admin().indices().prepareDelete("test").get();
        stopped.set(true);
        for (Thread searcher : searchers) {
            searcher.join();
        }
    }

    public void testLookUpSearchContext() throws Exception {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        IndexShard indexShard = indexService.getShard(0);
        List<ShardSearchContextId> contextIds = new ArrayList<>();
        int numContexts = randomIntBetween(1, 10);
        CountDownLatch latch = new CountDownLatch(1);
        indexShard.getThreadPool().executor(ThreadPool.Names.SEARCH).execute(() -> {
            try {
                for (int i = 0; i < numContexts; i++) {
                    ShardSearchRequest request = new ShardSearchRequest(
                        OriginalIndices.NONE, new SearchRequest().allowPartialSearchResults(true),
                        indexShard.shardId(), 1, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, -1, null, null);
                    final ReaderContext context = searchService.createAndPutReaderContext(request, indexService, indexShard,
                        indexShard.acquireSearcherSupplier(), randomBoolean());
                    assertThat(context.id().getId(), equalTo((long) (i + 1)));
                    contextIds.add(context.id());
                }
                assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                while (contextIds.isEmpty() == false) {
                    final ShardSearchContextId contextId = randomFrom(contextIds);
                    expectThrows(SearchContextMissingException.class,
                        () -> searchService.freeReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), contextId.getId())));
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                    if (randomBoolean()) {
                        assertTrue(searchService.freeReaderContext(contextId));
                    } else {
                        assertTrue(searchService.freeReaderContext((
                            new ShardSearchContextId(contextId.getSessionId(), contextId.getId()))));
                    }
                    contextIds.remove(contextId);
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                    assertFalse(searchService.freeReaderContext(contextId));
                    assertThat(searchService.getActiveContexts(), equalTo(contextIds.size()));
                }
            } finally {
                latch.countDown();
            }
        });
        latch.await();
    }

    public void testOpenReaderContext() {
        createIndex("index");
        SearchService searchService = getInstanceFromNode(SearchService.class);
        PlainActionFuture<ShardSearchContextId> future = new PlainActionFuture<>();
        searchService.openReaderContext(new ShardId(resolveIndex("index"), 0), TimeValue.timeValueMinutes(between(1, 10)), future);
        future.actionGet();
        assertThat(searchService.getActiveContexts(), equalTo(1));
        assertTrue(searchService.freeReaderContext(future.actionGet()));
    }

    private ReaderContext createReaderContext(IndexService indexService, IndexShard indexShard) {
        return new ReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
            indexService, indexShard, indexShard.acquireSearcherSupplier(), randomNonNegativeLong(), false);
    }
}
