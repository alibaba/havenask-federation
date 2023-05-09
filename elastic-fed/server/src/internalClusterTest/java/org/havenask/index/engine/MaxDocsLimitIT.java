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

package org.havenask.index.engine;

import org.apache.lucene.index.IndexWriterMaxDocsChanger;

import org.havenask.action.index.IndexResponse;
import org.havenask.action.search.SearchResponse;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.IndexSettings;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.translog.Translog;
import org.havenask.plugins.EnginePlugin;
import org.havenask.plugins.Plugin;
import org.havenask.rest.RestStatus;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.hamcrest.HavenaskAssertions;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MaxDocsLimitIT extends HavenaskIntegTestCase {

    private static final AtomicInteger maxDocs = new AtomicInteger();

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> {
                assert maxDocs.get() > 0 : "maxDocs is unset";
                return EngineTestCase.createEngine(config, maxDocs.get());
            });
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestEnginePlugin.class);
        return plugins;
    }

    @Before
    public void setMaxDocs() {
        maxDocs.set(randomIntBetween(10, 100)); // Do not set this too low as we can fail to write the cluster state
        IndexWriterMaxDocsChanger.setMaxDocs(maxDocs.get());
    }

    @After
    public void restoreMaxDocs() {
        IndexWriterMaxDocsChanger.restoreMaxDocs();
    }

    public void testMaxDocsLimit() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)));
        IndexingResult indexingResult = indexDocs(maxDocs.get(), 1);
        assertThat(indexingResult.numSuccess, equalTo(maxDocs.get()));
        assertThat(indexingResult.numFailures, equalTo(0));
        int rejectedRequests = between(1, 10);
        indexingResult = indexDocs(rejectedRequests, between(1, 8));
        assertThat(indexingResult.numFailures, equalTo(rejectedRequests));
        assertThat(indexingResult.numSuccess, equalTo(0));
        final IllegalArgumentException deleteError = expectThrows(IllegalArgumentException.class,
            () -> client().prepareDelete("test", "_doc", "any-id").get());
        assertThat(deleteError.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs.get() + "]"));
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE).setSize(0).get();
        HavenaskAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs.get()));
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").get();
        }
        internalCluster().fullRestart();
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureGreen("test");
        searchResponse = client().prepareSearch("test").setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE).setSize(0).get();
        HavenaskAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs.get()));
    }

    public void testMaxDocsLimitConcurrently() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)));
        IndexingResult indexingResult = indexDocs(between(maxDocs.get() + 1, maxDocs.get() * 2), between(2, 8));
        assertThat(indexingResult.numFailures, greaterThan(0));
        assertThat(indexingResult.numSuccess, both(greaterThan(0)).and(lessThanOrEqualTo(maxDocs.get())));
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE).setSize(0).get();
        HavenaskAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) indexingResult.numSuccess));
        int totalSuccess = indexingResult.numSuccess;
        while (totalSuccess < maxDocs.get()) {
            indexingResult = indexDocs(between(1, 10), between(1, 8));
            assertThat(indexingResult.numSuccess, greaterThan(0));
            totalSuccess += indexingResult.numSuccess;
        }
        if (randomBoolean()) {
            indexingResult = indexDocs(between(1, 10), between(1, 8));
            assertThat(indexingResult.numSuccess, equalTo(0));
        }
        client().admin().indices().prepareRefresh("test").get();
        searchResponse = client().prepareSearch("test").setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE).setSize(0).get();
        HavenaskAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) totalSuccess));
    }

    static final class IndexingResult {
        final int numSuccess;
        final int numFailures;

        IndexingResult(int numSuccess, int numFailures) {
            this.numSuccess = numSuccess;
            this.numFailures = numFailures;
        }
    }

    static IndexingResult indexDocs(int numRequests, int numThreads) throws Exception {
        final AtomicInteger completedRequests = new AtomicInteger();
        final AtomicInteger numSuccess = new AtomicInteger();
        final AtomicInteger numFailure = new AtomicInteger();
        Thread[] indexers = new Thread[numThreads];
        Phaser phaser = new Phaser(indexers.length);
        for (int i = 0; i < indexers.length; i++) {
            indexers[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                while (completedRequests.incrementAndGet() <= numRequests) {
                    try {
                        final IndexResponse resp = client().prepareIndex("test", "_doc").setSource("{}", XContentType.JSON).get();
                        numSuccess.incrementAndGet();
                        assertThat(resp.status(), equalTo(RestStatus.CREATED));
                    } catch (IllegalArgumentException e) {
                        numFailure.incrementAndGet();
                        assertThat(e.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs.get() + "]"));
                    }
                }
            });
            indexers[i].start();
        }
        for (Thread indexer : indexers) {
            indexer.join();
        }
        internalCluster().assertNoInFlightDocsInEngine();
        return new IndexingResult(numSuccess.get(), numFailure.get());
    }
}
