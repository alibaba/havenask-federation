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

package org.havenask.index;

import org.havenask.action.ActionFuture;
import org.havenask.action.DocWriteResponse;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkRequestBuilder;
import org.havenask.action.bulk.BulkResponse;
import org.havenask.action.delete.DeleteResponse;
import org.havenask.action.index.IndexResponse;
import org.havenask.action.support.WriteRequest.RefreshPolicy;
import org.havenask.action.update.UpdateResponse;
import org.havenask.client.Requests;
import org.havenask.common.settings.Settings;
import org.havenask.plugins.Plugin;
import org.havenask.rest.RestStatus;
import org.havenask.script.MockScriptPlugin;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.test.HavenaskIntegTestCase;

import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.havenask.index.query.QueryBuilders.matchQuery;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoFailures;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoSearchHits;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertSearchHits;

/**
 * Tests that requests with RefreshPolicy.WAIT_UNTIL will be visible when they return.
 */
public class WaitUntilRefreshIT extends HavenaskIntegTestCase {

    @Override
    public Settings indexSettings() {
        // Use a shorter refresh interval to speed up the tests. We'll be waiting on this interval several times.
        return Settings.builder().put(super.indexSettings()).put("index.refresh_interval", "40ms").build();
    }

    @Before
    public void createTestIndex() {
        createIndex("test");
    }

    public void testIndex() {
        IndexResponse index = client().prepareIndex("test", "index", "1").setSource("foo", "bar").setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .get();
        assertEquals(RestStatus.CREATED, index.status());
        assertFalse("request shouldn't have forced a refresh", index.forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");
    }

    public void testDelete() throws InterruptedException, ExecutionException {
        // Index normally
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "bar"));
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Now delete with blockUntilRefresh
        DeleteResponse delete = client().prepareDelete("test", "test", "1").setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).get();
        assertEquals(DocWriteResponse.Result.DELETED, delete.getResult());
        assertFalse("request shouldn't have forced a refresh", delete.forcedRefresh());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get());
    }

    public void testUpdate() throws InterruptedException, ExecutionException {
        // Index normally
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "bar"));
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Update with RefreshPolicy.WAIT_UNTIL
        UpdateResponse update = client().prepareUpdate("test", "test", "1")
            .setDoc(Requests.INDEX_CONTENT_TYPE, "foo", "baz").setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .get();
        assertEquals(2, update.getVersion());
        assertFalse("request shouldn't have forced a refresh", update.forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "baz")).get(), "1");

        // Upsert with RefreshPolicy.WAIT_UNTIL
        update = client().prepareUpdate("test", "test", "2").setDocAsUpsert(true).setDoc(Requests.INDEX_CONTENT_TYPE, "foo", "cat")
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).get();
        assertEquals(1, update.getVersion());
        assertFalse("request shouldn't have forced a refresh", update.forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "cat")).get(), "2");

        // Update-becomes-delete with RefreshPolicy.WAIT_UNTIL
        update = client().prepareUpdate("test", "test", "2").setScript(
            new Script(ScriptType.INLINE, "mockscript", "delete_plz", emptyMap()))
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).get();
        assertEquals(2, update.getVersion());
        assertFalse("request shouldn't have forced a refresh", update.forcedRefresh());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "cat")).get());
    }

    public void testBulk() {
        // Index by bulk with RefreshPolicy.WAIT_UNTIL
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareIndex("test", "test", "1").setSource("foo", "bar"));
        assertBulkSuccess(bulk.get());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");

        // Update by bulk with RefreshPolicy.WAIT_UNTIL
        bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareUpdate("test", "test", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "foo", "baz"));
        assertBulkSuccess(bulk.get());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "baz")).get(), "1");

        // Delete by bulk with RefreshPolicy.WAIT_UNTIL
        bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareDelete("test", "test", "1"));
        assertBulkSuccess(bulk.get());
        assertNoSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get());

        // Update makes a noop
        bulk = client().prepareBulk().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        bulk.add(client().prepareDelete("test", "test", "1"));
        assertBulkSuccess(bulk.get());
    }

    /**
     * Tests that an explicit request makes block_until_refresh return. It doesn't check that block_until_refresh doesn't return until the
     * explicit refresh if the interval is -1 because we don't have that kind of control over refresh. It can happen all on its own.
     */
    public void testNoRefreshInterval() throws InterruptedException, ExecutionException {
        client().admin().indices().prepareUpdateSettings("test").setSettings(singletonMap("index.refresh_interval", -1)).get();
        ActionFuture<IndexResponse> index = client().prepareIndex("test", "index", "1").setSource("foo", "bar")
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute();
        while (false == index.isDone()) {
            client().admin().indices().prepareRefresh("test").get();
        }
        assertEquals(RestStatus.CREATED, index.get().status());
        assertFalse("request shouldn't have forced a refresh", index.get().forcedRefresh());
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get(), "1");
    }

    private void assertBulkSuccess(BulkResponse response) {
        assertNoFailures(response);
        for (BulkItemResponse item : response) {
            assertFalse("request shouldn't have forced a refresh", item.getResponse().forcedRefresh());
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singleton(DeletePlzPlugin.class);
    }

    public static class DeletePlzPlugin extends MockScriptPlugin {
        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("delete_plz", params -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> ctx = (Map<String, Object>) params.get("ctx");
                ctx.put("op", "delete");
                return null;
            });
        }
    }
}
