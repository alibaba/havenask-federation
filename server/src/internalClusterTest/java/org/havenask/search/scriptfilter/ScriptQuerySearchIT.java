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

package org.havenask.search.scriptfilter;

import org.havenask.HavenaskException;
import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.index.IndexModule;
import org.havenask.index.fielddata.ScriptDocValues;
import org.havenask.plugins.Plugin;
import org.havenask.script.MockScriptPlugin;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.search.sort.SortOrder;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.index.query.QueryBuilders.scriptQuery;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.SUITE)
public class ScriptQuerySearchIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class, InternalSettingsPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("doc['num1'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return doc.get("num1");
            });

            scripts.put("doc['num1'].value > 1", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles num1 = (ScriptDocValues.Doubles) doc.get("num1");
                return num1.getValue() > 1;
            });

            scripts.put("doc['num1'].value > param1", vars -> {
                Integer param1 = (Integer) vars.get("param1");

                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles num1 = (ScriptDocValues.Doubles) doc.get("num1");
                return num1.getValue() > param1;
            });

            scripts.put("doc['binaryData'].get(0).length", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.BytesRefs) doc.get("binaryData")).get(0).length;
            });

            scripts.put("doc['binaryData'].get(0).length > 15", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.BytesRefs) doc.get("binaryData")).get(0).length > 15;
            });

            return scripts;
        }
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings())
                // aggressive filter caching so that we can assert on the number of iterations of the script filters
                .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), true)
                .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
                .build();
    }

    public void testCustomScriptBinaryField() throws Exception {
        final byte[] randomBytesDoc1 = getRandomBytes(15);
        final byte[] randomBytesDoc2 = getRandomBytes(16);

        assertAcked(
                client().admin().indices().prepareCreate("my-index")
                        .addMapping("my-type", createMappingSource("binary"))
                        .setSettings(indexSettings())
        );
        client().prepareIndex("my-index", "my-type", "1")
                .setSource(jsonBuilder().startObject().field("binaryData",
                        Base64.getEncoder().encodeToString(randomBytesDoc1)).endObject())
                .get();
        flush();
        client().prepareIndex("my-index", "my-type", "2")
                .setSource(jsonBuilder().startObject().field("binaryData",
                        Base64.getEncoder().encodeToString(randomBytesDoc2)).endObject())
                .get();
        flush();
        refresh();

        SearchResponse response = client().prepareSearch()
                .setQuery(scriptQuery(
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['binaryData'].get(0).length > 15", emptyMap())))
                .addScriptField("sbinaryData",
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['binaryData'].get(0).length", emptyMap()))
                .get();

        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(0).getFields().get("sbinaryData").getValues().get(0), equalTo(16));

    }

    private byte[] getRandomBytes(int len) {
        final byte[] randomBytes = new byte[len];
        random().nextBytes(randomBytes);
        return randomBytes;
    }

    private XContentBuilder createMappingSource(String fieldType) throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("my-type")
                .startObject("properties")
                .startObject("binaryData")
                .field("type", fieldType)
                .field("doc_values", "true")
                .endObject()
                .endObject()
                .endObject().endObject();
    }

    public void testCustomScriptBoost() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())
                .get();
        flush();
        client().prepareIndex("test", "type1", "2")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).endObject())
                .get();
        flush();
        client().prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).endObject())
                .get();
        refresh();

        logger.info("running doc['num1'].value > 1");
        SearchResponse response = client().prepareSearch()
                .setQuery(scriptQuery(
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > 1", Collections.emptyMap())))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1",
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value", Collections.emptyMap()))
                .get();

        assertThat(response.getHits().getTotalHits().value, equalTo(2L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(1).getFields().get("sNum1").getValues().get(0), equalTo(3.0));

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 2);

        logger.info("running doc['num1'].value > param1");
        response = client()
                .prepareSearch()
                .setQuery(scriptQuery(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > param1", params)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1",
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value", Collections.emptyMap()))
                .get();

        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(3.0));

        params = new HashMap<>();
        params.put("param1", -1);
        logger.info("running doc['num1'].value > param1");
        response = client()
                .prepareSearch()
                .setQuery(scriptQuery(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > param1", params)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1",
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value", Collections.emptyMap()))
                .get();

        assertThat(response.getHits().getTotalHits().value, equalTo(3L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(1.0));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(1).getFields().get("sNum1").getValues().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(2).getFields().get("sNum1").getValues().get(0), equalTo(3.0));
    }

    public void testDisallowExpensiveQueries() {
        try {
            assertAcked(
                    prepareCreate("test-index").addMapping("_doc", "num1", "type=double")
            );
            int docCount = 10;
            for (int i = 1; i <= docCount; i++) {
                client().prepareIndex("test-index", "_doc").setId("" + i)
                        .setSource("num1", i)
                        .get();
            }
            refresh();

            // Execute with search.allow_expensive_queries = null => default value = false => success
            Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > 1",
                    Collections.emptyMap());
            SearchResponse resp = client().prepareSearch("test-index")
                    .setQuery(scriptQuery(script))
                    .get();
            assertNoFailures(resp);

            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", false));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            // Set search.allow_expensive_queries to "false" => assert failure
            HavenaskException e = expectThrows(HavenaskException.class,
                    () -> client()
                            .prepareSearch("test-index")
                            .setQuery(scriptQuery(script))
                            .get());
            assertEquals("[script] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                    e.getCause().getMessage());

            // Set search.allow_expensive_queries to "true" => success
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            resp = client().prepareSearch("test-index")
                    .setQuery(scriptQuery(script))
                    .get();
            assertNoFailures(resp);
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }

    private static AtomicInteger scriptCounter = new AtomicInteger(0);

    public static int incrementScriptCounter() {
        return scriptCounter.incrementAndGet();
    }
}
