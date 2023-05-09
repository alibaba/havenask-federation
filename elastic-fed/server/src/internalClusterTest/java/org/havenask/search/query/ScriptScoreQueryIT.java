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

package org.havenask.search.query;

import org.havenask.HavenaskException;
import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.settings.Settings;
import org.havenask.index.fielddata.ScriptDocValues;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.RangeQueryBuilder;
import org.havenask.plugins.Plugin;
import org.havenask.script.MockScriptPlugin;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.havenask.index.query.QueryBuilders.boolQuery;
import static org.havenask.index.query.QueryBuilders.matchQuery;
import static org.havenask.index.query.QueryBuilders.scriptScoreQuery;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertFirstHit;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoFailures;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertOrderedSearchHits;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertSecondHit;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertThirdHit;
import static org.havenask.test.hamcrest.HavenaskAssertions.hasScore;

public class ScriptScoreQueryIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("doc['field2'].value * param1", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles field2Values = (ScriptDocValues.Doubles) doc.get("field2");
                Double param1 = (Double) vars.get("param1");
                return field2Values.getValue() * param1;
            });
            return scripts;
        }
    }

    // test that script_score works as expected:
    // 1) only matched docs retrieved
    // 2) score is calculated based on a script with params
    // 3) min score applied
    public void testScriptScore() {
        assertAcked(
            prepareCreate("test-index").addMapping("_doc", "field1", "type=text", "field2", "type=double")
        );
        int docCount = 10;
        for (int i = 1; i <= docCount; i++) {
            client().prepareIndex("test-index", "_doc", "" + i)
                .setSource("field1", "text" + (i % 2), "field2", i )
                .get();
        }
        refresh();

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        SearchResponse resp = client()
            .prepareSearch("test-index")
            .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script))
            .get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "8", "6", "4", "2");
        assertFirstHit(resp, hasScore(1.0f));
        assertSecondHit(resp, hasScore(0.8f));
        assertThirdHit(resp, hasScore(0.6f));

        // applying min score
        resp = client()
            .prepareSearch("test-index")
            .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script).setMinScore(0.6f))
            .get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "8", "6");
    }

    public void testScriptScoreBoolQuery() {
        assertAcked(
            prepareCreate("test-index").addMapping("_doc", "field1", "type=text", "field2", "type=double")
        );
        int docCount = 10;
        for (int i = 1; i <= docCount; i++) {
            client().prepareIndex("test-index", "_doc", "" + i)
                .setSource("field1", "text" + i, "field2", i)
                .get();
        }
        refresh();

        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        QueryBuilder boolQuery = boolQuery().should(matchQuery("field1", "text1")).should(matchQuery("field1", "text10"));
        SearchResponse resp = client()
            .prepareSearch("test-index")
            .setQuery(scriptScoreQuery(boolQuery, script))
            .get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "10", "1");
        assertFirstHit(resp, hasScore(1.0f));
        assertSecondHit(resp, hasScore(0.1f));
    }


    // test that when the internal query is rewritten script_score works well
    public void testRewrittenQuery() {
        assertAcked(
            prepareCreate("test-index2")
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .addMapping("_doc", "field1", "type=date", "field2", "type=double")
        );
        client().prepareIndex("test-index2", "_doc", "1").setSource("field1", "2019-09-01", "field2", 1).get();
        client().prepareIndex("test-index2", "_doc", "2").setSource("field1", "2019-10-01", "field2", 2).get();
        client().prepareIndex("test-index2", "_doc", "3").setSource("field1", "2019-11-01", "field2", 3).get();
        refresh();

        RangeQueryBuilder rangeQB = new RangeQueryBuilder("field1").from("2019-01-01"); // the query should be rewritten to from:null
        Map<String, Object> params = new HashMap<>();
        params.put("param1", 0.1);
        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
        SearchResponse resp = client()
            .prepareSearch("test-index2")
            .setQuery(scriptScoreQuery(rangeQB, script))
            .get();
        assertNoFailures(resp);
        assertOrderedSearchHits(resp, "3", "2", "1");
    }

    public void testDisallowExpensiveQueries() {
        try {
            assertAcked(
                    prepareCreate("test-index").addMapping("_doc", "field1", "type=text", "field2", "type=double")
            );
            int docCount = 10;
            for (int i = 1; i <= docCount; i++) {
                client().prepareIndex("test-index", "_doc").setId("" + i)
                        .setSource("field1", "text" + (i % 2), "field2", i)
                        .get();
            }
            refresh();

            Map<String, Object> params = new HashMap<>();
            params.put("param1", 0.1);

            // Execute with search.allow_expensive_queries = null => default value = true => success
            Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['field2'].value * param1", params);
            SearchResponse resp = client()
                    .prepareSearch("test-index")
                    .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script))
                    .get();
            assertNoFailures(resp);

            // Set search.allow_expensive_queries to "false" => assert failure
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", false));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            HavenaskException e = expectThrows(HavenaskException.class,
                    () -> client()
                            .prepareSearch("test-index")
                            .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script))
                            .get());
            assertEquals("[script score] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                    e.getCause().getMessage());

            // Set search.allow_expensive_queries to "true" => success
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            resp = client()
                    .prepareSearch("test-index")
                    .setQuery(scriptScoreQuery(matchQuery("field1", "text0"), script))
                    .get();
            assertNoFailures(resp);
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }
}
