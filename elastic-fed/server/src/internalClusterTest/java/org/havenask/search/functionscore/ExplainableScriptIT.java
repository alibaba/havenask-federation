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

package org.havenask.search.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.search.SearchType;
import org.havenask.common.lucene.search.function.CombineFunction;
import org.havenask.common.settings.Settings;
import org.havenask.index.fielddata.ScriptDocValues;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.ScriptPlugin;
import org.havenask.script.ExplainableScoreScript;
import org.havenask.script.ScoreScript;
import org.havenask.script.Script;
import org.havenask.script.ScriptContext;
import org.havenask.script.ScriptEngine;
import org.havenask.script.ScriptType;
import org.havenask.search.SearchHit;
import org.havenask.search.SearchHits;
import org.havenask.search.lookup.SearchLookup;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import org.havenask.test.HavenaskIntegTestCase.Scope;
import org.havenask.test.hamcrest.HavenaskAssertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.havenask.client.Requests.searchRequest;
import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.index.query.QueryBuilders.functionScoreQuery;
import static org.havenask.index.query.QueryBuilders.termQuery;
import static org.havenask.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.havenask.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ExplainableScriptIT extends HavenaskIntegTestCase {

    public static class ExplainableScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "test";
                }

                @Override
                public <T> T compile(
                    String scriptName,
                    String scriptSource,
                    ScriptContext<T> context,
                    Map<String, String> params
                ) {
                    assert scriptSource.equals("explainable_script");
                    assert context == ScoreScript.CONTEXT;
                    ScoreScript.Factory factory = (params1, lookup) -> new ScoreScript.LeafFactory() {
                        @Override
                        public boolean needs_score() {
                            return false;
                        }

                        @Override
                        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
                            return new MyScript(params1, lookup, ctx);
                        }
                    };
                    return context.factoryClazz.cast(factory);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Collections.singleton(ScoreScript.CONTEXT);
                }
            };
        }
    }

    static class MyScript extends ScoreScript implements ExplainableScoreScript {

        MyScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
            super(params, lookup, leafContext);
        }

        @Override
        public Explanation explain(Explanation subQueryScore) throws IOException {
            Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
            return Explanation.match((float) (execute(null)), "This script returned " + execute(null), scoreExp);
        }

        @Override
        public double execute(ExplanationHolder explanation) {
            return ((Number) ((ScriptDocValues) getDoc().get("number_field")).get(0)).doubleValue();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ExplainableScriptPlugin.class);
    }

    public void testExplainScript() throws InterruptedException, IOException, ExecutionException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            indexRequests.add(client().prepareIndex("test", "type").setId(Integer.toString(i)).setSource(
                    jsonBuilder().startObject().field("number_field", i).field("text", "text").endObject()));
        }
        indexRandom(true, true, indexRequests);
        client().admin().indices().prepareRefresh().get();
        ensureYellow();
        SearchResponse response = client().search(searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("text", "text"),
                                        scriptFunction(
                                            new Script(ScriptType.INLINE, "test", "explainable_script", Collections.emptyMap())))
                                        .boostMode(CombineFunction.REPLACE)))).actionGet();

        HavenaskAssertions.assertNoFailures(response);
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits().value, equalTo(20L));
        int idCounter = 19;
        for (SearchHit hit : hits.getHits()) {
            assertThat(hit.getId(), equalTo(Integer.toString(idCounter)));
            assertThat(hit.getExplanation().toString(), containsString(Double.toString(idCounter)));
            assertThat(hit.getExplanation().toString(), containsString("1 = n"));
            assertThat(hit.getExplanation().toString(), containsString("1 = N"));
            assertThat(hit.getExplanation().getDetails().length, equalTo(2));
            idCounter--;
        }
    }
}
