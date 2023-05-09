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

import org.havenask.HavenaskException;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.plugins.Plugin;
import org.havenask.script.MockScriptPlugin;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.havenask.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.havenask.index.query.QueryBuilders.scriptQuery;
import static org.havenask.search.SearchTimeoutIT.ScriptedTimeoutPlugin.SCRIPT_NAME;
import static org.hamcrest.Matchers.equalTo;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.SUITE)
public class SearchTimeoutIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    public void testSimpleTimeout() throws Exception {
        for (int i = 0; i < 32; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value").get();
        }
        refresh("test");

        SearchResponse searchResponse = client().prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(
                    new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .setAllowPartialSearchResults(true)
                .get();
        assertThat(searchResponse.isTimedOut(), equalTo(true));
    }

    public void testPartialResultsIntolerantTimeout() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        HavenaskException ex = expectThrows(HavenaskException.class, () ->
            client().prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                    .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
                    .get()
                );
        assertTrue(ex.toString().contains("Time exceeded"));
    }

    public static class ScriptedTimeoutPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_timeout";
        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
