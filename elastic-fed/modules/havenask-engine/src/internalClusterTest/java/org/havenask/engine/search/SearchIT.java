/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.search;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.havenask.ArpcThreadLeakFilter;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.TestHavenaskEnginePlugin;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.search.SearchResponse;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.HavenaskITTestCase;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.query.HnswQueryBuilder;
import org.havenask.plugins.Plugin;
import org.havenask.search.builder.SearchSourceBuilder;

@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class, ArpcThreadLeakFilter.class })
public class SearchIT extends HavenaskITTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestHavenaskEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSearch() throws Exception {
        String index = "test2";
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"vector\": {\n"
            + "      \"type\": \"dense_vector\",\n"
            + "      \"dims\": 2\n"
            + "    }\n"
            + "  }\n"
            + "}";

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                        .build()
                )
                .addMapping("_doc", mapping, XContentType.JSON)
                .get()
                .isAcknowledged()
        );

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = client().admin().cluster().health(new ClusterHealthRequest(index)).get();
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        HnswQueryBuilder hnswQueryBuilder = new HnswQueryBuilder("vector", new float[] { 1.5f, 2.5f }, 10);
        searchSourceBuilder.query(hnswQueryBuilder);
        SearchResponse searchResponse = client().prepareSearch(index).setSource(searchSourceBuilder).get();
        assertEquals(searchResponse.getHits().getTotalHits().value, 2);
    }
}
