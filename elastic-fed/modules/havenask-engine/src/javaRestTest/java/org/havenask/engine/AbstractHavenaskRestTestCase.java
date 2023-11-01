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

package org.havenask.engine;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.delete.DeleteRequest;
import org.havenask.action.get.GetRequest;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.update.UpdateRequest;
import org.havenask.client.RequestOptions;
import org.havenask.client.RestClient;
import org.havenask.client.RestHighLevelClient;
import org.havenask.client.ha.SqlRequest;
import org.havenask.client.ha.SqlResponse;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentType;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.search.SearchModule;
import org.havenask.test.rest.HavenaskRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

public abstract class AbstractHavenaskRestTestCase extends HavenaskRestTestCase {
    private static RestHighLevelClient restHighLevelClient;

    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        IOUtils.close(restHighLevelClient);
        restHighLevelClient = null;
    }

    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public static RestHighLevelClient highLevelClient() {
        return restHighLevelClient;
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
        }
    }

    protected void waitIndexGreen(String index) throws Exception {
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(ClusterHealthStatus.GREEN, clusterHealthResponse.getStatus());
        }, 2, TimeUnit.MINUTES);
    }

    //TODO 写入后立刻查询有可能会查不到doc，因此等待一会儿，可能是由havenask写入队列造成的，待确认
    protected void waitResponseExists(String index, String id) throws Exception {
        assertBusy(() -> {
            GetResponse getResponse = getDocById(index, id);
            assertEquals(true, getResponse.isExists());
        }, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    protected boolean createTestIndex(String index, Settings settings, Object map) throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        if (settings != null) {
            createIndexRequest.settings(settings);
        }
        if (map != null) {
            if (map instanceof java.util.Map) {
                createIndexRequest.mapping((java.util.Map<String, ?>) map);
            } else if (map instanceof XContentBuilder) {
                createIndexRequest.mapping((XContentBuilder) map);
            } else {
                throw new IllegalArgumentException("Unsupported map type: " + map.getClass());
            }
        }
        return highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
    }

    protected void deleteAndHeadIndex(String index) throws Exception {
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    protected void putDoc(String index, String id, java.util.Map<String, ?> source) throws Exception {
        highLevelClient().index(new IndexRequest(index).id(id).source(source, XContentType.JSON), RequestOptions.DEFAULT);
    }

    protected void putDoc(String index, java.util.Map<String, ?> source) throws Exception {
        highLevelClient().index(new IndexRequest(index).source(source, XContentType.JSON), RequestOptions.DEFAULT);
    }

    protected GetResponse getDocById(String index, String id) throws Exception {
        return highLevelClient().get(new GetRequest(index, id), RequestOptions.DEFAULT);
    }

    protected SqlResponse getSqlResponse(String sqlStr) throws Exception {
        return highLevelClient().havenask().sql(new SqlRequest(sqlStr), RequestOptions.DEFAULT);
    }

    protected void updateDoc(String index, String id, java.util.Map<String, Object> source) throws Exception {
        highLevelClient().update(new UpdateRequest(index, id).doc(source, XContentType.JSON), RequestOptions.DEFAULT);
    }

    protected void deleteDoc(String index, String id) throws Exception {
        highLevelClient().delete(new DeleteRequest(index, id), RequestOptions.DEFAULT);
    }
}
