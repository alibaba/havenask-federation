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

import com.alibaba.fastjson.JSONObject;
import org.apache.http.util.EntityUtils;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.delete.DeleteRequest;
import org.havenask.action.get.GetRequest;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.update.UpdateRequest;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.ha.SqlRequest;
import org.havenask.client.ha.SqlResponse;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;

import java.util.concurrent.TimeUnit;

public class DocIT extends AbstractHavenaskRestTestCase {
    // test document api, PUT/POST/DELETE and bulk
    public void testDocMethod() throws Exception {
        String index = "index_doc_method";
        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
                    )
                        .mapping(
                            Map.of(
                                "properties",
                                Map.of(
                                    "seq",
                                    Map.of("type", "integer"),
                                    "content",
                                    Map.of("type", "keyword"),
                                    "time",
                                    Map.of("type", "date")
                                )
                            )
                        ),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        // PUT doc
        highLevelClient().index(
            new IndexRequest(index).id("1").source(Map.of("seq", 1, "content", "欢迎使用1", "time", "20230718"), XContentType.JSON),
            RequestOptions.DEFAULT
        );
        highLevelClient().index(
            new IndexRequest(index).id("2").source(Map.of("seq", 2, "content", "欢迎使用2", "time", "20230717"), XContentType.JSON),
            RequestOptions.DEFAULT
        );
        highLevelClient().index(
            new IndexRequest(index).id("3").source(Map.of("seq", 3, "content", "欢迎使用3", "time", "20230716"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        // GET doc
        GetResponse getResponse = highLevelClient().get(new GetRequest(index, "1"), RequestOptions.DEFAULT);
        assertEquals(getResponse.isExists(), true);
        assertEquals(getResponse.getSourceAsMap().get("seq"), 1);
        assertEquals(getResponse.getSourceAsMap().get("content"), "欢迎使用1");
        assertEquals(getResponse.getSourceAsMap().get("time"), "20230718");

        GetResponse getResponse2 = highLevelClient().get(new GetRequest(index, "2"), RequestOptions.DEFAULT);
        assertEquals(getResponse2.isExists(), true);
        assertEquals(getResponse2.getSourceAsMap().get("seq"), 2);
        assertEquals(getResponse2.getSourceAsMap().get("content"), "欢迎使用2");
        assertEquals(getResponse2.getSourceAsMap().get("time"), "20230717");

        GetResponse getResponse3 = highLevelClient().get(new GetRequest(index, "3"), RequestOptions.DEFAULT);
        assertEquals(getResponse3.isExists(), true);
        assertEquals(getResponse3.getSourceAsMap().get("seq"), 3);
        assertEquals(getResponse3.getSourceAsMap().get("content"), "欢迎使用3");
        assertEquals(getResponse3.getSourceAsMap().get("time"), "20230716");

        // POST doc
        highLevelClient().index(
            new IndexRequest(index).source(Map.of("seq", 4, "content", "欢迎使用4", "time", "20230715"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        /// get index data count
        SqlResponse sqlResponse = highLevelClient().havenask().sql(new SqlRequest("select count(*) from " + index), RequestOptions.DEFAULT);
        assertEquals(sqlResponse.getRowCount(), 1);
        assertEquals(sqlResponse.getSqlResult().getData().length, 1);
        assertEquals(sqlResponse.getSqlResult().getColumnName().length, 1);
        assertEquals(sqlResponse.getSqlResult().getColumnType().length, 1);
        assertEquals(sqlResponse.getSqlResult().getData()[0][0], 4);
        assertEquals(sqlResponse.getSqlResult().getColumnName()[0], "COUNT(*)");
        assertEquals(sqlResponse.getSqlResult().getColumnType()[0], "int64");

        // get index stats
        {
            Response indexStatsResponse = highLevelClient().getLowLevelClient().performRequest(new Request("GET", "/" + index + "/_stats"));
            String indexStats = EntityUtils.toString(indexStatsResponse.getEntity());
            JSONObject indexStatsJson = JSONObject.parseObject(indexStats);
            long docCount = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("total")
                .getJSONObject("docs")
                .getLong("count");
            assertEquals(docCount, 4L);
            long storeSize = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("total")
                .getJSONObject("store")
                .getLong("size_in_bytes");
            assertTrue(storeSize >= 10240L);
        }

        // UPDATE doc
        highLevelClient().update(
            new UpdateRequest(index, "1").doc(Map.of("seq", 11, "content", "欢迎使用11", "time", "20230718"), XContentType.JSON),
            RequestOptions.DEFAULT
        );
        highLevelClient().update(
            new UpdateRequest(index, "2").doc(Map.of("seq", 12, "content", "欢迎使用12", "time", "20230717"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        // GET doc check update
        GetResponse getResponse11 = highLevelClient().get(new GetRequest(index, "1"), RequestOptions.DEFAULT);
        assertEquals(getResponse11.isExists(), true);
        assertEquals(getResponse11.getSourceAsMap().get("seq"), 11);
        assertEquals(getResponse11.getSourceAsMap().get("content"), "欢迎使用11");
        assertEquals(getResponse11.getSourceAsMap().get("time"), "20230718");

        GetResponse getResponse12 = highLevelClient().get(new GetRequest(index, "2"), RequestOptions.DEFAULT);
        assertEquals(getResponse12.isExists(), true);
        assertEquals(getResponse12.getSourceAsMap().get("seq"), 12);
        assertEquals(getResponse12.getSourceAsMap().get("content"), "欢迎使用12");
        assertEquals(getResponse12.getSourceAsMap().get("time"), "20230717");

        // get index data count
        {
            Response indexStatsResponse = highLevelClient().getLowLevelClient().performRequest(new Request("GET", "/" + index + "/_stats"));
            String indexStats = EntityUtils.toString(indexStatsResponse.getEntity());
            JSONObject indexStatsJson = JSONObject.parseObject(indexStats);
            long docCount = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("total")
                .getJSONObject("docs")
                .getLong("count");
            assertEquals(docCount, 4L);
            long storeSize = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("total")
                .getJSONObject("store")
                .getLong("size_in_bytes");
            assertTrue(storeSize >= 10240L);
        }

        // DELETE doc
        highLevelClient().delete(new DeleteRequest(index, "1"), RequestOptions.DEFAULT);
        highLevelClient().delete(new DeleteRequest(index, "2"), RequestOptions.DEFAULT);
        highLevelClient().delete(new DeleteRequest(index, "3"), RequestOptions.DEFAULT);

        // GET doc not exists
        GetResponse getResponse4 = highLevelClient().get(new GetRequest(index, "1"), RequestOptions.DEFAULT);
        assertEquals(getResponse4.isExists(), false);
        GetResponse getResponse5 = highLevelClient().get(new GetRequest(index, "2"), RequestOptions.DEFAULT);
        assertEquals(getResponse5.isExists(), false);
        GetResponse getResponse6 = highLevelClient().get(new GetRequest(index, "3"), RequestOptions.DEFAULT);
        assertEquals(getResponse6.isExists(), false);

        // get
        {
            Response indexStatsResponse = highLevelClient().getLowLevelClient().performRequest(new Request("GET", "/" + index + "/_stats"));
            String indexStats = EntityUtils.toString(indexStatsResponse.getEntity());
            JSONObject indexStatsJson = JSONObject.parseObject(indexStats);
            long docCount = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("total")
                .getJSONObject("docs")
                .getLong("count");
            assertEquals(docCount, 1L);
            long storeSize = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("total")
                .getJSONObject("store")
                .getLong("size_in_bytes");
            assertTrue(storeSize >= 10240L);
        }

        // bulk doc
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(index).id("1").source(Map.of("seq", 1, "content", "欢迎使用1", "time", "20230718"), XContentType.JSON)
        );
        bulkRequest.add(
            new IndexRequest(index).id("2").source(Map.of("seq", 2, "content", "欢迎使用2", "time", "20230717"), XContentType.JSON)
        );
        bulkRequest.add(
            new IndexRequest(index).id("3").source(Map.of("seq", 3, "content", "欢迎使用3", "time", "20230716"), XContentType.JSON)
        );
        bulkRequest.add(new DeleteRequest(index, "3"));
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // check data using sql search api
        String sqlStr = "select * from " + index + " where seq=1 AND content='欢迎使用1'";
        SqlResponse bulkSqlResponse = highLevelClient().havenask().sql(new SqlRequest(sqlStr), RequestOptions.DEFAULT);
        assertEquals(bulkSqlResponse.getRowCount(), 1);
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][1], "欢迎使用1");
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][4], 20230718);
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][6], 1);

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    // test common data type(int, double, boolean, date, text, keyword, array)
    public void testMultiDataType() throws Exception {
        String index = "index_multi_data_type";
        // create index with multi data type
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
                    )
                        .mapping(
                            Map.of(
                                "properties",
                                Map.of(
                                    "my_keyword",
                                    Map.of("type", "keyword"),
                                    "my_text",
                                    Map.of("type", "text"),
                                    "my_integer",
                                    Map.of("type", "integer"),
                                    "my_double",
                                    Map.of("type", "double"),
                                    "my_date",
                                    Map.of("type", "date"),
                                    "my_boolean",
                                    Map.of("type", "boolean"),
                                    "my_integer_array",
                                    Map.of("type", "integer")
                                )
                            )
                        ),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        highLevelClient().index(
            new IndexRequest(index).id("1").source(Map.of("my_keyword", "keyword_test"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        highLevelClient().bulk(
            new BulkRequest().add(
                new IndexRequest(index).id("2")
                    .source(
                        Map.of(
                            "my_keyword",
                            "keyword",
                            "my_text",
                            "text",
                            "my_integer",
                            1,
                            "my_double",
                            1.5,
                            "my_date",
                            "2020-01-01",
                            "my_boolean",
                            true,
                            "my_integer_array",
                            new int[] { 1, 2, 3 }
                        ),
                        XContentType.JSON
                    )
            )
                .add(
                    new IndexRequest(index).id("3")
                        .source(
                            Map.of(
                                "my_keyword",
                                1,
                                "my_text",
                                2,
                                "my_integer",
                                "-32768",
                                "my_double",
                                "3.14",
                                "my_date",
                                "20230718",
                                "my_boolean",
                                "false",
                                "my_integer_array",
                                new int[] { 2, 3, 4 }
                            ),
                            XContentType.JSON
                        )
                ),
            RequestOptions.DEFAULT
        );

        // check data using sql search api
        String sqlStr = "select * from "
            + index
            + " where my_keyword='keyword' AND my_text='text' AND my_integer=1 AND my_double=1.5 AND my_boolean='T' ";
        SqlResponse bulkSqlResponse = highLevelClient().havenask().sql(new SqlRequest(sqlStr), RequestOptions.DEFAULT);
        assertEquals(bulkSqlResponse.getRowCount(), 1);
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][1], 1);
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][2], 1577836800000L);
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][4], "text");
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][5], "keyword");
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][7], 1.5);
        assertEquals(bulkSqlResponse.getSqlResult().getData()[0][9], "T");

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }
}
