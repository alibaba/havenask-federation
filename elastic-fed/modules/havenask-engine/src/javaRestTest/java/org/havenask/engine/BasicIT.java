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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
import org.havenask.client.ha.SqlClientInfoRequest;
import org.havenask.client.ha.SqlClientInfoResponse;
import org.havenask.client.ha.SqlRequest;
import org.havenask.client.ha.SqlResponse;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.client.indices.GetIndexResponse;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;

import com.alibaba.fastjson.JSONObject;

public class BasicIT extends AbstractHavenaskRestTestCase {
    public void testCRUD() throws Exception {
        String index = "test";
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
                    ),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );

        GetIndexResponse getIndexResponse = highLevelClient().indices().get(new GetIndexRequest(index), RequestOptions.DEFAULT);
        assertEquals(getIndexResponse.getIndices().length, 1);
        assertEquals(getIndexResponse.getSetting(index, EngineSettings.ENGINE_TYPE_SETTING.getKey()), EngineSettings.ENGINE_HAVENASK);
        assertEquals(getIndexResponse.getSetting(index, "index.number_of_replicas"), "0");
        assertEquals(getIndexResponse.getMappings().get(index), new MappingMetadata("_doc", Map.of("dynamic", "false")));

        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        SqlClientInfoResponse sqlClientInfoResponse = highLevelClient().havenask()
            .sqlClientInfo(new SqlClientInfoRequest(), RequestOptions.DEFAULT);
        assertEquals(sqlClientInfoResponse.getErrorCode(), 0);
        assertEquals(sqlClientInfoResponse.getErrorMessage(), "");

        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> tables = (java.util.Map<String, Object>) ((java.util.Map<String, Object>) (((java.util.Map<
            String,
            Object>) (sqlClientInfoResponse.getResult().get("default"))).get("general"))).get("tables");
        assertTrue(tables.containsKey(index));

        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
    }

    // create index, get index, delete index, HEAD index and set mapping
    public void testIndexMethod() throws Exception {
        String index = "index_index_method";
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

        // get index
        GetIndexResponse getIndexResponse = highLevelClient().indices().get(new GetIndexRequest(index), RequestOptions.DEFAULT);
        assertEquals(getIndexResponse.getIndices().length, 1);
        assertEquals(getIndexResponse.getSetting(index, EngineSettings.ENGINE_TYPE_SETTING.getKey()), EngineSettings.ENGINE_HAVENASK);
        assertEquals(getIndexResponse.getSetting(index, "index.number_of_replicas"), "0");
        assertEquals(
            getIndexResponse.getMappings().get(index),
            new MappingMetadata(
                "_doc",
                Map.of(
                    "dynamic",
                    "false",
                    "properties",
                    Map.of("content", Map.of("type", "keyword"), "seq", Map.of("type", "integer"), "time", Map.of("type", "date"))
                )
            )
        );

        // delete index and HEAD index
        assertEquals(true, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

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
            new IndexRequest(index).opType("create")
                .id("2")
                .source(Map.of("seq", 2, "content", "欢迎使用2", "time", "20230717"), XContentType.JSON)
        );
        bulkRequest.add(
            new IndexRequest(index).id("3").source(Map.of("seq", 3, "content", "欢迎使用3", "time", "20230716"), XContentType.JSON)
        );
        bulkRequest.add(new DeleteRequest(index, "3"));
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));

        // todo: check data using sql search api
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
                                    "keyword",
                                    Map.of("type", "keyword"),
                                    "text",
                                    Map.of("type", "text"),
                                    "integer",
                                    Map.of("type", "integer"),
                                    "double",
                                    Map.of("type", "double"),
                                    "date",
                                    Map.of("type", "date"),
                                    "boolean",
                                    Map.of("type", "boolean"),
                                    "integer_array",
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
            new IndexRequest(index).id("1").source(Map.of("keyword", "keyword_test"), XContentType.JSON),
            RequestOptions.DEFAULT
        );

        highLevelClient().bulk(
            new BulkRequest().add(
                new IndexRequest(index).id("2")
                    .source(
                        Map.of(
                            "keyword",
                            "keyword",
                            "text",
                            "text",
                            "integer",
                            1,
                            "double",
                            1.0,
                            "date",
                            "2020-01-01",
                            "boolean",
                            true,
                            "integer_array",
                            new int[] { 1, 2, 3 }
                        ),
                        XContentType.JSON
                    )
            )
                .add(
                    new IndexRequest(index).id("3")
                        .source(
                            Map.of(
                                "keyword",
                                1,
                                "text",
                                2,
                                "integer",
                                "-32768",
                                "double",
                                "3.14",
                                "date",
                                "20230718",
                                "boolean",
                                "false",
                                "integer_array",
                                new int[] { 2, 3, 4 }
                            ),
                            XContentType.JSON
                        )
                ),
            RequestOptions.DEFAULT
        );

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));

        // todo: check data using sql search api
    }

    // test unsupported data type ()
    public void testUnsupportedDataType() throws Exception {
        String index = "index_unsupported_data_type";

        ArrayList<String> unsupportedDataType = new ArrayList<String>(
            Arrays.asList(
                "binary",
                "constant_keyword",
                "wildcard",
                "half_float",
                "scaled_float",
                "date_nanos",
                "alias",  // Common types
                "flattened",
                "nested",
                "join",    // Object And relational types
                "integer_range",
                "float_range",
                "long_range",
                "double_range",
                "date_range",
                "ip_range",
                "ip",
                "version",
                "murmur3",     // Structured data types
                "histogram",    // Aggregate data types
                "annotated-text",
                "completion",
                "search_as_you_type",
                "token_count",    // Text search types
                "dense_vector",
                "sparse_vector",
                "rank_feature",
                "rank_features",   // Document ranking types
                "point",
                "shape",   // Spatial data types
                "percolator"    // Other types
            )
        );

        for (String curDataType : unsupportedDataType) {
            org.havenask.HavenaskStatusException ex = expectThrows(
                org.havenask.HavenaskStatusException.class,
                () -> highLevelClient().indices()
                    .create(
                        new CreateIndexRequest(index).settings(
                            Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
                        ).mapping(Map.of("properties", Map.of("curDataType", Map.of("type", curDataType)))),
                        RequestOptions.DEFAULT
                    )
            );
            String ex2str = ex.getMessage();
            assertTrue(ex2str.contains("unsupported_operation_exception") || ex2str.contains("mapper_parsing_exception"));
        }
    }

}
