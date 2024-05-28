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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.delete.DeleteRequest;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.search.SearchRequest;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.ha.SqlResponse;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;

import com.alibaba.fastjson.JSONObject;

public class DocIT extends AbstractHavenaskRestTestCase {
    // static logger
    private static final Logger logger = LogManager.getLogger(DocIT.class);
    private static Set<String> docITIndices = new HashSet<>();

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : docITIndices) {
                if (highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                    highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                    logger.info("clean index {}", index);
                }
            }
        } catch (IOException e) {
            logger.error("clean index failed", e);
        }
    }

    // test document api, PUT/POST/DELETE and bulk, test GET /index/_stats
    public void testDocMethod() throws Exception {
        String index = "index_doc_method";
        docITIndices.add(index);

        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        // create index
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put("number_of_shards", shardsNum)
            .put("number_of_replicas", replicasNum)
            .build();

        java.util.Map<String, ?> map = Map.of(
            "properties",
            Map.of("seq", Map.of("type", "integer"), "content", Map.of("type", "keyword"), "time", Map.of("type", "date"))
        );
        assertTrue(createTestIndex(index, settings, map));

        waitIndexGreen(index);

        // PUT doc
        String[] idList = { "1", "2", "3" };
        List<java.util.Map<String, ?>> sourceList = new ArrayList<>();
        sourceList.add(Map.of("seq", 1, "content", "欢迎使用1", "time", "20230718"));
        sourceList.add(Map.of("seq", 2, "content", "欢迎使用2", "time", "20230717"));
        sourceList.add(Map.of("seq", 3, "content", "欢迎使用3", "time", "20230716"));
        for (int i = 0; i < idList.length; i++) {
            putDoc(index, idList[i], sourceList.get(i));
        }

        // GET doc
        int[] expectedSeq = { 1, 2, 3 };
        String[] expectedContent = { "欢迎使用1", "欢迎使用2", "欢迎使用3" };
        String[] expectedTime = { "20230718", "20230717", "20230716" };
        for (int i = 0; i < idList.length; i++) {
            waitResponseExists(index, idList[i]);
            GetResponse getResponse = getDocById(index, idList[i]);
            assertEquals(true, getResponse.isExists());
            assertEquals(expectedSeq[i], getResponse.getSourceAsMap().get("seq"));
            assertEquals(expectedContent[i], getResponse.getSourceAsMap().get("content"));
            assertEquals(expectedTime[i], getResponse.getSourceAsMap().get("time"));
        }

        // POST doc
        putDoc(index, Map.of("seq", 4, "content", "欢迎使用4", "time", "20230715"));

        /// get index data count
        assertBusy(() -> {
            waitSqlResponseExists("select count(*) from " + index, 1);
            SqlResponse sqlResponse = getSqlResponse("select count(*) from " + index);
            assertEquals(1, sqlResponse.getSqlResult().getData().length);
            assertEquals(4, sqlResponse.getSqlResult().getData()[0][0]);
        }, 5, TimeUnit.SECONDS);

        // get index stats
        checkStatsDocCount(index, 4L, replicasNum);

        // UPDATE doc
        updateDoc(index, "1", Map.of("seq", 11, "content", "欢迎使用11", "time", "20230718"));
        updateDoc(index, "2", Map.of("seq", 12, "content", "欢迎使用12", "time", "20230717"));
        String[] updateIdList = { "1", "2" };
        int[] expectedUpdateSeq = { 11, 12 };
        String[] expectedUpdateContent = { "欢迎使用11", "欢迎使用12" };
        String[] expectedUpdateTime = { "20230718", "20230717" };

        // check update
        for (int i = 0; i < updateIdList.length; i++) {
            GetResponse getResponse = getDocById(index, updateIdList[i]);
            assertEquals(expectedUpdateSeq[i], getResponse.getSourceAsMap().get("seq"));
            assertEquals(expectedUpdateContent[i], getResponse.getSourceAsMap().get("content"));
            assertEquals(expectedUpdateTime[i], getResponse.getSourceAsMap().get("time"));
        }

        // get index data count
        checkStatsDocCount(index, 4L, replicasNum);

        // delete and check
        String[] deleteIdList = { "1", "2", "3" };
        for (int i = 0; i < deleteIdList.length; i++) {
            deleteDoc(index, deleteIdList[i]);
            GetResponse getResponse = getDocById(index, deleteIdList[i]);
            assertFalse(getResponse.isExists());
        }
        checkStatsDocCount(index, 1L, replicasNum);

        // bulk doc
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < idList.length; i++) {
            bulkRequest.add(new IndexRequest(index).id(idList[i]).source(sourceList.get(i), XContentType.JSON));
        }
        bulkRequest.add(new DeleteRequest(index, "3"));
        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // check data using sql search api
        String sqlStr = "select * from " + index + " where seq=1 AND content='欢迎使用1'";
        waitSqlResponseExists(sqlStr, 1);
        SqlResponse bulkSqlResponse = getSqlResponse(sqlStr);
        java.util.Map<String, Integer> dataIndexMap = new HashMap<>();
        for (int i = 0; i < bulkSqlResponse.getSqlResult().getColumnName().length; i++) {
            dataIndexMap.put(bulkSqlResponse.getSqlResult().getColumnName()[i], i);
        }
        assertEquals(1, bulkSqlResponse.getRowCount());
        assertEquals("欢迎使用1", bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("content")]);
        assertEquals(20230718, bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("time")]);
        assertEquals(1, bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("seq")]);

        deleteAndHeadIndex(index);
    }

    // test common data type(int, double, boolean, date, text, keyword, array)
    public void testMultiDataType() throws Exception {
        String index = "index_multi_data_type";
        docITIndices.add(index);

        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        // create index with multi data type
        Settings settings = Settings.builder()
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put("number_of_shards", shardsNum)
            .put("number_of_replicas", replicasNum)
            .build();

        java.util.Map<String, ?> map = Map.of(
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
        );
        assertTrue(createTestIndex(index, settings, map));
        waitIndexGreen(index);

        putDoc(index, "1", Map.of("my_keyword", "keyword_test"));

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
        waitSqlResponseExists(sqlStr, 1);
        SqlResponse bulkSqlResponse = getSqlResponse(sqlStr);
        java.util.Map<String, Integer> dataIndexMap = new HashMap<>();
        for (int i = 0; i < bulkSqlResponse.getSqlResult().getColumnName().length; i++) {
            dataIndexMap.put(bulkSqlResponse.getSqlResult().getColumnName()[i], i);
        }
        assertEquals(1, bulkSqlResponse.getRowCount());
        assertEquals(1, bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("my_integer")]);
        assertEquals(1577836800000L, bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("my_date")]);
        assertEquals("text", bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("my_text")]);
        assertEquals("keyword", bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("my_keyword")]);
        assertEquals(1.5, bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("my_double")]);
        assertEquals("T", bulkSqlResponse.getSqlResult().getData()[0][dataIndexMap.get("my_boolean")]);

        deleteAndHeadIndex(index);
    }

    public void testIllegalVectorParams() throws Exception {
        String index = "illegal_vector_test";
        docITIndices.add(index);

        String fieldName = "vector";
        int vectorDims = 2;
        String similarity = "dot_product";

        float[] vectorParams = { 1.0f, 2.0f };

        ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
            .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        int numberOfDataNodes = clusterHealthResponse.getNumberOfDataNodes();

        int shardsNum = randomIntBetween(1, 6);
        int replicasNum = randomIntBetween(0, numberOfDataNodes - 1);
        // create index
        assertTrue(
            createTestIndex(
                index,
                Settings.builder()
                    .put("index.number_of_shards", shardsNum)
                    .put("index.number_of_replicas", replicasNum)
                    .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                    .build(),
                createMapping(fieldName, vectorDims, similarity)
            )
        );

        waitIndexGreen(index);

        org.havenask.HavenaskStatusException ex1 = expectThrows(
            org.havenask.HavenaskStatusException.class,
            () -> highLevelClient().index(
                new IndexRequest(index).id("1").source(Map.of(fieldName, vectorParams), XContentType.JSON),
                RequestOptions.DEFAULT
            )
        );
        assertTrue(ex1.getCause().getMessage().contains("The [dot_product] similarity can only be used with unit-length vectors."));

        // get data with _search
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder(fieldName, new float[] { 1.5f, 2.5f }, 10, 100, null)));
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        org.havenask.HavenaskStatusException ex2 = expectThrows(
            org.havenask.HavenaskStatusException.class,
            () -> highLevelClient().search(searchRequest, RequestOptions.DEFAULT)
        );
        assertTrue(ex2.getDetailedMessage().contains("The [dot_product] similarity can only be used with unit-length vectors."));

        deleteAndHeadIndex(index);
    }

    private static XContentBuilder createMapping(String fieldName, int vectorDims, String similarity) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", DenseVectorFieldMapper.CONTENT_TYPE)
            .field("dims", vectorDims)
            .field("similarity", similarity)
            .endObject()
            .endObject()
            .endObject();
        return mappingBuilder;
    }

    private static void checkStatsDocCount(String index, long expectedDocCount, int replicasNum) throws Exception {
        assertBusy(() -> {
            Response indexStatsResponse = highLevelClient().getLowLevelClient().performRequest(new Request("GET", "/" + index + "/_stats"));
            String indexStats = EntityUtils.toString(indexStatsResponse.getEntity());
            JSONObject indexStatsJson = JSONObject.parseObject(indexStats);
            long primariesDocCount = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("primaries")
                .getJSONObject("docs")
                .getLong("count");
            assertEquals(expectedDocCount, primariesDocCount);
            long totalDocCount = indexStatsJson.getJSONObject("indices")
                .getJSONObject(index)
                .getJSONObject("total")
                .getJSONObject("docs")
                .getLong("count");
            assertEquals(expectedDocCount * (1 + replicasNum), totalDocCount);
        }, 10, TimeUnit.SECONDS);
    }

    protected void waitSqlResponseExists(String sqlStr, int expectedRowCount) throws Exception {
        assertBusy(() -> {
            SqlResponse sqlResponse = getSqlResponse(sqlStr);
            assertEquals(expectedRowCount, sqlResponse.getRowCount());
        }, 10, TimeUnit.SECONDS);
    }
}
