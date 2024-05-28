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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.client.RequestOptions;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
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

public class MappingIT extends AbstractHavenaskRestTestCase {
    // static logger
    private static final Logger logger = LogManager.getLogger(MappingIT.class);
    private static Set<String> mappingITIndices = new HashSet<>();

    @AfterClass
    public static void cleanIndices() {
        try {
            for (String index : mappingITIndices) {
                if (highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                    highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                    logger.info("clean index {}", index);
                }
            }
        } catch (IOException e) {
            logger.error("clean index failed", e);
        }
    }

    // test supported data type
    public void testSupportedDataType() throws Exception {
        String index = "index_supported_data_type";
        mappingITIndices.add(index);

        int shardsNum = randomIntBetween(1, 6);
        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put("number_of_shards", shardsNum)
                            .put("number_of_replicas", 0)
                            .build()
                    )
                        .mapping(
                            Map.of(
                                "properties",
                                Map.of(
                                    "dataTypeBoolean",
                                    Map.of("type", "boolean"),
                                    "dataTypeKeyword",
                                    Map.of("type", "keyword"),
                                    "dataTypeLong",
                                    Map.of("type", "long"),
                                    "dataTypeInteger",
                                    Map.of("type", "integer"),
                                    "dataTypeShort",
                                    Map.of("type", "short"),
                                    "dataTypeByte",
                                    Map.of("type", "byte"),
                                    "dataTypeDouble",
                                    Map.of("type", "double"),
                                    "dataTypeFloat",
                                    Map.of("type", "float"),
                                    "dataTypeDate",
                                    Map.of("type", "date"),
                                    "dataTypeText",
                                    Map.of("type", "text")
                                )/*,
                                 "properties2",  //暂不支持的dataType
                                 Map.of(
                                        "dataTypeGeoPoint",
                                        Map.of("type", "geo_point"),
                                        "dataTypeGeoShape",
                                        Map.of("type", "geo_shape"),
                                        "dataTypeUnsignedLong",
                                        Map.of("type", "unsigned_long")
                                 )*/
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

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    // test unsupported data type
    public void testUnsupportedDataType() throws Exception {
        String index = "index_unsupported_data_type";
        mappingITIndices.add(index);

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
                            Settings.builder()
                                .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                                .put("number_of_replicas", 0)
                                .build()
                        ).mapping(Map.of("properties", Map.of("curDataType", Map.of("type", curDataType)))),
                        RequestOptions.DEFAULT
                    )
            );
            String exMessage = ex.getMessage();
            assertTrue(exMessage.contains("unsupported_operation_exception") || exMessage.contains("mapper_parsing_exception"));
        }
    }

    // 对havenask v2版本的向量索引配置适配后的测试
    public void testVectorData() throws Exception {
        String index = "index_vector_data";
        mappingITIndices.add(index);

        final int TYPE_POS = 0;
        String fieldName = "image";
        int vectorDims = 2;
        String similarity = "L2_NORM";

        final String[] types = { "hnsw", "qc", "linear" };
        final String[] typesStr = { "HNSW", "QC", "LINEAR" };
        final String[] indexOptionNames = new String[] {
            "type",
            "embedding_delimiter",
            "major_order",
            "ignore_invalid_doc",
            "enable_recall_report",
            "is_embedding_saved",
            "min_scan_doc_cnt",
            "linear_build_threshold" };
        final String[] indexOptionValues = new String[] { "", ",", "row", "true", "true", "true", "20000", "500" };

        final String[][] searchIndexParamsNames = new String[][] {
            new String[] { "proxima.hnsw.searcher.ef" },
            new String[] { "proxima.qc.searcher.scan_ratio", "proxima.qc.searcher.brute_force_threshold" },
            null };
        final String[][] searchIndexParamsValues = new String[][] { new String[] { "500" }, new String[] { "0.01", "1000" }, null };

        final String[][] buildIndexParamsNames = new String[][] {
            new String[] {
                "proxima.hnsw.builder.max_neighbor_count",
                "proxima.hnsw.builder.efconstruction",
                "proxima.hnsw.builder.thread_count" },
            new String[] {
                "proxima.qc.builder.train_sample_count",
                "proxima.qc.builder.thread_count",
                "proxima.qc.builder.centroid_count",
                "proxima.qc.builder.cluster_auto_tuning",
                "proxima.qc.builder.quantize_by_centroid",
                "proxima.qc.builder.store_original_features",
                "proxima.qc.builder.train_sample_ratio" },
            new String[] { "proxima.linear.builder.column_major_order" } };
        final String[][] buildIndexParamsValues = new String[][] {
            new String[] { "100", "500", "0" },
            new String[] { "0", "0", "1000", "false", "false", "false", "1.0" },
            new String[] { "false" } };

        int dataNum = 8;
        String[] ids = { "1", "2", "3", "4", "5", "6", "7", "8" };
        float[][] images = new float[][] {
            { 1.1f, 1.1f },
            { 2.2f, 2.2f },
            { 3.3f, 3.3f },
            { 4.4f, 4.4f },
            { 5.5f, 5.5f },
            { 6.6f, 6.6f },
            { 7.7f, 7.7f },
            { 8.8f, 8.8f } };

        String[] resSourceAsString = new String[] {
            "{\"image\":[2.2,2.2]}",
            "{\"image\":[1.1,1.1]}",
            "{\"image\":[3.3,3.3]}",
            "{\"image\":[4.4,4.4]}",
            "{\"image\":[5.5,5.5]}",
            "{\"image\":[6.6,6.6]}",
            "{\"image\":[7.7,7.7]}",
            "{\"image\":[8.8,8.8]}" };

        float delta = 0.0001F;
        float expectedMaxScore = 0.6329114F;
        float[] expectedScores = new float[] {
            0.6329114F,
            0.32051283F,
            0.20491804F,
            0.07680491F,
            0.03846154F,
            0.02282063F,
            0.015042119F,
            0.010640562F };

        for (int type = 0; type < types.length; type++) {
            indexOptionValues[TYPE_POS] = types[type];
            // int shardsNum = randomIntBetween(1, 6);
            // int replicasNum = randomIntBetween(1, 2);
            // create index
            assertTrue(
                highLevelClient().indices()
                    .create(
                        new CreateIndexRequest(index).settings(
                            Settings.builder()
                                .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                                // TODO 目前knn查询如果有shard中没有doc会出错，暂时将shard数设置为2
                                .put("index.number_of_shards", 2)
                                // .put("index.number_of_shards", shardsNum)
                                .put("index.number_of_replicas", 0)
                                // .put("index.number_of_replicas", replicasNum)
                                .build()
                        )
                            .mapping(
                                createVectorMapping(
                                    vectorDims,
                                    fieldName,
                                    similarity,
                                    indexOptionNames,
                                    indexOptionValues,
                                    searchIndexParamsNames[type],
                                    searchIndexParamsValues[type],
                                    buildIndexParamsNames[type],
                                    buildIndexParamsValues[type]
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

            // get mapping
            indexOptionValues[TYPE_POS] = typesStr[type];
            java.util.Map<String, Object> expectedMapping = createExpectedMapping(
                vectorDims,
                fieldName,
                similarity,
                indexOptionNames,
                indexOptionValues,
                searchIndexParamsNames[type],
                searchIndexParamsValues[type],
                buildIndexParamsNames[type],
                buildIndexParamsValues[type]
            );
            java.util.Map<String, Object> actualMapping = highLevelClient().indices()
                .get(new GetIndexRequest(index), RequestOptions.DEFAULT)
                .getMappings()
                .get(index)
                .getSourceAsMap();
            assertEquals(expectedMapping.toString(), actualMapping.toString());

            // put and get some doc
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < dataNum; i++) {
                bulkRequest.add(new IndexRequest(index).id(ids[i]).source(Map.of(fieldName, images[i]), XContentType.JSON));
            }

            highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

            // get data with _search
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder(fieldName, new float[] { 1.5f, 2.5f }, 10, 100, null)));
            searchRequest.source(searchSourceBuilder);

            // 执行查询请求并获取相应结果
            assertBusy(() -> {
                SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
                assertEquals(dataNum, searchResponse.getHits().getHits().length);
            }, 10, TimeUnit.SECONDS);
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
            assertEquals(expectedMaxScore, searchResponse.getHits().getMaxScore(), delta);
            for (int i = 0; i < dataNum; i++) {
                assertEquals(resSourceAsString[i], searchResponse.getHits().getHits()[i].getSourceAsString());
                assertEquals(expectedScores[i], searchResponse.getHits().getHits()[i].getScore(), delta);
            }

            // delete index and HEAD index
            assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
            assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
        }
    }

    /**
     *     "dimension": "2",
     *     "embedding_delimiter": ",",
     *     "linear_build_threshold": "10000",
     *     "enable_recall_report": "true",
     *     "min_scan_doc_cnt":"20000",
     *     "ignore_invalid_doc":"true",
     *     "build_index_params": {},
     *     "search_index_params": {proxima.qc.searcher.scan_ratio:0.10}
     */

    @SuppressWarnings("unchecked")
    public void testVectorDataWithPartialParams() throws Exception {
        String index = "index_vector_data_with_partial_params";
        mappingITIndices.add(index);

        final int TYPE_POS = 0;
        String fieldName = "image";
        int vectorDims = 2;
        String similarity = "L2_NORM";

        final String[] indexOptionNames = new String[] {
            "type",
            "embedding_delimiter",
            "linear_build_threshold",
            "enable_recall_report",
            "min_scan_doc_cnt",
            "ignore_invalid_doc" };

        final String[] indexOptionValues = new String[] { "qc", ",", "10000", "true", "20000", "true" };

        final String[] searchIndexParamsNames = new String[] { "proxima.qc.searcher.scan_ratio" };
        final String[] searchIndexParamsValues = new String[] { "0.01" };

        int dataNum = 8;
        String[] ids = { "1", "2", "3", "4", "5", "6", "7", "8" };
        float[][] images = new float[][] {
            { 1.1f, 1.1f },
            { 2.2f, 2.2f },
            { 3.3f, 3.3f },
            { 4.4f, 4.4f },
            { 5.5f, 5.5f },
            { 6.6f, 6.6f },
            { 7.7f, 7.7f },
            { 8.8f, 8.8f } };

        String[] resSourceAsString = new String[] {
            "{\"image\":[2.2,2.2]}",
            "{\"image\":[1.1,1.1]}",
            "{\"image\":[3.3,3.3]}",
            "{\"image\":[4.4,4.4]}",
            "{\"image\":[5.5,5.5]}",
            "{\"image\":[6.6,6.6]}",
            "{\"image\":[7.7,7.7]}",
            "{\"image\":[8.8,8.8]}" };

        float delta = 0.0001F;
        float expectedMaxScore = 0.6329114F;
        float[] expectedScores = new float[] {
            0.6329114F,
            0.32051283F,
            0.20491804F,
            0.07680491F,
            0.03846154F,
            0.02282063F,
            0.015042119F,
            0.010640562F };

        // int shardsNum = randomIntBetween(1, 6);
        // int replicasNum = randomIntBetween(0, 2);
        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            // TODO 目前knn查询如果有shard中没有doc会出错，暂时将shard数设置为2
                            .put("index.number_of_shards", 2)
                            // .put("index.number_of_shards", shardsNum)
                            .put("index.number_of_replicas", 0)
                            // .put("index.number_of_replicas", replicasNum)
                            .build()
                    )
                        .mapping(
                            createVectorMapping(
                                vectorDims,
                                fieldName,
                                similarity,
                                indexOptionNames,
                                indexOptionValues,
                                searchIndexParamsNames,
                                searchIndexParamsValues,
                                null,
                                null
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

        // get mapping
        indexOptionValues[TYPE_POS] = "QC";
        java.util.Map<String, Object> expectedMapping = createExpectedMapping(
            vectorDims,
            fieldName,
            similarity,
            indexOptionNames,
            indexOptionValues,
            searchIndexParamsNames,
            searchIndexParamsValues,
            null,
            null
        );
        java.util.Map<String, Object> actualMapping = highLevelClient().indices()
            .get(new GetIndexRequest(index), RequestOptions.DEFAULT)
            .getMappings()
            .get(index)
            .getSourceAsMap();
        assertEquals(expectedMapping.toString(), actualMapping.toString());
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < dataNum; i++) {
            bulkRequest.add(new IndexRequest(index).id(ids[i]).source(Map.of(fieldName, images[i]), XContentType.JSON));
        }

        highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);

        // get data with _search
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder(fieldName, new float[] { 1.5f, 2.5f }, 10, 100, null)));
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getHits().length);
        assertEquals(expectedMaxScore, searchResponse.getHits().getMaxScore(), delta);
        for (int i = 0; i < dataNum; i++) {
            assertEquals(resSourceAsString[i], searchResponse.getHits().getHits()[i].getSourceAsString());
            assertEquals(expectedScores[i], searchResponse.getHits().getHits()[i].getScore(), delta);
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    public void testIllegalVectorParamsCheck() throws Exception {
        String index = "index_test_illegal_vector_params_check";
        mappingITIndices.add(index);

        String fieldName = "image";
        int vectorDims = 2;
        String similarity = "DOT_PRODUCT";

        int dataNum = 3;
        String[] ids = { "1", "2", "3" };
        float[][] images = new float[][] { { 0.6f, 0.8f }, { 0.8f, 0.6f }, { 1.0f, 0f } };

        float[] wrongVector = new float[] { 1f, 1f };

        String[] resSourceAsString = new String[] { "{\"image\":[0.6,0.8]}", "{\"image\":[0.8,0.6]}", "{\"image\":[1.0,0.0]}" };

        float delta = 0.0001F;
        float expectedMaxScore = 1F;
        float[] expectedScores = new float[] { 1F, 0.98F, 0.8F };

        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .build()
                    ).mapping(createVectorMapping(vectorDims, fieldName, similarity, null, null, null, null, null, null)),
                    RequestOptions.DEFAULT
                )
                .isAcknowledged()
        );
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = highLevelClient().cluster()
                .health(new ClusterHealthRequest(index), RequestOptions.DEFAULT);
            assertEquals(clusterHealthResponse.getStatus(), ClusterHealthStatus.GREEN);
        }, 2, TimeUnit.MINUTES);

        // get mapping
        java.util.Map<String, Object> expectedMapping = createExpectedMapping(
            vectorDims,
            fieldName,
            similarity,
            null,
            null,
            null,
            null,
            null,
            null
        );
        java.util.Map<String, Object> actualMapping = highLevelClient().indices()
            .get(new GetIndexRequest(index), RequestOptions.DEFAULT)
            .getMappings()
            .get(index)
            .getSourceAsMap();
        assertEquals(expectedMapping.toString(), actualMapping.toString());

        // put doc
        for (int i = 0; i < dataNum; i++) {
            highLevelClient().index(
                new IndexRequest(index).id(ids[i]).source(Map.of(fieldName, images[i]), XContentType.JSON),
                RequestOptions.DEFAULT
            );
        }
        try {
            highLevelClient().index(
                new IndexRequest(index).id("4").source(Map.of(fieldName, wrongVector), XContentType.JSON),
                RequestOptions.DEFAULT
            );
            fail("Should throw exception");
        } catch (Exception e) {
            assertTrue(e.getCause().toString().contains("The [dot_product] similarity can only be used with unit-length vectors."));
        }

        // get data with _search
        try {
            SearchRequest wrongSearchRequest = new SearchRequest(index);
            SearchSourceBuilder wrongSearchSourceBuilder = new SearchSourceBuilder();
            wrongSearchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder(fieldName, new float[] { 1f, 2f }, 10, 100, null)));
            wrongSearchRequest.source(wrongSearchSourceBuilder);
        } catch (Exception e) {
            assertTrue(e.getCause().toString().contains("The [dot_product] similarity can only be used with unit-length vectors."));
        }

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder(fieldName, new float[] { 0.6f, 0.8f }, 10, 100, null)));
        searchRequest.source(searchSourceBuilder);

        // 执行查询请求并获取相应结果
        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(dataNum, searchResponse.getHits().getHits().length);
        }, 10, TimeUnit.SECONDS);
        SearchResponse searchResponse = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(dataNum, searchResponse.getHits().getHits().length);
        assertEquals(expectedMaxScore, searchResponse.getHits().getMaxScore(), delta);
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            assertEquals(resSourceAsString[i], searchResponse.getHits().getHits()[i].getSourceAsString());
            assertEquals(expectedScores[i], searchResponse.getHits().getHits()[i].getScore(), delta);
        }

        // delete index and HEAD index
        assertTrue(highLevelClient().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged());
        assertEquals(false, highLevelClient().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    private static XContentBuilder createVectorMapping(
        int vectorDims,
        String fieldName,
        String similarityIn,
        String[] indexOptionNames,
        String[] IndexOptionValues,
        String[] searchIndexParamsNames,
        String[] searchIndexParamsValues,
        String[] buildIndexParamsNames,
        String[] buildIndexParamsValues
    ) throws IOException {
        String similarity = similarityIn.toLowerCase(Locale.getDefault());
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject(fieldName);
                {
                    mappingBuilder.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    mappingBuilder.field("dims", vectorDims);
                    mappingBuilder.field("similarity", similarity);
                    if (indexOptionNames != null && indexOptionNames.length > 0) {
                        mappingBuilder.startObject("index_options");
                        {
                            for (int i = 0; i < indexOptionNames.length; i++) {
                                mappingBuilder.field(indexOptionNames[i], IndexOptionValues[i]);
                            }
                            if (searchIndexParamsValues != null) {
                                mappingBuilder.startObject("search_index_params");
                                {
                                    for (int i = 0; i < searchIndexParamsNames.length; i++) {
                                        mappingBuilder.field(searchIndexParamsNames[i], searchIndexParamsValues[i]);
                                    }
                                }
                                mappingBuilder.endObject();
                            }
                            if (buildIndexParamsValues != null) {
                                mappingBuilder.startObject("build_index_params");
                                {
                                    for (int i = 0; i < buildIndexParamsNames.length; i++) {
                                        mappingBuilder.field(buildIndexParamsNames[i], buildIndexParamsValues[i]);
                                    }
                                }
                                mappingBuilder.endObject();
                            }
                        }
                        mappingBuilder.endObject();
                    }
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        return mappingBuilder;
    }

    private static java.util.Map<String, Object> createExpectedMapping(
        int vectorDims,
        String fieldName,
        String similarity,
        String[] indexOptionNames,
        String[] IndexOptionValues,
        String[] searchIndexParamsNames,
        String[] searchIndexParamsValues,
        String[] buildIndexParamsNames,
        String[] buildIndexParamsValues
    ) {
        java.util.Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("dynamic", "false");
        java.util.Map<String, Object> properties = new HashMap<>();
        expectedMapping.put("properties", properties);
        java.util.Map<String, Object> fieldMap = new HashMap<>();
        properties.put(fieldName, fieldMap);
        fieldMap.put("dims", vectorDims);
        fieldMap.put("similarity", similarity);
        fieldMap.put("type", DenseVectorFieldMapper.CONTENT_TYPE);
        if (indexOptionNames != null) {
            java.util.Map<String, Object> indexOptions = new HashMap<>();
            fieldMap.put("index_options", indexOptions);
            for (int i = 0; i < indexOptionNames.length; i++) {
                indexOptions.put(indexOptionNames[i], IndexOptionValues[i]);
            }
            java.util.Map<String, Object> searchIndexParams = new HashMap<>();
            if (searchIndexParamsNames != null) {
                indexOptions.put("search_index_params", searchIndexParams);
                for (int i = 0; i < searchIndexParamsNames.length; i++) {
                    searchIndexParams.put(searchIndexParamsNames[i], searchIndexParamsValues[i]);
                }
            }
            java.util.Map<String, Object> buildIndexParams = new HashMap<>();
            indexOptions.put("build_index_params", buildIndexParams);
            if (buildIndexParamsNames != null) {
                for (int i = 0; i < buildIndexParamsNames.length; i++) {
                    buildIndexParams.put(buildIndexParamsNames[i], buildIndexParamsValues[i]);
                }
            }
        }
        return expectedMapping;
    }
}
