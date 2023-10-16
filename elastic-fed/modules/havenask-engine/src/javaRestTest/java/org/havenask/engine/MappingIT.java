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
import java.util.concurrent.TimeUnit;

import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.client.RequestOptions;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.engine.index.engine.EngineSettings;

public class MappingIT extends AbstractHavenaskRestTestCase {
    public static final String[] hnswSearchIndexParamsNames = new String[] { "proxima.hnsw.searcher.ef" };
    public static final String[] hnswSearchIndexParamsValues = new String[] { "500" };
    public static final String[] hnswBuildIndexParamsNames = new String[] {
        "proxima.hnsw.builder.max_neighbor_count",
        "proxima.hnsw.builder.efconstruction",
        "proxima.hnsw.builder.thread_count" };
    public static final String[] hnswBuildIndexParamsValues = new String[] { "100", "500", "0" };

    // test supported data type
    public void testSupportedDataType() throws Exception {
        String index = "index_supported_data_type";
        // create index
        assertTrue(
            highLevelClient().indices()
                .create(
                    new CreateIndexRequest(index).settings(
                        Settings.builder()
                            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
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
    public void testUpdatedVectorData() throws Exception {
        String index = "index_updated_vector_data";
        String fieldName = "image";
        final String[] indexOptionNames = new String[] {
            "type",
            "embedding_delimiter",
            "distance_type",
            "major_order",
            "enable_rt_build",
            "ignore_invalid_doc",
            "enable_recall_report",
            "is_embedding_saved",
            "min_scan_doc_cnt",
            "linear_build_threshold" };
        final String[] hnswIndexOptionValues = new String[] {
            "hnsw",
            ",",
            "InnerProduct",
            "row",
            "true",
            "true",
            "true",
            "true",
            "20000",
            "500" };

        int vectorDims = 2;
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
                    ).mapping(createVectorMapping(vectorDims, fieldName, indexOptionNames, hnswIndexOptionValues)),
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

    private static XContentBuilder createVectorMapping(
        int vectorDims,
        String fieldName,
        String[] indexOptionNames,
        String[] IndexOptionValues
    ) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject(fieldName);
                {
                    mappingBuilder.field("type", "dense_vector");
                    mappingBuilder.field("dims", vectorDims);
                    mappingBuilder.field("similarity", "dot_product");
                    mappingBuilder.startObject("index_options");
                    {
                        for (int i = 0; i < indexOptionNames.length; i++) {
                            mappingBuilder.field(indexOptionNames[i], IndexOptionValues[i]);
                        }
                        mappingBuilder.startObject("search_index_params");
                        {
                            for (int i = 0; i < hnswSearchIndexParamsNames.length; i++) {
                                mappingBuilder.field(hnswSearchIndexParamsNames[i], hnswSearchIndexParamsValues[i]);
                            }
                        }
                        mappingBuilder.endObject();
                        mappingBuilder.startObject("build_index_params");
                        {
                            for (int i = 0; i < hnswBuildIndexParamsNames.length; i++) {
                                mappingBuilder.field(hnswBuildIndexParamsNames[i], hnswBuildIndexParamsValues[i]);
                            }
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        return mappingBuilder;
    }
}
