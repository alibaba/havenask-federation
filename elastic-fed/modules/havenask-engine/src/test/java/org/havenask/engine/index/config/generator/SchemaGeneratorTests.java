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

package org.havenask.engine.index.config.generator;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AnalyzerScope;
import org.havenask.engine.index.analysis.HavenaskJiebaAnalyzer;
import org.havenask.engine.index.analysis.HavenaskSimpleAnalyzer;
import org.havenask.engine.index.analysis.HavenaskSinglewsAnalyzer;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperServiceTestCase;
import org.havenask.index.similarity.SimilarityService;
import org.havenask.indices.IndicesModule;
import org.havenask.indices.mapper.MapperRegistry;
import org.havenask.plugins.MapperPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.ScriptPlugin;
import org.havenask.script.ScriptModule;
import org.havenask.script.ScriptService;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class SchemaGeneratorTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new HavenaskEnginePlugin(Settings.EMPTY));
    }

    private String indexName = randomAlphaOfLength(5);

    public void testSchemaGenerate() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            {
                b.startObject("name");
                {
                    b.field("type", "text");
                }
                b.endObject();
                b.startObject("age");
                {
                    b.field("type", "integer");
                }
                b.endObject();
                b.startObject("price");
                {
                    b.field("type", "float");
                }
                b.endObject();
                b.startObject("date");
                {
                    b.field("type", "date");
                }
                b.endObject();
                b.startObject("bool");
                {
                    b.field("type", "boolean");
                }
                b.endObject();
                b.startObject("byte");
                {
                    b.field("type", "byte");
                }
                b.endObject();
                b.startObject("short");
                {
                    b.field("type", "short");
                }
                b.endObject();
                b.startObject("long");
                {
                    b.field("type", "long");
                }
                b.endObject();
                b.startObject("double");
                {
                    b.field("type", "double");
                }
                b.endObject();
                b.startObject("keyword");
                {
                    b.field("type", "keyword");
                }
                b.endObject();
                b.startObject("text");
                {
                    b.field("type", "text");
                }
                b.endObject();
                // object type
                b.startObject("object");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("name");
                        {
                            b.field("type", "text");
                        }
                        b.endObject();
                        b.startObject("age");
                        {
                            b.field("type", "integer");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
        }));
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, Settings.EMPTY, mapperService);
        String actual = schema.toString();
        String expect = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"attributes\":[\"_id\",\"date\",\"bool\",\"byte\",\"double\",\"long\",\"_seq_no\",\"object_age\","
                + "\"price\",\"short\",\"keyword\",\"_version\",\"age\",\"_primary_term\"],\n"
                + "\t\"fields\":[{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_id\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_routing\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"date\",\n"
                + "\t\t\"field_type\":\"UINT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"bool\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"byte\",\n"
                + "\t\t\"field_type\":\"INT8\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"double\",\n"
                + "\t\t\"field_type\":\"DOUBLE\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"long\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"analyzer\":\"simple_analyzer\",\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"object_name\",\n"
                + "\t\t\"field_type\":\"TEXT\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_seq_no\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"object_age\",\n"
                + "\t\t\"field_type\":\"INTEGER\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"price\",\n"
                + "\t\t\"field_type\":\"FLOAT\"\n"
                + "\t},{\n"
                + "\t\t\"analyzer\":\"simple_analyzer\",\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"name\",\n"
                + "\t\t\"field_type\":\"TEXT\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"short\",\n"
                + "\t\t\"field_type\":\"INT16\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_source\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"analyzer\":\"simple_analyzer\",\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"text\",\n"
                + "\t\t\"field_type\":\"TEXT\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"keyword\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_version\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"age\",\n"
                + "\t\t\"field_type\":\"INTEGER\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_primary_term\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t}],\n"
                + "\t\"indexs\":[{\n"
                + "\t\t\"has_primary_key_attribute\":true,\n"
                + "\t\t\"index_fields\":\"_id\",\n"
                + "\t\t\"index_name\":\"_id\",\n"
                + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                + "\t\t\"is_primary_key_sorted\":false\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_routing\",\n"
                + "\t\t\"index_name\":\"_routing\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"date\",\n"
                + "\t\t\"index_name\":\"date\",\n"
                + "\t\t\"index_type\":\"DATE\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"bool\",\n"
                + "\t\t\"index_name\":\"bool\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"byte\",\n"
                + "\t\t\"index_name\":\"byte\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"long\",\n"
                + "\t\t\"index_name\":\"long\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"doc_payload_flag\":1,\n"
                + "\t\t\"index_fields\":\"object_name\",\n"
                + "\t\t\"index_name\":\"object_name\",\n"
                + "\t\t\"index_type\":\"TEXT\",\n"
                + "\t\t\"position_list_flag\":1,\n"
                + "\t\t\"position_payload_flag\":1,\n"
                + "\t\t\"term_frequency_flag\":1\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_seq_no\",\n"
                + "\t\t\"index_name\":\"_seq_no\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"object_age\",\n"
                + "\t\t\"index_name\":\"object_age\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"doc_payload_flag\":1,\n"
                + "\t\t\"index_fields\":\"name\",\n"
                + "\t\t\"index_name\":\"name\",\n"
                + "\t\t\"index_type\":\"TEXT\",\n"
                + "\t\t\"position_list_flag\":1,\n"
                + "\t\t\"position_payload_flag\":1,\n"
                + "\t\t\"term_frequency_flag\":1\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"short\",\n"
                + "\t\t\"index_name\":\"short\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"doc_payload_flag\":1,\n"
                + "\t\t\"index_fields\":\"text\",\n"
                + "\t\t\"index_name\":\"text\",\n"
                + "\t\t\"index_type\":\"TEXT\",\n"
                + "\t\t\"position_list_flag\":1,\n"
                + "\t\t\"position_payload_flag\":1,\n"
                + "\t\t\"term_frequency_flag\":1\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"keyword\",\n"
                + "\t\t\"index_name\":\"keyword\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"age\",\n"
                + "\t\t\"index_name\":\"age\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t}],\n"
                + "\t\"settings\":{\n"
                + "\t\t\"enable_all_text_field_meta\":true\n"
                + "\t},\n"
                + "\t\"summarys\":{\n"
                + "\t\t\"compress\":true,\n"
                + "\t\t\"summary_fields\":[\"_id\",\"_routing\",\"_source\"]\n"
                + "\t},\n"
                + "\t\"table_name\":\"%s\",\n"
                + "\t\"table_type\":\"normal\"\n"
                + "}",
            indexName
        );
        assertEquals(expect, actual);
    }

    // test not support nested type
    public void testNestedType() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            {
                // nested type
                b.startObject("nested_field");
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("name");
                    b.field("type", "text");
                    b.endObject();
                    b.startObject("age");
                    b.field("type", "integer");
                    b.endObject();
                }
                b.endObject();
                b.endObject();
            }
        }));
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        // java.lang.UnsupportedOperationException: nested field not support
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> schemaGenerator.getSchema(indexName, Settings.EMPTY, mapperService)
        );
        assertEquals("nested field not support", e.getMessage());
    }

    // test not support geo_point type
    public void testGeoPointType() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            {
                // geo_point type
                b.startObject("geo_point_field");
                b.field("type", "geo_point");
                b.endObject();
            }
        }));
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        // java.lang.UnsupportedOperationException: geo_point field not support
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> schemaGenerator.getSchema(indexName, Settings.EMPTY, mapperService)
        );
        assertEquals("no support mapping type (geo_point) for field geo_point_field", e.getMessage());
    }

    // test default schema
    public void testDefaultSchema() throws IOException {
        MapperService mapperService = null;
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, Settings.EMPTY, mapperService);
        String actual = schema.toString();
        String expect = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"attributes\":[\n"
                + "\t\t\"_seq_no\",\n"
                + "\t\t\"_id\",\n"
                + "\t\t\"_version\",\n"
                + "\t\t\"_primary_term\"\n"
                + "\t],\n"
                + "\t\"fields\":[\n"
                + "\t\t{\n"
                + "\t\t\t\"binary_field\":false,\n"
                + "\t\t\t\"field_name\":\"_routing\",\n"
                + "\t\t\t\"field_type\":\"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"binary_field\":false,\n"
                + "\t\t\t\"field_name\":\"_seq_no\",\n"
                + "\t\t\t\"field_type\":\"INT64\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"binary_field\":false,\n"
                + "\t\t\t\"field_name\":\"_source\",\n"
                + "\t\t\t\"field_type\":\"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"binary_field\":false,\n"
                + "\t\t\t\"field_name\":\"_id\",\n"
                + "\t\t\t\"field_type\":\"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"binary_field\":false,\n"
                + "\t\t\t\"field_name\":\"_version\",\n"
                + "\t\t\t\"field_type\":\"INT64\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"binary_field\":false,\n"
                + "\t\t\t\"field_name\":\"_primary_term\",\n"
                + "\t\t\t\"field_type\":\"INT64\"\n"
                + "\t\t}\n"
                + "\t],\n"
                + "\t\"indexs\":[\n"
                + "\t\t{\n"
                + "\t\t\t\"index_fields\":\"_routing\",\n"
                + "\t\t\t\"index_name\":\"_routing\",\n"
                + "\t\t\t\"index_type\":\"STRING\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"index_fields\":\"_seq_no\",\n"
                + "\t\t\t\"index_name\":\"_seq_no\",\n"
                + "\t\t\t\"index_type\":\"NUMBER\"\n"
                + "\t\t},\n"
                + "\t\t{\n"
                + "\t\t\t\"has_primary_key_attribute\":true,\n"
                + "\t\t\t\"index_fields\":\"_id\",\n"
                + "\t\t\t\"index_name\":\"_id\",\n"
                + "\t\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                + "\t\t\t\"is_primary_key_sorted\":false\n"
                + "\t\t}\n"
                + "\t],\n"
                + "\t\"settings\":{\n"
                + "\t\t\"enable_all_text_field_meta\":true\n"
                + "\t},\n"
                + "\t\"summarys\":{\n"
                + "\t\t\"compress\":true,\n"
                + "\t\t\"summary_fields\":[\n"
                + "\t\t\t\"_routing\",\n"
                + "\t\t\t\"_source\",\n"
                + "\t\t\t\"_id\"\n"
                + "\t\t]\n"
                + "\t},\n"
                + "\t\"table_name\":\"%s\",\n"
                + "\t\"table_type\":\"normal\"\n"
                + "}",
            indexName
        );
        assertEquals(expect, actual);
    }

    // test index vector
    public void testIndexVector() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            {
                b.startObject("field");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 128);
                }
                b.endObject();
            }
        }));
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, Settings.EMPTY, mapperService);
        String actual = schema.toString();
        String expected = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"attributes\":[\"_id\",\"_seq_no\",\"_version\",\"_primary_term\"],\n"
                + "\t\"fields\":[{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_id\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_routing\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_seq_no\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"field\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_source\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_version\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"DUP_field\",\n"
                + "\t\t\"field_type\":\"RAW\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_primary_term\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t}],\n"
                + "\t\"indexs\":[{\n"
                + "\t\t\"has_primary_key_attribute\":true,\n"
                + "\t\t\"index_fields\":\"_id\",\n"
                + "\t\t\"index_name\":\"_id\",\n"
                + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                + "\t\t\"is_primary_key_sorted\":false\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_routing\",\n"
                + "\t\t\"index_name\":\"_routing\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_seq_no\",\n"
                + "\t\t\"index_name\":\"_seq_no\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":[\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"_id\"\n"
                + "\t\t\t},\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"DUP_field\"\n"
                + "\t\t\t}\n"
                + "\t\t],\n"
                + "\t\t\"index_name\":\"field\",\n"
                + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                + "\t\t\"indexer\":\"aitheta2_indexer\",\n"
                + "\t\t\"parameters\":{\n"
                + "\t\t\t\"dimension\":\"128\",\n"
                + "\t\t\t\"enable_rt_build\":\"true\",\n"
                + "\t\t\t\"distance_type\":\"SquaredEuclidean\",\n"
                + "\t\t\t\"ignore_invalid_doc\":\"true\",\n"
                + "\t\t\t\"builder_name\":\"HnswBuilder\",\n"
                + "\t\t\t\"searcher_name\":\"HnswSearcher\"\n"
                + "\t\t}\n"
                + "\t}],\n"
                + "\t\"settings\":{\n"
                + "\t\t\"enable_all_text_field_meta\":true\n"
                + "\t},\n"
                + "\t\"summarys\":{\n"
                + "\t\t\"compress\":true,\n"
                + "\t\t\"summary_fields\":[\"_id\",\"_routing\",\"_source\"]\n"
                + "\t},\n"
                + "\t\"table_name\":\"%s\",\n"
                + "\t\"table_type\":\"normal\"\n"
                + "}",
            indexName
        );
        assertEquals(expected, actual);
    }

    // test index vector with all parameters
    public void testIndexVectorWithAllParameters() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            {
                b.startObject("linear_field");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 128);
                    b.field("similarity", "L2_NORM");
                    b.startObject("index_options");
                    {
                        b.field("type", "linear");
                        b.field("embedding_delimiter", ",");
                        b.field("major_order", "row");
                        b.field("ignore_invalid_doc", "true");
                        b.field("enable_recall_report", "true");
                        b.field("is_embedding_saved", "true");
                        b.field("min_scan_doc_cnt", "20000");
                        b.field("linear_build_threshold", "500");
                        b.startObject("build_index_params");
                        {
                            b.field("proxima.linear.builder.column_major_order", "false");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();

                b.startObject("qc_field");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 128);
                    b.field("similarity", "DOT_PRODUCT");
                    b.startObject("index_options");
                    {
                        b.field("type", "qc");
                        b.field("embedding_delimiter", ",");
                        b.field("major_order", "row");
                        b.field("ignore_invalid_doc", "true");
                        b.field("enable_recall_report", "true");
                        b.field("is_embedding_saved", "true");
                        b.field("min_scan_doc_cnt", "20000");
                        b.field("linear_build_threshold", "500");
                        b.startObject("build_index_params");
                        {
                            b.field("proxima.qc.builder.train_sample_count", "0");
                            b.field("proxima.qc.builder.thread_count", "0");
                            b.field("proxima.qc.builder.centroid_count", "1000");
                            b.field("proxima.qc.builder.cluster_auto_tuning", "false");
                            b.field("proxima.qc.builder.quantize_by_centroid", "false");
                            b.field("proxima.qc.builder.store_original_features", "false");
                            b.field("proxima.qc.builder.train_sample_ratio", "1.0");
                        }
                        b.endObject();
                        b.startObject("search_index_params");
                        {
                            b.field("proxima.qc.searcher.scan_ratio", "0.01");
                            b.field("proxima.qc.searcher.brute_force_threshold", "1000");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();

                b.startObject("hnsw_field");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 128);
                    b.field("similarity", "L2_NORM");
                    b.startObject("index_options");
                    {
                        b.field("type", "HNSW");
                        b.field("embedding_delimiter", ",");
                        b.field("major_order", "row");
                        b.field("ignore_invalid_doc", "true");
                        b.field("enable_recall_report", "true");
                        b.field("is_embedding_saved", "true");
                        b.field("min_scan_doc_cnt", "20000");
                        b.field("linear_build_threshold", "500");

                        b.startObject("build_index_params");
                        {
                            b.field("proxima.hnsw.builder.max_neighbor_count", "100");
                            b.field("proxima.hnsw.builder.efconstruction", "500");
                            b.field("proxima.hnsw.builder.thread_count", "0");
                        }
                        b.endObject();

                        b.startObject("search_index_params");
                        {
                            b.field("proxima.hnsw.searcher.ef", "500");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
        }));

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, Settings.EMPTY, mapperService);
        String actual = schema.toString();
        String expected = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"attributes\":[\"_id\",\"_seq_no\",\"_version\",\"_primary_term\"],\n"
                + "\t\"fields\":[{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_id\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_routing\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"linear_field\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"hnsw_field\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_seq_no\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"qc_field\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_source\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_version\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"DUP_linear_field\",\n"
                + "\t\t\"field_type\":\"RAW\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"DUP_hnsw_field\",\n"
                + "\t\t\"field_type\":\"RAW\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"DUP_qc_field\",\n"
                + "\t\t\"field_type\":\"RAW\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_primary_term\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t}],\n"
                + "\t\"indexs\":[{\n"
                + "\t\t\"has_primary_key_attribute\":true,\n"
                + "\t\t\"index_fields\":\"_id\",\n"
                + "\t\t\"index_name\":\"_id\",\n"
                + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                + "\t\t\"is_primary_key_sorted\":false\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_routing\",\n"
                + "\t\t\"index_name\":\"_routing\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_seq_no\",\n"
                + "\t\t\"index_name\":\"_seq_no\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":[\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"_id\"\n"
                + "\t\t\t},\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"DUP_linear_field\"\n"
                + "\t\t\t}\n"
                + "\t\t],\n"
                + "\t\t\"index_name\":\"linear_field\",\n"
                + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                + "\t\t\"indexer\":\"aitheta2_indexer\",\n"
                + "\t\t\"parameters\":{\n"
                + "\t\t\t\"dimension\":\"128\",\n"
                + "\t\t\t\"enable_rt_build\":\"true\",\n"
                + "\t\t\t\"distance_type\":\"SquaredEuclidean\",\n"
                + "\t\t\t\"embedding_delimiter\":\",\",\n"
                + "\t\t\t\"major_order\":\"row\",\n"
                + "\t\t\t\"ignore_invalid_doc\":\"true\",\n"
                + "\t\t\t\"enable_recall_report\":\"true\",\n"
                + "\t\t\t\"is_embedding_saved\":\"true\",\n"
                + "\t\t\t\"min_scan_doc_cnt\":\"20000\",\n"
                + "\t\t\t\"linear_build_threshold\":\"500\",\n"
                + "\t\t\t\"builder_name\":\"LinearBuilder\",\n"
                + "\t\t\t\"searcher_name\":\"LinearSearcher\",\n"
                + "\t\t\t\"searcher_index_params\":\"{\\\"proxima.hnsw.builder.linear_build_threshold\\\":500}\"\n"
                + "\t\t}\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":[\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"_id\"\n"
                + "\t\t\t},\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"DUP_hnsw_field\"\n"
                + "\t\t\t}\n"
                + "\t\t],\n"
                + "\t\t\"index_name\":\"hnsw_field\",\n"
                + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                + "\t\t\"indexer\":\"aitheta2_indexer\",\n"
                + "\t\t\"parameters\":{\n"
                + "\t\t\t\"dimension\":\"128\",\n"
                + "\t\t\t\"enable_rt_build\":\"true\",\n"
                + "\t\t\t\"distance_type\":\"SquaredEuclidean\",\n"
                + "\t\t\t\"embedding_delimiter\":\",\",\n"
                + "\t\t\t\"major_order\":\"row\",\n"
                + "\t\t\t\"ignore_invalid_doc\":\"true\",\n"
                + "\t\t\t\"enable_recall_report\":\"true\",\n"
                + "\t\t\t\"is_embedding_saved\":\"true\",\n"
                + "\t\t\t\"min_scan_doc_cnt\":\"20000\",\n"
                + "\t\t\t\"linear_build_threshold\":\"500\",\n"
                + "\t\t\t\"builder_name\":\"HnswBuilder\",\n"
                + "\t\t\t\"searcher_name\":\"HnswSearcher\",\n"
                + "\t\t\t\"searcher_index_params\":\"{\\\"proxima.hnsw.searcher.ef\\\":500}\",\n"
                + "\t\t\t\"builder_index_params\":\"{\\\"proxima.hnsw.builder.max_neighbor_cnt\\\":100,"
                + "\\\"proxima.hnsw.builder.ef_construction\\\":500,\\\"proxima.hnsw.builder.thread_cnt\\\":0}\"\n"
                + "\t\t}\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":[\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"_id\"\n"
                + "\t\t\t},\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"DUP_qc_field\"\n"
                + "\t\t\t}\n"
                + "\t\t],\n"
                + "\t\t\"index_name\":\"qc_field\",\n"
                + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                + "\t\t\"indexer\":\"aitheta2_indexer\",\n"
                + "\t\t\"parameters\":{\n"
                + "\t\t\t\"dimension\":\"128\",\n"
                + "\t\t\t\"enable_rt_build\":\"true\",\n"
                + "\t\t\t\"distance_type\":\"InnerProduct\",\n"
                + "\t\t\t\"embedding_delimiter\":\",\",\n"
                + "\t\t\t\"major_order\":\"row\",\n"
                + "\t\t\t\"ignore_invalid_doc\":\"true\",\n"
                + "\t\t\t\"enable_recall_report\":\"true\",\n"
                + "\t\t\t\"is_embedding_saved\":\"true\",\n"
                + "\t\t\t\"min_scan_doc_cnt\":\"20000\",\n"
                + "\t\t\t\"linear_build_threshold\":\"500\",\n"
                + "\t\t\t\"builder_name\":\"QcBuilder\",\n"
                + "\t\t\t\"searcher_name\":\"QcSearcher\",\n"
                + "\t\t\t\"searcher_index_params\":\"{\\\"proxima.qc.searcher.scan_ratio\\\":0.01,"
                + "\\\"proxima.qc.searcher.brute_force_threshold\\\":1000}\",\n"
                + "\t\t\t\"builder_index_params\":\"{\\\"proxima.qc.builder.train_sample_count\\\":0,"
                + "\\\"proxima.qc.builder.thread_count\\\":0,\\\"proxima.qc.builder.centroid_count\\\":\\\"1000\\\","
                + "\\\"proxima.qc.builder.cluster_auto_tuning\\\":false,\\\"proxima.qc.builder.quantize_by_centroid\\\":false,"
                + "\\\"proxima.qc.builder.store_original_features\\\":false,\\\"proxima.qc.builder.train_sample_ratio\\\":1.0}\"\n"
                + "\t\t}\n"
                + "\t}],\n"
                + "\t\"settings\":{\n"
                + "\t\t\"enable_all_text_field_meta\":true\n"
                + "\t},\n"
                + "\t\"summarys\":{\n"
                + "\t\t\"compress\":true,\n"
                + "\t\t\"summary_fields\":[\"_id\",\"_routing\",\"_source\"]\n"
                + "\t},\n"
                + "\t\"table_name\":\"%s\",\n"
                + "\t\"table_type\":\"normal\"\n"
                + "}",
            indexName
        );
        assertEquals(expected, actual);
    }

    // test havenask analyzer: simple_analyzer, singlews_analyzer and jieba_analyzer
    public void testHavenaskAnalyzer() throws IOException {
        MapperService mapperService = createMapperServiceIncludingHavenaskAnalyzer(Version.CURRENT, mapping(b -> {
            {
                b.startObject("simple_text");
                {
                    b.field("type", "text");
                    b.field("analyzer", "simple_analyzer");
                }
                b.endObject();
                b.startObject("singlews_text");
                {
                    b.field("type", "text");
                    b.field("analyzer", "singlews_analyzer");
                }
                b.endObject();
                b.startObject("jieba_text");
                {
                    b.field("type", "text");
                    b.field("analyzer", "jieba_analyzer");
                }
                b.endObject();
            }
        }));
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, Settings.EMPTY, mapperService);
        String actual = schema.toString();
        String expect = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"attributes\":[\"_id\",\"_seq_no\",\"_version\",\"_primary_term\"],\n"
                + "\t\"fields\":[{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_id\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_routing\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"analyzer\":\"jieba_analyzer\",\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"jieba_text\",\n"
                + "\t\t\"field_type\":\"TEXT\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_seq_no\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"analyzer\":\"simple_analyzer\",\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"simple_text\",\n"
                + "\t\t\"field_type\":\"TEXT\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_source\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"analyzer\":\"singlews_analyzer\",\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"singlews_text\",\n"
                + "\t\t\"field_type\":\"TEXT\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_version\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_primary_term\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t}],\n"
                + "\t\"indexs\":[{\n"
                + "\t\t\"has_primary_key_attribute\":true,\n"
                + "\t\t\"index_fields\":\"_id\",\n"
                + "\t\t\"index_name\":\"_id\",\n"
                + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                + "\t\t\"is_primary_key_sorted\":false\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_routing\",\n"
                + "\t\t\"index_name\":\"_routing\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"doc_payload_flag\":1,\n"
                + "\t\t\"index_fields\":\"jieba_text\",\n"
                + "\t\t\"index_name\":\"jieba_text\",\n"
                + "\t\t\"index_type\":\"TEXT\",\n"
                + "\t\t\"position_list_flag\":1,\n"
                + "\t\t\"position_payload_flag\":1,\n"
                + "\t\t\"term_frequency_flag\":1\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_seq_no\",\n"
                + "\t\t\"index_name\":\"_seq_no\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"doc_payload_flag\":1,\n"
                + "\t\t\"index_fields\":\"simple_text\",\n"
                + "\t\t\"index_name\":\"simple_text\",\n"
                + "\t\t\"index_type\":\"TEXT\",\n"
                + "\t\t\"position_list_flag\":1,\n"
                + "\t\t\"position_payload_flag\":1,\n"
                + "\t\t\"term_frequency_flag\":1\n"
                + "\t},{\n"
                + "\t\t\"doc_payload_flag\":1,\n"
                + "\t\t\"index_fields\":\"singlews_text\",\n"
                + "\t\t\"index_name\":\"singlews_text\",\n"
                + "\t\t\"index_type\":\"TEXT\",\n"
                + "\t\t\"position_list_flag\":1,\n"
                + "\t\t\"position_payload_flag\":1,\n"
                + "\t\t\"term_frequency_flag\":1\n"
                + "\t}],\n"
                + "\t\"settings\":{\n"
                + "\t\t\"enable_all_text_field_meta\":true\n"
                + "\t},\n"
                + "\t\"summarys\":{\n"
                + "\t\t\"compress\":true,\n"
                + "\t\t\"summary_fields\":[\"_id\",\"_routing\",\"_source\"]\n"
                + "\t},\n"
                + "\t\"table_name\":\"%s\",\n"
                + "\t\"table_type\":\"normal\"\n"
                + "}",
            indexName
        );
        assertEquals(expect, actual);
    }

    public void testVectorWithCategory() throws IOException {
        String[] illegalTypes = new String[] { "text", "double", "float", "boolean", "date" };
        String[] legalTypes = new String[] { "keyword", "long", "integer", "short", "byte" };
        for (String type : illegalTypes) {
            MapperService illegalMapperService = createMapperService(mapping(b -> {
                {
                    b.startObject("text_field");
                    {
                        b.field("type", type);
                    }
                    b.endObject();
                    b.startObject("image_vector");
                    {
                        b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                        b.field("dims", 128);
                        b.field("category", "text_field");
                    }
                    b.endObject();
                }
            }));
            SchemaGenerator schemaGenerator = new SchemaGenerator();

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> schemaGenerator.getSchema(indexName, Settings.EMPTY, illegalMapperService)
            );
            assertEquals(
                "category [text_field] is not a legal type, category must be keyword or a integer value type"
                    + "(long, integer, short, or byte)",
                e.getMessage()
            );
        }

        for (String type : legalTypes) {
            MapperService illegalMapperService = createMapperService(mapping(b -> {
                {
                    b.startObject("text_field");
                    {
                        b.field("type", type);
                    }
                    b.endObject();
                    b.startObject("image_vector");
                    {
                        b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                        b.field("dims", 128);
                        b.field("category", "text_field");
                    }
                    b.endObject();
                }
            }));
            SchemaGenerator schemaGenerator = new SchemaGenerator();
            schemaGenerator.getSchema(indexName, Settings.EMPTY, illegalMapperService);
        }

        // test Object Mapper
        MapperService ObjectMapperService = createMapperService(mapping(b -> {
            {
                b.startObject("type");
                {
                    b.startObject("properties");
                    {
                        b.startObject("file_type");
                        {
                            b.field("type", "integer");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.startObject("image_vector");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 128);
                    b.field("category", "type.file_type");
                }
                b.endObject();
            }
        }));
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema ObjectSchema = schemaGenerator.getSchema(indexName, Settings.EMPTY, ObjectMapperService);
        String ObjectActual = ObjectSchema.toString();
        String ObjectExpected = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"attributes\":[\"_id\",\"_seq_no\",\"type_file_type\",\"_version\",\"_primary_term\"],\n"
                + "\t\"fields\":[{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_id\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_routing\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_seq_no\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"type_file_type\",\n"
                + "\t\t\"field_type\":\"INTEGER\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_source\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_version\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"image_vector\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"DUP_image_vector\",\n"
                + "\t\t\"field_type\":\"RAW\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_primary_term\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t}],\n"
                + "\t\"indexs\":[{\n"
                + "\t\t\"has_primary_key_attribute\":true,\n"
                + "\t\t\"index_fields\":\"_id\",\n"
                + "\t\t\"index_name\":\"_id\",\n"
                + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                + "\t\t\"is_primary_key_sorted\":false\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_routing\",\n"
                + "\t\t\"index_name\":\"_routing\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_seq_no\",\n"
                + "\t\t\"index_name\":\"_seq_no\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"type_file_type\",\n"
                + "\t\t\"index_name\":\"type_file_type\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":[\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"_id\"\n"
                + "\t\t\t},\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"type_file_type\"\n"
                + "\t\t\t},\n"
                + "\t\t\t{\n"
                + "\t\t\t\t\"boost\":1,\n"
                + "\t\t\t\t\"field_name\":\"DUP_image_vector\"\n"
                + "\t\t\t}\n"
                + "\t\t],\n"
                + "\t\t\"index_name\":\"image_vector\",\n"
                + "\t\t\"index_type\":\"CUSTOMIZED\",\n"
                + "\t\t\"indexer\":\"aitheta2_indexer\",\n"
                + "\t\t\"parameters\":{\n"
                + "\t\t\t\"dimension\":\"128\",\n"
                + "\t\t\t\"enable_rt_build\":\"true\",\n"
                + "\t\t\t\"distance_type\":\"SquaredEuclidean\",\n"
                + "\t\t\t\"ignore_invalid_doc\":\"true\",\n"
                + "\t\t\t\"builder_name\":\"HnswBuilder\",\n"
                + "\t\t\t\"searcher_name\":\"HnswSearcher\"\n"
                + "\t\t}\n"
                + "\t}],\n"
                + "\t\"settings\":{\n"
                + "\t\t\"enable_all_text_field_meta\":true\n"
                + "\t},\n"
                + "\t\"summarys\":{\n"
                + "\t\t\"compress\":true,\n"
                + "\t\t\"summary_fields\":[\"_id\",\"_routing\",\"_source\"]\n"
                + "\t},\n"
                + "\t\"table_name\":\"%s\",\n"
                + "\t\"table_type\":\"normal\"\n"
                + "}",
            indexName
        );
        assertEquals(ObjectExpected, ObjectActual);
    }

    protected final MapperService createMapperServiceIncludingHavenaskAnalyzer(Version version, XContentBuilder mapping)
        throws IOException {
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(Settings.builder().put("index.version.created", version))
            .numberOfReplicas(0)
            .numberOfShards(1)
            .build();
        IndexSettings indexSettings = new IndexSettings(meta, getIndexSettings());
        MapperRegistry mapperRegistry = new IndicesModule(
            getPlugins().stream().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
        ).getMapperRegistry();
        ScriptModule scriptModule = new ScriptModule(
            Settings.EMPTY,
            getPlugins().stream().filter(p -> p instanceof ScriptPlugin).map(p -> (ScriptPlugin) p).collect(toList())
        );
        ScriptService scriptService = new ScriptService(getIndexSettings(), scriptModule.engines, scriptModule.contexts);
        SimilarityService similarityService = new SimilarityService(indexSettings, scriptService, emptyMap());
        MapperService mapperService = new MapperService(
            indexSettings,
            createIndexAnalyzersIncludingHavenaskAnalyzer(indexSettings),
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> { throw new UnsupportedOperationException(); },
            () -> true,
            scriptService
        );
        merge(mapperService, mapping);
        return mapperService;
    }

    protected IndexAnalyzers createIndexAnalyzersIncludingHavenaskAnalyzer(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "simple_analyzer",
                new NamedAnalyzer("simple_analyzer", AnalyzerScope.INDEX, new HavenaskSimpleAnalyzer()),
                "singlews_analyzer",
                new NamedAnalyzer("singlews_analyzer", AnalyzerScope.INDEX, new HavenaskSinglewsAnalyzer()),
                "jieba_analyzer",
                new NamedAnalyzer("jieba_analyzer", AnalyzerScope.INDEX, new HavenaskJiebaAnalyzer())
            ),
            emptyMap(),
            emptyMap()
        );
    }
}
