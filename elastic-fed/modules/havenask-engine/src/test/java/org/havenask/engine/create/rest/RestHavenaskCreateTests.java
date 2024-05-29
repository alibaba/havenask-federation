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

package org.havenask.engine.create.rest;

import org.havenask.common.settings.Settings;
import org.havenask.index.mapper.MapperServiceTestCase;

import java.util.Locale;
import java.util.Map;

public class RestHavenaskCreateTests extends MapperServiceTestCase {
    public static RestHavenaskCreate restHandler = new RestHavenaskCreate();

    public void testClusterJsonValidate() {
        String indexName = randomAlphaOfLength(5);
        Map<String, Object> sourceSettings = Map.of(
            "index.number_of_shards",
            "1",
            "index.havenask.hash_mode.hash_field",
            "_id",
            "index.havenask.build_config.max_doc_count",
            "10000",
            "index.havenask.wal_config.sink.queue_size",
            "5000"
        );

        String clustersJsonStr = String.format(
            Locale.ROOT,
            "{\n"
                + "    \"online_index_config\": {\n"
                + "\t\t\"build_config\": {\n"
                + "\t\t\t\"max_doc_count\": 10000\n"
                + "\t\t}\n"
                + "\t},\n"
                + "\n"
                + "    \"cluster_config\" : {\n"
                + "        \"builder_rule_config\" : {\n"
                + "            \"partition_count\" : 1\n"
                + "        },\n"
                + "        \"cluster_name\" : \"%s\",\n"
                + "        \"hash_mode\" : {\n"
                + "            \"hash_field\" : \"_id\",\n"
                + "            \"hash_function\" : \"HASH\"\n"
                + "        },\n"
                + "        \"table_name\" : \"%s\"\n"
                + "    },\n"
                + "    \n"
                + "    \"direct_write\" : true, \n"
                + "    \"wal_config\": { \n"
                + "        \"strategy\": \"queue\",\n"
                + "        \"sink\": {\n"
                + "            \"queue_name\": \"%s\",\n"
                + "            \"queue_size\": \"5000\"\n"
                + "        }\n"
                + "    }\n"
                + "}",
            indexName,
            indexName,
            indexName
        );

        Settings settings = Settings.builder().loadFromMap(sourceSettings).build();
        restHandler.clusterJsonValidate(indexName, clustersJsonStr, settings);
    }

    public void testIllegalClusterJsonParams() {
        String indexName = randomAlphaOfLength(5);
        Map<String, Object> sourceSettings = Map.of(
            "index.number_of_shards",
            "1",
            "index.havenask.hash_mode.hash_field",
            "_id",
            "index.havenask.build_config.max_doc_count",
            "10000",
            "index.havenask.wal_config.sink.queue_size",
            "5000"
        );

        String illegalClusterName = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"cluster_name\" : \"errorIndexName\"\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalClusterName, sourceSettings);

        String illegalTableName = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"table_name\" : \"errorIndexName\"\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalTableName, sourceSettings);

        String illegalQueueName = "{\n"
            + "    \"wal_config\": { \n"
            + "        \"sink\": {\n"
            + "            \"queue_name\": \"errorIndexName\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalQueueName, sourceSettings);

        String illegalPartitionCount = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"builder_rule_config\" : {\n"
            + "            \"partition_count\" : 2\n"
            + "        }\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalPartitionCount, sourceSettings);

        String illegalDirectWrite = "{\n" + "    \"direct_write\" : false, \n" + "}";
        validateIllegalClusterJsonParams(indexName, illegalDirectWrite, sourceSettings);

        String illegalStrategy = "{\n" + "    \"wal_config\": { \n" + "        \"strategy\": \"errorStrategy\"\n" + "    }\n" + "}";
        validateIllegalClusterJsonParams(indexName, illegalStrategy, sourceSettings);

        String illegalHashField = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"hash_mode\" : {\n"
            + "            \"hash_field\" : \"errorField\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalHashField, sourceSettings);

        String illegalHashFunction = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"hash_mode\" : {\n"
            + "            \"hash_function\" : \"errorFunction\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalHashFunction, sourceSettings);

        String illegalSize = "{\n"
            + "    \"wal_config\": { \n"
            + "        \"sink\": {\n"
            + "            \"queue_size\": \"1000\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalSize, sourceSettings);

        String illegalMaxDocCount = "{\n"
            + "    \"online_index_config\": {\n"
            + "        \"build_config\": {\n"
            + "            \"max_doc_count\": 2000\n"
            + "        }\n"
            + "    }\n"
            + "}";
        validateIllegalClusterJsonParams(indexName, illegalMaxDocCount, sourceSettings);

        Map<String, Object> errorSourceSettings = Map.of("index.engine", "lucene");
        validateIllegalClusterJsonParams(indexName, null, errorSourceSettings);

        Map<String, Object> defaultNumberOfShardsSourceSettings = Map.of(
            "index.havenask.hash_mode.hash_field",
            "_id",
            "index.havenask.build_config.max_doc_count",
            "10000",
            "index.havenask.wal_config.sink.queue_size",
            "5000"
        );
        validateIllegalClusterJsonParams(indexName, illegalPartitionCount, defaultNumberOfShardsSourceSettings);
    }

    private void validateIllegalClusterJsonParams(String indexName, String illegalClusterJsonStr, Map<String, Object> sourceSettings) {
        Settings settings = Settings.builder().loadFromMap(sourceSettings).build();
        expectThrows(IllegalArgumentException.class, () -> restHandler.clusterJsonValidate(indexName, illegalClusterJsonStr, settings));
    }

    public void testSchemaJsonValidate() {
        String indexName = randomAlphaOfLength(5);
        String schemaJsonStr = String.format(
            Locale.ROOT,
            "{\n"
                + "    \"attributes\":[\"_seq_no\",\"_id\",\"_version\",\"_primary_term\"],\n"
                + "    \"fields\":[{\n"
                + "        \"analyzer\":\"simple_analyzer\",\n"
                + "        \"binary_field\":false,\n"
                + "        \"field_name\":\"foo\",\n"
                + "        \"field_type\":\"TEXT\"\n"
                + "    }],\n"
                + "    \"indexs\":[{\n"
                + "        \"doc_payload_flag\":1,\n"
                + "        \"index_fields\":\"foo\",\n"
                + "        \"index_name\":\"foo\",\n"
                + "        \"index_type\":\"TEXT\",\n"
                + "        \"position_list_flag\":1,\n"
                + "        \"position_payload_flag\":1,\n"
                + "        \"term_frequency_flag\":1\n"
                + "    }],\n"
                + "    \"table_name\":\"%s\",\n"
                + "    \"table_type\":\"normal\"\n"
                + "}",
            indexName
        );

        restHandler.schemaJsonValidate(indexName, schemaJsonStr);
    }

    public void testIllegalSchemaJsonParams() {
        String indexName = randomAlphaOfLength(5);

        String wrongIndexNameSchemaJsonStr = "{\n" + "    \"table_name\":\"wrong_index_name\",\n" + "    \"table_type\":\"normal\"\n" + "}";

        IllegalArgumentException indexError = expectThrows(
            IllegalArgumentException.class,
            () -> restHandler.schemaJsonValidate(indexName, wrongIndexNameSchemaJsonStr)
        );
        assertEquals(
            String.format(Locale.ROOT, "'schema' Value:'table_name' is expected to be '%s', but found 'wrong_index_name'", indexName),
            indexError.getMessage()
        );

        String wrongIndexIdSchemaJsonStr = "{\n"
            + "\t\"indexs\":[\n"
            + "\t\t{\n"
            + "\t\t\t\"index_fields\":\"_id\",\n"
            + "\t\t\t\"index_name\":\"_id\",\n"
            + "\t\t\t\"index_type\":\"STRING\"\n"
            + "\t\t}\n"
            + "\t]\n"
            + "}";

        IllegalArgumentException indexIdError = expectThrows(
            IllegalArgumentException.class,
            () -> restHandler.schemaJsonValidate(indexName, wrongIndexIdSchemaJsonStr)
        );

        assertEquals(
            "['schema.indexs.index_name':'_id'] is an internal parameter of fed, " + "please do not configure it.",
            indexIdError.getMessage()
        );

        String wrongFieldSourceSchemaJsonStr = "{\n"
            + "\t\"fields\":[\n"
            + "\t\t{\n"
            + "\t\t\t\"binary_field\":false,\n"
            + "\t\t\t\"field_name\":\"_source\",\n"
            + "\t\t\t\"field_type\":\"INT64\"\n"
            + "\t\t}\n"
            + "\t]\n"
            + "}";

        IllegalArgumentException fieldSourceError = expectThrows(
            IllegalArgumentException.class,
            () -> restHandler.schemaJsonValidate(indexName, wrongFieldSourceSchemaJsonStr)
        );

        assertEquals(
            "['schema.fields.field_name':'_source'] is an internal parameter of fed, " + "please do not configure it.",
            fieldSourceError.getMessage()
        );

        String wrongIndexRoutingSchemaJsonStr = "{\n"
            + "\t\"indexs\":[\n"
            + "\t\t{\n"
            + "\t\t\t\"has_primary_key_attribute\":true,\n"
            + "\t\t\t\"index_fields\":\"_id\",\n"
            + "\t\t\t\"index_name\":\"_id\",\n"
            + "\t\t\t\"index_type\":\"PRIMARYKEY64\",\n"
            + "\t\t\t\"is_primary_key_sorted\":false\n"
            + "\t\t},\n"
            + "\t\t{\n"
            + "\t\t\t\"index_fields\":\"_routing\",\n"
            + "\t\t\t\"index_name\":\"_routing\",\n"
            + "\t\t\t\"index_type\":\"INT64\"\n"
            + "\t\t}\n"
            + "\t]\n"
            + "}";

        IllegalArgumentException indexRoutingError = expectThrows(
            IllegalArgumentException.class,
            () -> restHandler.schemaJsonValidate(indexName, wrongIndexRoutingSchemaJsonStr)
        );

        assertEquals(
            "['schema.indexs.index_name':'_routing'] is an internal parameter of fed, please do not configure it.",
            indexRoutingError.getMessage()
        );

        String illegalIndexNameSchemaJsonStr = "{\n"
            + "\t\"indexs\":[\n"
            + "{\n"
            + "\t\t\"index_fields\":\"summary\",\n"
            + "\t\t\"index_name\":\"summary\",\n"
            + "\t\t\"index_type\":\"STRING\"\n"
            + "\t}"
            + "\t]\n"
            + "}";

        IllegalArgumentException illegalIndexNameError = expectThrows(
            IllegalArgumentException.class,
            () -> restHandler.schemaJsonValidate(indexName, illegalIndexNameSchemaJsonStr)
        );

        assertEquals(
            "['schema.indexs.index_name':'summary'] is an unsupported index_name of fed, " + "havenask index_name cannot be 'summary'",
            illegalIndexNameError.getMessage()
        );
    }

    public void testDataTableJsonValidate() {
        String indexName = randomAlphaOfLength(5);
        String dataTableJson = String.format(
            Locale.ROOT,
            "{\n"
                + "    \"processor_chain_config\" : [\n"
                + "        {\n"
                + "            \"clusters\" : [\n"
                + "                \"%s\"\n"
                + "            ],\n"
                + "            \"document_processor_chain\" : [\n"
                + "                {\n"
                + "                    \"class_name\" : \"TokenizeDocumentProcessor\",\n"
                + "                    \"module_name\" : \"\",\n"
                + "                    \"parameters\" : {\n"
                + "                    }\n"
                + "                }\n"
                + "            ],\n"
                + "            \"modules\" : [\n"
                + "            ]\n"
                + "        }\n"
                + "    ]\n"
                + "}",
            indexName
        );

        restHandler.dataTableJsonValidate(indexName, dataTableJson);
    }

    public void validateIllegalDataTableJsonParams() {
        String indexName = randomAlphaOfLength(5);
        String wrongDataTableJson = "{\n"
            + "    \"processor_chain_config\" : [\n"
            + "        {\n"
            + "            \"clusters\" : [\n"
            + "                \"wrong_index_name\"\n"
            + "            ],\n"
            + "            \"document_processor_chain\" : [\n"
            + "                {\n"
            + "                    \"class_name\" : \"TokenizeDocumentProcessor\",\n"
            + "                    \"module_name\" : \"\",\n"
            + "                    \"parameters\" : {\n"
            + "                    }\n"
            + "                }\n"
            + "            ],\n"
            + "            \"modules\" : [\n"
            + "            ]\n"
            + "        }\n"
            + "    ]\n"
            + "}";
        expectThrows(IllegalArgumentException.class, () -> restHandler.schemaJsonValidate(indexName, wrongDataTableJson));
    }
}
