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

        restHandler.clusterJsonValidate(indexName, clustersJsonStr, sourceSettings);
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
        testIllegalParams(indexName, illegalClusterName, sourceSettings);

        String illegalTableName = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"table_name\" : \"errorIndexName\"\n"
            + "    }\n"
            + "}";
        testIllegalParams(indexName, illegalTableName, sourceSettings);

        String illegalQueueName = "{\n"
            + "    \"wal_config\": { \n"
            + "        \"sink\": {\n"
            + "            \"queue_name\": \"errorIndexName\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        testIllegalParams(indexName, illegalQueueName, sourceSettings);

        String illegalPartitionCount = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"builder_rule_config\" : {\n"
            + "            \"partition_count\" : 2\n"
            + "        }\n"
            + "    }\n"
            + "}";
        testIllegalParams(indexName, illegalPartitionCount, sourceSettings);

        String illegalDirectWrite = "{\n" + "    \"direct_write\" : false, \n" + "}";
        testIllegalParams(indexName, illegalDirectWrite, sourceSettings);

        String illegalStrategy = "{\n" + "    \"wal_config\": { \n" + "        \"strategy\": \"errorStrategy\"\n" + "    }\n" + "}";
        testIllegalParams(indexName, illegalStrategy, sourceSettings);

        String illegalHashField = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"hash_mode\" : {\n"
            + "            \"hash_field\" : \"errorField\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        testIllegalParams(indexName, illegalHashField, sourceSettings);

        String illegalHashFunction = "{\n"
            + "    \"cluster_config\" : {\n"
            + "        \"hash_mode\" : {\n"
            + "            \"hash_function\" : \"errorFunction\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        testIllegalParams(indexName, illegalHashFunction, sourceSettings);

        String illegalSize = "{\n"
            + "    \"wal_config\": { \n"
            + "        \"sink\": {\n"
            + "            \"queue_size\": \"1000\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        testIllegalParams(indexName, illegalSize, sourceSettings);

        String illegalMaxDocCount = "{\n"
            + "    \"online_index_config\": {\n"
            + "        \"build_config\": {\n"
            + "            \"max_doc_count\": 2000\n"
            + "        }\n"
            + "    }\n"
            + "}";
        testIllegalParams(indexName, illegalMaxDocCount, sourceSettings);

        Map<String, Object> errorSourceSettings = Map.of("index.engine", "lucene");
        testIllegalParams(indexName, null, errorSourceSettings);

        Map<String, Object> defaultNumberOfShardsSourceSettings = Map.of(
            "index.havenask.hash_mode.hash_field",
            "_id",
            "index.havenask.build_config.max_doc_count",
            "10000",
            "index.havenask.wal_config.sink.queue_size",
            "5000"
        );
        testIllegalParams(indexName, illegalPartitionCount, defaultNumberOfShardsSourceSettings);
    }

    private void testIllegalParams(String indexName, String illegalClusterJsonStr, Map<String, Object> sourceSettings) {
        expectThrows(
            IllegalArgumentException.class,
            () -> restHandler.clusterJsonValidate(indexName, illegalClusterJsonStr, sourceSettings)
        );
    }
}
