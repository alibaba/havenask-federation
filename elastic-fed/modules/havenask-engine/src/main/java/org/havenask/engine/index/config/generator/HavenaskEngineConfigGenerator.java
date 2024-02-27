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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.ClusterJsonMinMustParams;
import org.havenask.engine.index.config.DataTable;
import org.havenask.engine.index.config.Processor;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.util.JsonPrettyFormatter;

import java.util.List;

public class HavenaskEngineConfigGenerator {
    public static String generateClusterJsonStr(String indexName, Settings indexSettings, String inputClusterJsonStr) {
        ClusterJsonMinMustParams clusterJsonMinMustParams = new ClusterJsonMinMustParams();
        clusterJsonMinMustParams.cluster_config.builder_rule_config.partition_count = indexSettings.getAsInt(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            1
        );
        clusterJsonMinMustParams.online_index_config.build_config.max_doc_count = EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.get(
            indexSettings
        );
        clusterJsonMinMustParams.cluster_config.cluster_name = indexName;
        clusterJsonMinMustParams.cluster_config.table_name = indexName;
        clusterJsonMinMustParams.wal_config.sink.queue_name = indexName;
        clusterJsonMinMustParams.wal_config.sink.queue_size = String.valueOf(
            EngineSettings.HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE.get(indexSettings)
        );
        if (EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.exists(indexSettings)) {
            clusterJsonMinMustParams.cluster_config.hash_mode.hash_field = EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD.get(indexSettings);
        }
        String defaultClusterJson = clusterJsonMinMustParams.toString();
        String mergedClusterJsonStr = JsonPrettyFormatter.toJsonString(
            mergeClusterJson(JSONObject.parseObject(inputClusterJsonStr), JSONObject.parseObject(defaultClusterJson))
        );

        return mergedClusterJsonStr;
    }

    public static String generateSchemaJsonStr(String indexName, String inputSchemaJsonStr) {
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        String defaultSchemaJsonStr = schemaGenerator.defaultSchema(indexName).toString();
        String mergedSchemaJsonStr = JsonPrettyFormatter.toJsonString(
            mergeSchemaJson(JSONObject.parseObject(inputSchemaJsonStr), JSONObject.parseObject(defaultSchemaJsonStr))
        );
        return mergedSchemaJsonStr;
    }

    public static String generateDataTableJsonStr(String indexName, String inputDataTableJsonStr) {
        DataTable defaultDataTable = new DataTable();

        // If the user does not configure processor_chain_config, then configure a default value
        JSONObject dataTableJson = JSONObject.parseObject(inputDataTableJsonStr);
        Object value = JSONPath.eval(dataTableJson, "$." + "processor_chain_config");
        if (value == null) {
            Processor.ProcessorChainConfig processorChainConfig = new Processor.ProcessorChainConfig();
            processorChainConfig.clusters = List.of(indexName);
            defaultDataTable.processor_chain_config = List.of(processorChainConfig);
        }

        String defaultDataTableJsonStr = defaultDataTable.toString();
        String mergedDataTableJsonStr = JsonPrettyFormatter.toJsonString(
            mergeDataTableJson(dataTableJson, JSONObject.parseObject(defaultDataTableJsonStr))
        );
        return mergedDataTableJsonStr;
    }

    private static JSONObject mergeClusterJson(JSONObject clusterJson, JSONObject defaultClusterJson) {
        for (String key : defaultClusterJson.keySet()) {
            Object valueB = defaultClusterJson.get(key);
            if (clusterJson.containsKey(key)) {
                Object valueA = clusterJson.get(key);
                if (valueA instanceof JSONObject && valueB instanceof JSONObject) {
                    clusterJson.put(key, mergeClusterJson((JSONObject) valueA, (JSONObject) valueB));
                }
            } else {
                clusterJson.put(key, valueB);
            }
        }

        return clusterJson;
    }

    private static JSONObject mergeSchemaJson(JSONObject schemaJson, JSONObject defaultSchemaJson) {
        for (String key : defaultSchemaJson.keySet()) {
            if (schemaJson.containsKey(key)) {
                if (schemaJson.get(key) instanceof JSONArray && defaultSchemaJson.get(key) instanceof JSONArray) {
                    JSONArray jsonArrayA = schemaJson.getJSONArray(key);
                    JSONArray jsonArrayB = defaultSchemaJson.getJSONArray(key);
                    for (Object item : jsonArrayB) {
                        if (!jsonArrayA.contains(item)) {
                            jsonArrayA.add(item);
                        }
                    }
                } else if (schemaJson.get(key) instanceof JSONObject && defaultSchemaJson.get(key) instanceof JSONObject) {
                    schemaJson.put(key, mergeSchemaJson((JSONObject) schemaJson.get(key), (JSONObject) defaultSchemaJson.get(key)));
                } else {
                    schemaJson.put(key, defaultSchemaJson.get(key));
                }
            } else {
                schemaJson.put(key, defaultSchemaJson.get(key));
            }
        }
        return schemaJson;
    }

    private static JSONObject mergeDataTableJson(JSONObject dataTableJson, JSONObject defaultDataTableJson) {
        for (String key : defaultDataTableJson.keySet()) {
            Object valueB = defaultDataTableJson.get(key);
            if (dataTableJson.containsKey(key)) {
                Object valueA = dataTableJson.get(key);
                if (valueA instanceof JSONObject && valueB instanceof JSONObject) {
                    dataTableJson.put(key, mergeClusterJson((JSONObject) valueA, (JSONObject) valueB));
                }
            } else {
                dataTableJson.put(key, valueB);
            }
        }

        return dataTableJson;
    }
}
