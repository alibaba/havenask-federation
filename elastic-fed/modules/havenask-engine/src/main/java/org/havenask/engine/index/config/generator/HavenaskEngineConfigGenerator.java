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
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.ClusterJsonMinMustParams;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.util.JsonPrettyFormatter;

public class HavenaskEngineConfigGenerator {
    public static String generateClusterJsonStr(String indexName, Settings indexSettings, String clusterJson) {
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
        String clusterJsonStr = JsonPrettyFormatter.toJsonString(
            mergeClusterJson(JSONObject.parseObject(clusterJson), JSONObject.parseObject(defaultClusterJson))
        );

        return clusterJsonStr;
    }

    public static String generateSchemaJsonStr(String indexName, String schemaJson) {
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        String defaultSchemaJsonStr = schemaGenerator.defaultSchema(indexName).toString();
        String schemaJsonStr = JsonPrettyFormatter.toJsonString(
            mergeSchemaJson(JSONObject.parseObject(schemaJson), JSONObject.parseObject(defaultSchemaJsonStr))
        );
        return schemaJsonStr;
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
                } else {
                    // TODO 在REST层做校验应该更好
                    throw new RuntimeException(key + " is illegal with value:" + schemaJson.get(key));
                }
            } else {
                schemaJson.put(key, defaultSchemaJson.get(key));
            }
        }
        return schemaJson;
    }
}
