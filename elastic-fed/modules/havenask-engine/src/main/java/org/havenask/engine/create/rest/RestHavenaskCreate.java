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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.havenask.action.admin.indices.create.CreateIndexRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.util.JsonPrettyFormatter;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RestHavenaskCreate extends BaseRestHandler {
    @Override
    public String getName() {
        return "havenask_create_action";
    }

    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_havenask/create/{index}"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        XContentParser parser = request.contentParser();
        Map<String, Object> source = parser.map();

        Map<String, Object> settingsMap = source.containsKey("settings")
            ? (Map<String, Object>) source.remove("settings")
            : new HashMap<>();
        Settings settings = Settings.builder().loadFromMap(settingsMap).build();

        Map<String, Object> clusters = (Map<String, Object>) source.remove("cluster");
        if (clusters != null) {
            String clustersJsonStr = JsonPrettyFormatter.toJsonString(clusters);
            clusterJsonValidate(index, clustersJsonStr, settings);
        }

        Map<String, Object> dataTables = (Map<String, Object>) source.remove("data_table");
        if (dataTables != null) {
            String dataTablesJsonStr = JsonPrettyFormatter.toJsonString(dataTables);
            dataTableJsonValidate(index, dataTablesJsonStr);
        }

        Map<String, Object> schemas = (Map<String, Object>) source.remove("schema");
        if (schemas != null) {
            String schemasJsonStr = JsonPrettyFormatter.toJsonString(schemas);
            schemaJsonValidate(index, schemasJsonStr);
        }

        if (!settingsMap.containsKey(EngineSettings.ENGINE_TYPE_SETTING.getKey())) {
            settingsMap.put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK);
        }
        if (clusters != null) {
            settingsMap.put("index.havenask.cluster_json", JsonPrettyFormatter.toJsonString(clusters));
        }
        if (dataTables != null) {
            settingsMap.put("index.havenask.data_table_json", JsonPrettyFormatter.toJsonString(dataTables));
        }
        if (schemas != null) {
            settingsMap.put("index.havenask.schema_json", JsonPrettyFormatter.toJsonString(schemas));
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settingsMap);
        if (source.containsKey("mappings")) {
            Map<String, Object> mappings = (Map<String, Object>) source.remove("mappings");
            if (mappings.containsKey("properties") && schemas != null) {
                throw new IllegalArgumentException(
                    "Configuring both 'mappings' and 'schema' simultaneously is not supported. "
                        + "Please check your configuration and ensure that only one of these settings is specified."
                );
            }
            createIndexRequest.mapping("_doc", mappings);
        }
        createIndexRequest.source(source, LoggingDeprecationHandler.INSTANCE);

        return channel -> client.admin().indices().create(createIndexRequest, new RestToXContentListener<>(channel));
    }

    protected void clusterJsonValidate(String index, String clusterJsonInput, Settings settings) {
        JSONObject clusterJsonObject = JSONObject.parseObject(clusterJsonInput);

        if (settings.hasValue(EngineSettings.ENGINE_TYPE_SETTING.getKey())) {
            String engineType = settings.get(EngineSettings.ENGINE_TYPE_SETTING.getKey());
            if (!engineType.equals(EngineSettings.ENGINE_HAVENASK)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "index.engine type must be '%s', but found '" + engineType + "'",
                        EngineSettings.ENGINE_HAVENASK
                    )
                );
            }
        }

        validateValueAtPathWithSettingsValue(
            clusterJsonObject,
            "cluster_config.builder_rule_config.partition_count",
            String.valueOf(settings.getAsInt("index.number_of_shards", 1)),
            "cluster"
        );
        validateValueAtPath(clusterJsonObject, "cluster_config.cluster_name", index, "cluster");
        validateValueAtPath(clusterJsonObject, "cluster_config.table_name", index, "cluster");
        validateValueAtPath(clusterJsonObject, "wal_config.sink.queue_name", index, "cluster");
        validateValueAtPath(clusterJsonObject, "wal_config.strategy", "queue", "cluster");
        validateValueAtPath(clusterJsonObject, "direct_write", "true", "cluster");
        if (settings.hasValue("index.havenask.hash_mode.hash_field")) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                "cluster_config.hash_mode.hash_field",
                settings.get("index.havenask.hash_mode.hash_field"),
                "cluster"
            );
        }
        validateValueAtPath(clusterJsonObject, "cluster_config.hash_mode.hash_function", "HASH", "cluster");

        if (settings.hasValue("index.havenask.build_config.max_doc_count")) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                "online_index_config.build_config.max_doc_count",
                String.valueOf(settings.getAsInt("index.havenask.build_config.max_doc_count", 10000)),
                "cluster"
            );
        }
        if (settings.hasValue("index.havenask.wal_config.sink.queue_size")) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                "wal_config.sink.queue_size",
                String.valueOf(settings.getAsInt("index.havenask.wal_config.sink.queue_size", 5000)),
                "cluster"
            );
        }
    }

    protected void schemaJsonValidate(String index, String schemaJsonInput) {
        JSONObject schemaJsonObject = JSONObject.parseObject(schemaJsonInput);

        validateValueAtPath(schemaJsonObject, "table_name", index, "schema");
    }

    protected void dataTableJsonValidate(String index, String dataTableJsonInput) {
        JSONObject dataTableJsonObject = JSONObject.parseObject(dataTableJsonInput);

        if (dataTableJsonObject.containsKey("processor_chain_config")) {
            JSONArray processorChainConfig = dataTableJsonObject.getJSONArray("processor_chain_config");
            for (int i = 0; i < processorChainConfig.size(); i++) {
                JSONObject processor = processorChainConfig.getJSONObject(i);

                if (processor.containsKey("clusters")) {
                    JSONArray clusters = processor.getJSONArray("clusters");
                    if (!clusters.contains(index)) {
                        throw new IllegalArgumentException(
                            "'"
                                + "data_table"
                                + "'"
                                + " Value:'"
                                + "processor_chain_config.clusters"
                                + "' is expected to contain '"
                                + index
                                + "'"
                        );
                    }
                }
            }
        }
    }

    private void validateValueAtPath(JSONObject jsonObject, String path, String expectedValue, String configName) {
        Object value = JSONPath.eval(jsonObject, "$." + path);

        if (value != null && !String.valueOf(value).equals(expectedValue)) {
            String errorMessage = "'"
                + configName
                + "'"
                + " Value:'"
                + path
                + "' is expected to be '"
                + expectedValue
                + "', but found '"
                + value
                + "'";
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private void validateValueAtPathWithSettingsValue(JSONObject jsonObject, String path, String settingsValue, String configName) {
        Object value = JSONPath.eval(jsonObject, "$." + path);
        if (value != null && !String.valueOf(value).equals(settingsValue)) {
            String errorMessage = "'"
                + configName
                + "'"
                + " Value:'"
                + path
                + "' is expected to be '"
                + settingsValue
                + "', but found '"
                + value
                + "'";
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
