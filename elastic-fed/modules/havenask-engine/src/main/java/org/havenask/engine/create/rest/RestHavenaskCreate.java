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

        Map<String, Object> settings = source.containsKey("settings") ? (Map<String, Object>) source.remove("settings") : new HashMap<>();

        Map<String, Object> clusters = (Map<String, Object>) source.remove("cluster");
        String clustersJsonStr = JsonPrettyFormatter.toJsonString(clusters);
        clusterJsonValidate(index, clustersJsonStr, settings);

        Map<String, Object> dataTables = (Map<String, Object>) source.remove("data_table");
        String dataTablesJsonStr = JsonPrettyFormatter.toJsonString(dataTables);

        Map<String, Object> schemas = (Map<String, Object>) source.remove("schema");
        String schemasJsonStr = JsonPrettyFormatter.toJsonString(schemas);

        if (!settings.containsKey(EngineSettings.ENGINE_TYPE_SETTING.getKey())) {
            settings.put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK);
        }
        if (clusters != null) {
            settings.put("index.havenask.cluster_json", JsonPrettyFormatter.toJsonString(clusters));
        }
        if (dataTables != null) {
            settings.put("index.havenask.data_table_json", JsonPrettyFormatter.toJsonString(dataTables));
        }
        if (schemas != null) {
            settings.put("index.havenask.schema_json", JsonPrettyFormatter.toJsonString(schemas));
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settings);
        if (source.containsKey("mappings")) {
            Map<String, Object> mappings = (Map<String, Object>) source.remove("mappings");
            createIndexRequest.mapping("_doc", mappings);
        }
        createIndexRequest.source(source, LoggingDeprecationHandler.INSTANCE);

        return channel -> client.admin().indices().create(createIndexRequest, new RestToXContentListener<>(channel));
    }

    protected void clusterJsonValidate(String index, String clusterJsonInput, Map<String, Object> sourceSettings) {
        Settings settings = Settings.builder().loadFromMap(sourceSettings).build();

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
            String.valueOf(settings.getAsInt("index.number_of_shards", 1))
        );
        validateValueAtPath(clusterJsonObject, "cluster_config.cluster_name", index);
        validateValueAtPath(clusterJsonObject, "cluster_config.table_name", index);
        validateValueAtPath(clusterJsonObject, "wal_config.sink.queue_name", index);
        validateValueAtPath(clusterJsonObject, "wal_config.strategy", "queue");
        validateValueAtPath(clusterJsonObject, "direct_write", "true");
        if (settings.hasValue("index.havenask.hash_mode.hash_field")) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                "cluster_config.hash_mode.hash_field",
                settings.get("index.havenask.hash_mode.hash_field")
            );
        }
        validateValueAtPath(clusterJsonObject, "cluster_config.hash_mode.hash_function", "HASH");

        if (settings.hasValue("index.havenask.build_config.max_doc_count")) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                "online_index_config.build_config.max_doc_count",
                String.valueOf(settings.getAsInt("index.havenask.build_config.max_doc_count", 10000))
            );
        }
        if (settings.hasValue("index.havenask.wal_config.sink.queue_size")) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                "wal_config.sink.queue_size",
                String.valueOf(settings.getAsInt("index.havenask.wal_config.sink.queue_size", 5000))
            );
        }

    }

    private void validateValueAtPath(JSONObject jsonObject, String path, String expectedValue) {
        Object value = JSONPath.eval(jsonObject, "$." + path);

        if (value != null && !String.valueOf(value).equals(expectedValue)) {
            throw new IllegalArgumentException("Value '" + path + "' is expected to be '" + expectedValue + "', but found '" + value + "'");
        }
    }

    private void validateValueAtPathWithSettingsValue(JSONObject jsonObject, String path, String settingsValue) {
        Object value = JSONPath.eval(jsonObject, "$." + path);
        if (value != null && !String.valueOf(value).equals(settingsValue)) {
            throw new IllegalArgumentException(
                "Value '" + path + "' is '" + settingsValue + "' set by settings" + ", but found '" + value + "' in params json"
            );
        }
    }
}
