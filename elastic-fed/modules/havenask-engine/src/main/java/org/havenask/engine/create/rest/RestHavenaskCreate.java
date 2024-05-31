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
import com.alibaba.fastjson.parser.Feature;
import org.havenask.action.admin.indices.create.CreateIndexRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.HavenaskIndexMappingProvider;
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
import java.util.Objects;

import static org.havenask.engine.index.engine.EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT;
import static org.havenask.engine.index.engine.EngineSettings.HAVENASK_CLUSTER_JSON;
import static org.havenask.engine.index.engine.EngineSettings.HAVENASK_DATA_TABLE_JSON;
import static org.havenask.engine.index.engine.EngineSettings.HAVENASK_HASH_MODE_HASH_FIELD;
import static org.havenask.engine.index.engine.EngineSettings.HAVENASK_SCHEMA_JSON;
import static org.havenask.engine.index.engine.EngineSettings.HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE;

public class RestHavenaskCreate extends BaseRestHandler {
    public static final String SETTINGS = "settings";
    public static final String MAPPINGS = "mappings";
    public static final String CLUSTER = "cluster";
    public static final String DATA_TABLE = "data_table";
    public static final String SCHEMA = "schema";
    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String BUILDER_RULE_CONFIG_PARTITION_COUNT = "cluster_config.builder_rule_config.partition_count";
    public static final String CLUSTER_CONFIG_CLUSTER_NAME = "cluster_config.cluster_name";
    public static final String CLUSTER_CONFIG_TABLE_NAME = "cluster_config.table_name";
    public static final String WAL_CONFIG_SINK_QUEUE_NAME = "wal_config.sink.queue_name";
    public static final String WAL_CONFIG_STRATEGY = "wal_config.strategy";
    public static final String DIRECT_WRITE = "direct_write";
    public static final String CLUSTER_CONFIG_HASH_MODE_HASH_FIELD = "cluster_config.hash_mode.hash_field";
    public static final String CLUSTER_CONFIG_HASH_MODE_HASH_FUNCTION = "cluster_config.hash_mode.hash_function";
    public static final String ONLINE_INDEX_CONFIG_BUILD_CONFIG_MAX_DOC_COUNT = "online_index_config.build_config.max_doc_count";
    public static final String WAL_CONFIG_SINK_QUEUE_SIZE = "wal_config.sink.queue_size";
    public static final String TABLE_NAME = "table_name";
    public static final String PROCESSOR_CHAIN_CONFIG = "processor_chain_config";
    private static final int DEFAULT_PARTITION_COUNT = 1;

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
        Map<String, Object> source = parser.mapOrdered();

        Map<String, Object> settingsMap = source.containsKey(SETTINGS) ? (Map<String, Object>) source.remove(SETTINGS) : new HashMap<>();
        Settings settings = Settings.builder().loadFromMap(settingsMap).build();

        Map<String, Object> clusters = (Map<String, Object>) source.remove(CLUSTER);
        if (clusters != null) {
            String clustersJsonStr = JsonPrettyFormatter.toJsonString(clusters);
            clusterJsonValidate(index, clustersJsonStr, settings);
            settingsMap.put(HAVENASK_CLUSTER_JSON.getKey(), JsonPrettyFormatter.toJsonString(clusters));
        }

        Map<String, Object> dataTables = (Map<String, Object>) source.remove(DATA_TABLE);
        if (dataTables != null) {
            String dataTablesJsonStr = JsonPrettyFormatter.toJsonString(dataTables);
            dataTableJsonValidate(index, dataTablesJsonStr);
            settingsMap.put(HAVENASK_DATA_TABLE_JSON.getKey(), JsonPrettyFormatter.toJsonString(dataTables));
        }

        Map<String, Object> schemas = (Map<String, Object>) source.remove(SCHEMA);
        if (schemas != null) {
            String schemasJsonStr = JsonPrettyFormatter.toJsonString(schemas);
            schemaJsonValidate(index, schemasJsonStr);
            settingsMap.put(HAVENASK_SCHEMA_JSON.getKey(), JsonPrettyFormatter.toJsonString(schemas));
        }

        if (!settingsMap.containsKey(EngineSettings.ENGINE_TYPE_SETTING.getKey())) {
            settingsMap.put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK);
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settingsMap);
        if (source.containsKey(MAPPINGS)) {
            Map<String, Object> mappings = (Map<String, Object>) source.remove(MAPPINGS);
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
        JSONObject clusterJsonObject = JSONObject.parseObject(clusterJsonInput, Feature.OrderedField);

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

        int partitionCount = DEFAULT_PARTITION_COUNT;
        if (Objects.nonNull(settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS))) {
            partitionCount = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        } else if (Objects.nonNull(settings.get(NUMBER_OF_SHARDS))) {
            partitionCount = settings.getAsInt(NUMBER_OF_SHARDS, 1);
        }
        validateValueAtPathWithSettingsValue(
            clusterJsonObject,
            BUILDER_RULE_CONFIG_PARTITION_COUNT,
            String.valueOf(partitionCount),
            "cluster"
        );
        validateValueAtPath(clusterJsonObject, CLUSTER_CONFIG_CLUSTER_NAME, index, CLUSTER);
        validateValueAtPath(clusterJsonObject, CLUSTER_CONFIG_TABLE_NAME, index, CLUSTER);
        validateValueAtPath(clusterJsonObject, WAL_CONFIG_SINK_QUEUE_NAME, index, CLUSTER);
        validateValueAtPath(clusterJsonObject, WAL_CONFIG_STRATEGY, "queue", CLUSTER);
        validateValueAtPath(clusterJsonObject, DIRECT_WRITE, "true", CLUSTER);
        if (settings.hasValue(HAVENASK_HASH_MODE_HASH_FIELD.getKey())) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                CLUSTER_CONFIG_HASH_MODE_HASH_FIELD,
                settings.get((HAVENASK_HASH_MODE_HASH_FIELD.getKey())),
                CLUSTER
            );
        }
        validateValueAtPath(clusterJsonObject, CLUSTER_CONFIG_HASH_MODE_HASH_FUNCTION, "HASH", CLUSTER);

        if (settings.hasValue(HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.getKey())) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                ONLINE_INDEX_CONFIG_BUILD_CONFIG_MAX_DOC_COUNT,
                String.valueOf(settings.getAsInt(HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.getKey(), 10000)),
                CLUSTER
            );
        }
        if (settings.hasValue(HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE.getKey())) {
            validateValueAtPathWithSettingsValue(
                clusterJsonObject,
                WAL_CONFIG_SINK_QUEUE_SIZE,
                String.valueOf(settings.getAsInt(HAVENASK_WAL_CONFIG_SINK_QUEUE_SIZE.getKey(), 5000)),
                CLUSTER
            );
        }
    }

    protected void schemaJsonValidate(String index, String schemaJsonInput) {
        JSONObject schemaJsonObject = JSONObject.parseObject(schemaJsonInput, Feature.OrderedField);

        validateValueAtPath(schemaJsonObject, TABLE_NAME, index, SCHEMA);
        validateDefaultSchemaFieldsValue(schemaJsonObject);
        validateDefaultSchemaIndexField(schemaJsonObject);
    }

    protected void dataTableJsonValidate(String index, String dataTableJsonInput) {
        JSONObject dataTableJsonObject = JSONObject.parseObject(dataTableJsonInput, Feature.OrderedField);

        if (dataTableJsonObject.containsKey(PROCESSOR_CHAIN_CONFIG)) {
            JSONArray processorChainConfig = dataTableJsonObject.getJSONArray(PROCESSOR_CHAIN_CONFIG);
            for (int i = 0; i < processorChainConfig.size(); i++) {
                JSONObject processor = processorChainConfig.getJSONObject(i);

                if (processor.containsKey(CLUSTER)) {
                    JSONArray clusters = processor.getJSONArray(CLUSTER);
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

    private void validateDefaultSchemaFieldsValue(JSONObject schemaJson) {
        JSONArray fields = schemaJson.getJSONArray("fields");
        if (fields == null) {
            return;
        }

        final String ID_FIELD = "_id";
        final String ID_TYPE = "STRING";
        final String ROUTING_FIELD = "_routing";
        final String ROUTING_TYPE = "STRING";
        final String SEQNO_FIELD = "_seq_no";
        final String SEQNO_TYPE = "INT64";
        final String SOURCE_FIELD = "_source";
        final String SOURCE_TYPE = "STRING";
        final String VERSION_FIELD = "_version";
        final String VERSION_TYPE = "INT64";
        final String PRIMARY_TERM_FIELD = "_primary_term";
        final String PRIMARY_TERM_TYPE = "INT64";

        for (int i = 0; i < fields.size(); i++) {
            JSONObject field = fields.getJSONObject(i);
            compareDefaultSchemaFieldValue(field, ID_FIELD, ID_TYPE);
            compareDefaultSchemaFieldValue(field, ROUTING_FIELD, ROUTING_TYPE);
            compareDefaultSchemaFieldValue(field, SEQNO_FIELD, SEQNO_TYPE);
            compareDefaultSchemaFieldValue(field, SOURCE_FIELD, SOURCE_TYPE);
            compareDefaultSchemaFieldValue(field, VERSION_FIELD, VERSION_TYPE);
            compareDefaultSchemaFieldValue(field, PRIMARY_TERM_FIELD, PRIMARY_TERM_TYPE);
        }
    }

    private void compareDefaultSchemaFieldValue(JSONObject field, String field_name, String field_type) {
        final String BINARY_FIELD = "binary_field";
        final String FIELD_NAME = "field_name";
        final String FIELD_TYPE = "field_type";
        if (field.containsKey(FIELD_NAME) && field.get(FIELD_NAME) instanceof String && field.getString(FIELD_NAME).equals(field_name)) {
            if (field.containsKey(BINARY_FIELD) && field.getBoolean(BINARY_FIELD) != false
                || field.containsKey(FIELD_TYPE) && !field.getString(FIELD_TYPE).equals(field_type)) {
                String errorMessage = String.format(
                    Locale.ROOT,
                    "['schema.fields.field_name':'%s'] is an internal parameter of fed, please do not configure it.",
                    field_name
                );
                throw new IllegalArgumentException(errorMessage);
            }
        }
    }

    private void validateDefaultSchemaIndexField(JSONObject schemaJson) {
        JSONArray fields = schemaJson.getJSONArray("indexs");
        if (fields == null) {
            return;
        }

        final String ID_FIELD = "_id";
        final String ROUTING_FIELD = "_routing";
        final String ROUTING_TYPE = "STRING";
        final String SEQNO_FIELD = "_seq_no";
        final String SEQNO_TYPE = "NUMBER";

        for (int i = 0; i < fields.size(); i++) {
            JSONObject field = fields.getJSONObject(i);
            compareDefaultSchemaPrimaryIndexValue(field, ID_FIELD);
            compareDefaultSchemaNormalIndexValue(field, ROUTING_FIELD, ROUTING_TYPE);
            compareDefaultSchemaNormalIndexValue(field, SEQNO_FIELD, SEQNO_TYPE);
            validateSchemaIndex(field);
        }
    }

    private void compareDefaultSchemaNormalIndexValue(JSONObject field, String field_name, String field_type) {
        final String INDEX_FIELDS = "index_fields";
        final String INDEX_NAME = "index_name";
        final String INDEX_TYPE = "index_type";
        if (field.containsKey(INDEX_NAME) && field.get(INDEX_NAME) instanceof String && field.getString(INDEX_NAME).equals(field_name)) {
            if (field.containsKey(INDEX_FIELDS)
                && field.get(INDEX_FIELDS) instanceof String
                && !field.getString(INDEX_FIELDS).equals(field_name)
                || field.containsKey(INDEX_TYPE)
                    && field.get(INDEX_TYPE) instanceof String
                    && !field.getString(INDEX_TYPE).equals(field_type)) {
                String errorMessage = String.format(
                    Locale.ROOT,
                    "['schema.indexs.index_name':'%s'] is an internal parameter of fed, please do not configure it.",
                    field_name
                );
                throw new IllegalArgumentException(errorMessage);
            }
        }
    }

    private void compareDefaultSchemaPrimaryIndexValue(JSONObject field, String field_name) {
        final String HAS_PRIMARY_KEY_ATTRIBUTE = "has_primary_key_attribute";
        final String INDEX_FIELDS = "index_fields";
        final String INDEX_NAME = "index_name";
        final String INDEX_TYPE = "index_type";
        final String IS_PRIMARY_KEY_SORTED = "is_primary_key_sorted";
        final String PRIMARY_KEY = "PRIMARYKEY64";
        if (field.containsKey(INDEX_NAME) && field.get(INDEX_NAME) instanceof String && field.getString(INDEX_NAME).equals(field_name)) {
            if (field.containsKey(INDEX_FIELDS)
                && field.get(INDEX_FIELDS) instanceof String
                && !field.getString(INDEX_FIELDS).equals(field_name)
                || field.containsKey(INDEX_TYPE)
                    && field.get(INDEX_TYPE) instanceof String
                    && !field.getString(INDEX_TYPE).equals(PRIMARY_KEY)
                || field.containsKey(IS_PRIMARY_KEY_SORTED)
                    && field.get(IS_PRIMARY_KEY_SORTED) instanceof Boolean
                    && field.getBoolean(IS_PRIMARY_KEY_SORTED) != false
                || field.containsKey(HAS_PRIMARY_KEY_ATTRIBUTE)
                    && field.get(HAS_PRIMARY_KEY_ATTRIBUTE) instanceof Boolean
                    && field.getBoolean(HAS_PRIMARY_KEY_ATTRIBUTE) != true) {
                String errorMessage = String.format(
                    Locale.ROOT,
                    "['schema.indexs.index_name':'%s'] is an internal parameter of fed, please do not configure it.",
                    field_name
                );
                throw new IllegalArgumentException(errorMessage);
            }
        }
    }

    private void validateSchemaIndex(JSONObject field) {
        final String INDEX_NAME = "index_name";
        if (field.containsKey(INDEX_NAME)
            && field.get(INDEX_NAME) instanceof String
            && field.getString(INDEX_NAME).equals(HavenaskIndexMappingProvider.ILLEGAL_HAVENASK_FIELD_NAME)) {
            String errorMessage = String.format(
                Locale.ROOT,
                "['schema.indexs.index_name':'%s'] is an unsupported index_name of fed, havenask index_name cannot be '%s'",
                HavenaskIndexMappingProvider.ILLEGAL_HAVENASK_FIELD_NAME,
                HavenaskIndexMappingProvider.ILLEGAL_HAVENASK_FIELD_NAME
            );
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
