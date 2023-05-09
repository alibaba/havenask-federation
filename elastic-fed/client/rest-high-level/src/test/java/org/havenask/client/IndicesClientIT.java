/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.havenask.HavenaskException;
import org.havenask.HavenaskStatusException;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.admin.indices.alias.IndicesAliasesRequest;
import org.havenask.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.havenask.action.admin.indices.alias.get.GetAliasesRequest;
import org.havenask.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.havenask.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.admin.indices.flush.FlushRequest;
import org.havenask.action.admin.indices.flush.FlushResponse;
import org.havenask.action.admin.indices.flush.SyncedFlushRequest;
import org.havenask.action.admin.indices.forcemerge.ForceMergeRequest;
import org.havenask.action.admin.indices.forcemerge.ForceMergeResponse;
import org.havenask.action.admin.indices.open.OpenIndexRequest;
import org.havenask.action.admin.indices.open.OpenIndexResponse;
import org.havenask.action.admin.indices.refresh.RefreshRequest;
import org.havenask.action.admin.indices.refresh.RefreshResponse;
import org.havenask.action.admin.indices.settings.get.GetSettingsRequest;
import org.havenask.action.admin.indices.settings.get.GetSettingsResponse;
import org.havenask.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.havenask.action.admin.indices.shrink.ResizeRequest;
import org.havenask.action.admin.indices.shrink.ResizeResponse;
import org.havenask.action.admin.indices.shrink.ResizeType;
import org.havenask.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.havenask.action.admin.indices.validate.query.ValidateQueryRequest;
import org.havenask.action.admin.indices.validate.query.ValidateQueryResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.WriteRequest;
import org.havenask.action.support.broadcast.BroadcastResponse;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.client.indices.AnalyzeRequest;
import org.havenask.client.indices.AnalyzeResponse;
import org.havenask.client.indices.CloseIndexRequest;
import org.havenask.client.indices.CloseIndexResponse;
import org.havenask.client.indices.ComposableIndexTemplateExistRequest;
import org.havenask.client.indices.CreateDataStreamRequest;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.CreateIndexResponse;
import org.havenask.client.indices.DataStream;
import org.havenask.client.indices.DataStreamsStatsRequest;
import org.havenask.client.indices.DataStreamsStatsResponse;
import org.havenask.client.indices.DataStreamsStatsResponse.DataStreamStats;
import org.havenask.client.indices.DeleteAliasRequest;
import org.havenask.client.indices.DeleteComposableIndexTemplateRequest;
import org.havenask.client.indices.DeleteDataStreamRequest;
import org.havenask.client.indices.GetComposableIndexTemplateRequest;
import org.havenask.client.indices.GetComposableIndexTemplatesResponse;
import org.havenask.client.indices.GetDataStreamRequest;
import org.havenask.client.indices.GetDataStreamResponse;
import org.havenask.client.indices.GetFieldMappingsRequest;
import org.havenask.client.indices.GetFieldMappingsResponse;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.client.indices.GetIndexResponse;
import org.havenask.client.indices.GetIndexTemplatesRequest;
import org.havenask.client.indices.GetIndexTemplatesResponse;
import org.havenask.client.indices.GetMappingsRequest;
import org.havenask.client.indices.GetMappingsResponse;
import org.havenask.client.indices.IndexTemplateMetadata;
import org.havenask.client.indices.IndexTemplatesExistRequest;
import org.havenask.client.indices.PutComposableIndexTemplateRequest;
import org.havenask.client.indices.PutIndexTemplateRequest;
import org.havenask.client.indices.PutMappingRequest;
import org.havenask.client.indices.SimulateIndexTemplateRequest;
import org.havenask.client.indices.SimulateIndexTemplateResponse;
import org.havenask.client.indices.rollover.RolloverRequest;
import org.havenask.client.indices.rollover.RolloverResponse;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.cluster.metadata.Template;
import org.havenask.common.Strings;
import org.havenask.common.ValidationException;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.common.xcontent.support.XContentMapValues;
import org.havenask.index.IndexSettings;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryBuilders;
import org.havenask.indices.flush.SyncedFlushService;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.admin.indices.RestCreateIndexAction;
import org.havenask.rest.action.admin.indices.RestGetFieldMappingAction;
import org.havenask.rest.action.admin.indices.RestGetIndexTemplateAction;
import org.havenask.rest.action.admin.indices.RestGetIndicesAction;
import org.havenask.rest.action.admin.indices.RestGetMappingAction;
import org.havenask.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.havenask.rest.action.admin.indices.RestPutMappingAction;
import org.havenask.rest.action.admin.indices.RestRolloverIndexAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.havenask.common.xcontent.support.XContentMapValues.extractRawValues;
import static org.havenask.common.xcontent.support.XContentMapValues.extractValue;

public class IndicesClientIT extends HavenaskRestHighLevelClientTestCase {

    public void testIndicesExists() throws IOException {
        // Index present
        {
            String indexName = "test_index_exists_index_present";
            createIndex(indexName, Settings.EMPTY);

            GetIndexRequest request = new GetIndexRequest(indexName);

            boolean response = execute(
                request,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync
            );
            assertTrue(response);
        }

        // Index doesn't exist
        {
            String indexName = "non_existent_index";

            GetIndexRequest request = new GetIndexRequest(indexName);

            boolean response = execute(
                request,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync
            );
            assertFalse(response);
        }

        // One index exists, one doesn't
        {
            String existingIndex = "apples";
            createIndex(existingIndex, Settings.EMPTY);

            String nonExistentIndex = "oranges";

            GetIndexRequest request = new GetIndexRequest(existingIndex, nonExistentIndex);

            boolean response = execute(
                request,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync
            );
            assertFalse(response);
        }
    }

    public void testIndicesExistsWithTypes() throws IOException {
        // Index present
        String indexName = "test_index_exists_index_present";
        createIndex(indexName, Settings.EMPTY);

        org.havenask.action.admin.indices.get.GetIndexRequest request
            = new org.havenask.action.admin.indices.get.GetIndexRequest();
        request.indices(indexName);

        boolean response = execute(request, highLevelClient().indices()::exists, highLevelClient().indices()::existsAsync);
        assertTrue(response);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCreateIndex() throws IOException {
        {
            // Create index
            String indexName = "plain_index";
            assertFalse(indexExists(indexName));

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            CreateIndexResponse createIndexResponse =
                    execute(createIndexRequest, highLevelClient().indices()::create, highLevelClient().indices()::createAsync);
            assertTrue(createIndexResponse.isAcknowledged());

            assertTrue(indexExists(indexName));
        }
        {
            // Create index with mappings, aliases and settings
            String indexName = "rich_index";
            assertFalse(indexExists(indexName));

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            Alias alias = new Alias("alias_name");
            alias.filter("{\"term\":{\"year\":2016}}");
            alias.routing("1");
            createIndexRequest.alias(alias);

            Settings.Builder settings = Settings.builder();
            settings.put(SETTING_NUMBER_OF_REPLICAS, 2);
            createIndexRequest.settings(settings);

            XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
            mappingBuilder.startObject().startObject("properties").startObject("field");
            mappingBuilder.field("type", "text");
            mappingBuilder.endObject().endObject().endObject();
            createIndexRequest.mapping(mappingBuilder);

            CreateIndexResponse createIndexResponse =
                    execute(createIndexRequest, highLevelClient().indices()::create, highLevelClient().indices()::createAsync);
            assertTrue(createIndexResponse.isAcknowledged());

            Map<String, Object> getIndexResponse = getAsMap(indexName);
            assertEquals("2", XContentMapValues.extractValue(indexName + ".settings.index.number_of_replicas", getIndexResponse));

            Map<String, Object> aliasData =
                    (Map<String, Object>)XContentMapValues.extractValue(indexName + ".aliases.alias_name", getIndexResponse);
            assertNotNull(aliasData);
            assertEquals("1", aliasData.get("index_routing"));
            Map<String, Object> filter = (Map) aliasData.get("filter");
            Map<String, Object> term = (Map) filter.get("term");
            assertEquals(2016, term.get("year"));

            assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.properties.field.type", getIndexResponse));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCreateIndexWithTypes() throws IOException {
        {
            // Create index
            String indexName = "plain_index";
            assertFalse(indexExists(indexName));

            org.havenask.action.admin.indices.create.CreateIndexRequest createIndexRequest =
                new org.havenask.action.admin.indices.create.CreateIndexRequest(indexName);

            org.havenask.action.admin.indices.create.CreateIndexResponse createIndexResponse = execute(
                createIndexRequest,
                highLevelClient().indices()::create,
                highLevelClient().indices()::createAsync,
                expectWarnings(RestCreateIndexAction.TYPES_DEPRECATION_MESSAGE));
            assertTrue(createIndexResponse.isAcknowledged());

            assertTrue(indexExists(indexName));
        }
        {
            // Create index with mappings, aliases and settings
            String indexName = "rich_index";
            assertFalse(indexExists(indexName));

            org.havenask.action.admin.indices.create.CreateIndexRequest createIndexRequest =
                new org.havenask.action.admin.indices.create.CreateIndexRequest(indexName);

            Alias alias = new Alias("alias_name");
            alias.filter("{\"term\":{\"year\":2016}}");
            alias.routing("1");
            createIndexRequest.alias(alias);

            Settings.Builder settings = Settings.builder();
            settings.put(SETTING_NUMBER_OF_REPLICAS, 2);
            createIndexRequest.settings(settings);

            XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
            mappingBuilder.startObject().startObject("properties").startObject("field");
            mappingBuilder.field("type", "text");
            mappingBuilder.endObject().endObject().endObject();
            createIndexRequest.mapping(MapperService.SINGLE_MAPPING_NAME, mappingBuilder);

            org.havenask.action.admin.indices.create.CreateIndexResponse createIndexResponse = execute(
                createIndexRequest,
                highLevelClient().indices()::create,
                highLevelClient().indices()::createAsync,
                expectWarnings(RestCreateIndexAction.TYPES_DEPRECATION_MESSAGE));
            assertTrue(createIndexResponse.isAcknowledged());

            Map<String, Object> getIndexResponse = getAsMap(indexName);
            assertEquals("2", XContentMapValues.extractValue(indexName + ".settings.index.number_of_replicas", getIndexResponse));

            Map<String, Object> aliasData =
                (Map<String, Object>)XContentMapValues.extractValue(indexName + ".aliases.alias_name", getIndexResponse);
            assertNotNull(aliasData);
            assertEquals("1", aliasData.get("index_routing"));
            Map<String, Object> filter = (Map) aliasData.get("filter");
            Map<String, Object> term = (Map) filter.get("term");
            assertEquals(2016, term.get("year"));

            assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.properties.field.type", getIndexResponse));
        }
    }

    public void testGetSettings() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indexName);
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNull(getSettingsResponse.getSetting(indexName, "index.refresh_interval"));
        assertEquals("1", getSettingsResponse.getSetting(indexName, "index.number_of_shards"));

        updateIndexSettings(indexName, Settings.builder().put("refresh_interval", "30s"));

        GetSettingsResponse updatedResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);
        assertEquals("30s", updatedResponse.getSetting(indexName, "index.refresh_interval"));
    }

    public void testGetSettingsNonExistentIndex() throws IOException {
        String nonExistentIndex = "index_that_doesnt_exist";
        assertFalse(indexExists(nonExistentIndex));

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(nonExistentIndex);
        HavenaskException exception = expectThrows(HavenaskException.class,
            () -> execute(getSettingsRequest, highLevelClient().indices()::getSettings, highLevelClient().indices()::getSettingsAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testGetSettingsFromMultipleIndices() throws IOException {
        String indexName1 = "get_multiple_settings_one";
        createIndex(indexName1, Settings.builder().put("number_of_shards", 2).build());

        String indexName2 = "get_multiple_settings_two";
        createIndex(indexName2, Settings.builder().put("number_of_shards", 3).build());

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("get_multiple_settings*");
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertEquals("2", getSettingsResponse.getSetting(indexName1, "index.number_of_shards"));
        assertEquals("3", getSettingsResponse.getSetting(indexName2, "index.number_of_shards"));
    }

    public void testGetSettingsFiltered() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indexName).names("index.number_of_shards");
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNull(getSettingsResponse.getSetting(indexName, "index.number_of_replicas"));
        assertEquals("1", getSettingsResponse.getSetting(indexName, "index.number_of_shards"));
        assertEquals(1, getSettingsResponse.getIndexToSettings().get("get_settings_index").size());
    }

    public void testGetSettingsWithDefaults() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indexName).includeDefaults(true);
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNotNull(getSettingsResponse.getSetting(indexName, "index.refresh_interval"));
        assertEquals(IndexSettings.DEFAULT_REFRESH_INTERVAL,
            getSettingsResponse.getIndexToDefaultSettings().get("get_settings_index").getAsTime("index.refresh_interval", null));
        assertEquals("1", getSettingsResponse.getSetting(indexName, "index.number_of_shards"));
    }

    public void testGetSettingsWithDefaultsFiltered() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
            .indices(indexName)
            .names("index.refresh_interval")
            .includeDefaults(true);
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNull(getSettingsResponse.getSetting(indexName, "index.number_of_replicas"));
        assertNull(getSettingsResponse.getSetting(indexName, "index.number_of_shards"));
        assertEquals(0, getSettingsResponse.getIndexToSettings().get("get_settings_index").size());
        assertEquals(1, getSettingsResponse.getIndexToDefaultSettings().get("get_settings_index").size());
    }

    @SuppressWarnings("unchecked")
    public void testGetIndex() throws IOException {
        String indexName = "get_index_test";
        Settings basicSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        String mappings = "\"properties\":{\"field-1\":{\"type\":\"integer\"}}";
        createIndex(indexName, basicSettings, mappings);

        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName).includeDefaults(false);
        GetIndexResponse getIndexResponse =
            execute(getIndexRequest, highLevelClient().indices()::get, highLevelClient().indices()::getAsync);

        // default settings should be null
        assertNull(getIndexResponse.getSetting(indexName, "index.refresh_interval"));
        assertEquals("1", getIndexResponse.getSetting(indexName, SETTING_NUMBER_OF_SHARDS));
        assertEquals("0", getIndexResponse.getSetting(indexName, SETTING_NUMBER_OF_REPLICAS));
        assertNotNull(getIndexResponse.getMappings().get(indexName));
        assertNotNull(getIndexResponse.getMappings().get(indexName));
        MappingMetadata mappingMetadata = getIndexResponse.getMappings().get(indexName);
        assertNotNull(mappingMetadata);
        assertEquals("_doc", mappingMetadata.type());
        assertEquals("{\"properties\":{\"field-1\":{\"type\":\"integer\"}}}", mappingMetadata.source().string());
        Object o = mappingMetadata.getSourceAsMap().get("properties");
        assertThat(o, instanceOf(Map.class));
        //noinspection unchecked
        assertThat(((Map<String, Object>) o).get("field-1"), instanceOf(Map.class));
        //noinspection unchecked
        Map<String, Object> fieldMapping = (Map<String, Object>) ((Map<String, Object>) o).get("field-1");
        assertEquals("integer", fieldMapping.get("type"));
    }

    @SuppressWarnings("unchecked")
    public void testGetIndexWithTypes() throws IOException {
        String indexName = "get_index_test";
        Settings basicSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        String mappings = "\"properties\":{\"field-1\":{\"type\":\"integer\"}}";
        createIndex(indexName, basicSettings, mappings);

        org.havenask.action.admin.indices.get.GetIndexRequest getIndexRequest =
                new org.havenask.action.admin.indices.get.GetIndexRequest().indices(indexName).includeDefaults(false);
        org.havenask.action.admin.indices.get.GetIndexResponse getIndexResponse = execute(getIndexRequest,
                highLevelClient().indices()::get, highLevelClient().indices()::getAsync,
                expectWarnings(RestGetIndicesAction.TYPES_DEPRECATION_MESSAGE));

        // default settings should be null
        assertNull(getIndexResponse.getSetting(indexName, "index.refresh_interval"));
        assertEquals("1", getIndexResponse.getSetting(indexName, SETTING_NUMBER_OF_SHARDS));
        assertEquals("0", getIndexResponse.getSetting(indexName, SETTING_NUMBER_OF_REPLICAS));
        assertNotNull(getIndexResponse.getMappings().get(indexName));
        MappingMetadata mappingMetadata = getIndexResponse.getMappings().get(indexName).get("_doc");
        assertNotNull(mappingMetadata);
        assertEquals("_doc", mappingMetadata.type());
        assertEquals("{\"properties\":{\"field-1\":{\"type\":\"integer\"}}}", mappingMetadata.source().string());
    }

    @SuppressWarnings("unchecked")
    public void testGetIndexWithDefaults() throws IOException {
        String indexName = "get_index_test";
        Settings basicSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        String mappings = "\"properties\":{\"field-1\":{\"type\":\"integer\"}}";
        createIndex(indexName, basicSettings, mappings);

        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName).includeDefaults(true);
        GetIndexResponse getIndexResponse =
            execute(getIndexRequest, highLevelClient().indices()::get, highLevelClient().indices()::getAsync);

        assertNotNull(getIndexResponse.getSetting(indexName, "index.refresh_interval"));
        assertEquals(IndexSettings.DEFAULT_REFRESH_INTERVAL,
            getIndexResponse.getDefaultSettings().get(indexName).getAsTime("index.refresh_interval", null));
        assertEquals("1", getIndexResponse.getSetting(indexName, SETTING_NUMBER_OF_SHARDS));
        assertEquals("0", getIndexResponse.getSetting(indexName, SETTING_NUMBER_OF_REPLICAS));
        assertNotNull(getIndexResponse.getMappings().get(indexName));
        assertNotNull(getIndexResponse.getMappings().get(indexName));
        Object o = getIndexResponse.getMappings().get(indexName).getSourceAsMap().get("properties");
        assertThat(o, instanceOf(Map.class));
        assertThat(((Map<String, Object>) o).get("field-1"), instanceOf(Map.class));
        Map<String, Object> fieldMapping = (Map<String, Object>) ((Map<String, Object>) o).get("field-1");
        assertEquals("integer", fieldMapping.get("type"));
    }

    public void testGetIndexNonExistentIndex() throws IOException {
        String nonExistentIndex = "index_that_doesnt_exist";
        assertFalse(indexExists(nonExistentIndex));

        GetIndexRequest getIndexRequest = new GetIndexRequest(nonExistentIndex);
        HavenaskException exception = expectThrows(HavenaskException.class,
            () -> execute(getIndexRequest, highLevelClient().indices()::get, highLevelClient().indices()::getAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testPutMapping() throws IOException {
        String indexName = "mapping_index";
        createIndex(indexName, Settings.EMPTY);

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject().startObject("properties").startObject("field");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject().endObject().endObject();
        putMappingRequest.source(mappingBuilder);

        AcknowledgedResponse putMappingResponse = execute(putMappingRequest,
            highLevelClient().indices()::putMapping,
            highLevelClient().indices()::putMappingAsync);
        assertTrue(putMappingResponse.isAcknowledged());

        Map<String, Object> getIndexResponse = getAsMap(indexName);
        assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.properties.field.type",
            getIndexResponse));
    }

    public void testPutMappingWithTypes() throws IOException {
        String indexName = "mapping_index";
        createIndex(indexName, Settings.EMPTY);

        org.havenask.action.admin.indices.mapping.put.PutMappingRequest putMappingRequest =
            new org.havenask.action.admin.indices.mapping.put.PutMappingRequest(indexName);
        putMappingRequest.type("some_type");

        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject().startObject("properties").startObject("field");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject().endObject().endObject();
        putMappingRequest.source(mappingBuilder);

        AcknowledgedResponse putMappingResponse = execute(putMappingRequest,
            highLevelClient().indices()::putMapping,
            highLevelClient().indices()::putMappingAsync,
            expectWarnings(RestPutMappingAction.TYPES_DEPRECATION_MESSAGE));
        assertTrue(putMappingResponse.isAcknowledged());

        Map<String, Object> getIndexResponse = getAsMap(indexName);
        assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.properties.field.type",
            getIndexResponse));
    }

    public void testGetMapping() throws IOException {
        String indexName = "test";
        createIndex(indexName, Settings.EMPTY);

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject().startObject("properties").startObject("field");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject().endObject().endObject();
        putMappingRequest.source(mappingBuilder);

        AcknowledgedResponse putMappingResponse = execute(putMappingRequest,
            highLevelClient().indices()::putMapping,
            highLevelClient().indices()::putMappingAsync);
        assertTrue(putMappingResponse.isAcknowledged());

        Map<String, Object> getIndexResponse = getAsMap(indexName);
        assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.properties.field.type", getIndexResponse));

        GetMappingsRequest request = new GetMappingsRequest().indices(indexName);

        GetMappingsResponse getMappingsResponse = execute(
            request,
            highLevelClient().indices()::getMapping,
            highLevelClient().indices()::getMappingAsync);

        Map<String, Object> mappings = getMappingsResponse.mappings().get(indexName).sourceAsMap();
        Map<String, String> type = new HashMap<>();
        type.put("type", "text");
        Map<String, Object> field = new HashMap<>();
        field.put("field", type);
        Map<String, Object> expected = new HashMap<>();
        expected.put("properties", field);
        assertThat(mappings, equalTo(expected));
    }

    public void testGetMappingWithTypes() throws IOException {
        String indexName = "test";
        createIndex(indexName, Settings.EMPTY);

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject().startObject("properties").startObject("field");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject().endObject().endObject();
        putMappingRequest.source(mappingBuilder);

        AcknowledgedResponse putMappingResponse = execute(putMappingRequest,
            highLevelClient().indices()::putMapping,
            highLevelClient().indices()::putMappingAsync);
        assertTrue(putMappingResponse.isAcknowledged());

        Map<String, Object> getIndexResponse = getAsMap(indexName);
        assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.properties.field.type", getIndexResponse));

        org.havenask.action.admin.indices.mapping.get.GetMappingsRequest request =
            new org.havenask.action.admin.indices.mapping.get.GetMappingsRequest().indices(indexName);

        org.havenask.action.admin.indices.mapping.get.GetMappingsResponse getMappingsResponse = execute(
            request,
            highLevelClient().indices()::getMapping,
            highLevelClient().indices()::getMappingAsync,
            expectWarnings(RestGetMappingAction.TYPES_DEPRECATION_MESSAGE));

        Map<String, Object> mappings = getMappingsResponse.getMappings().get(indexName).get("_doc").sourceAsMap();
        Map<String, String> type = new HashMap<>();
        type.put("type", "text");
        Map<String, Object> field = new HashMap<>();
        field.put("field", type);
        Map<String, Object> expected = new HashMap<>();
        expected.put("properties", field);
        assertThat(mappings, equalTo(expected));
    }

    public void testGetFieldMapping() throws IOException {
        String indexName = "test";
        createIndex(indexName, Settings.EMPTY);

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject().startObject("properties").startObject("field");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject().endObject().endObject();
        putMappingRequest.source(mappingBuilder);

        AcknowledgedResponse putMappingResponse =
            execute(putMappingRequest, highLevelClient().indices()::putMapping, highLevelClient().indices()::putMappingAsync);
        assertTrue(putMappingResponse.isAcknowledged());

        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest()
            .indices(indexName)
            .fields("field");

        GetFieldMappingsResponse getFieldMappingsResponse =
            execute(getFieldMappingsRequest,
                highLevelClient().indices()::getFieldMapping,
                highLevelClient().indices()::getFieldMappingAsync);

        final Map<String, GetFieldMappingsResponse.FieldMappingMetadata> fieldMappingMap =
            getFieldMappingsResponse.mappings().get(indexName);

        final GetFieldMappingsResponse.FieldMappingMetadata metadata =
            new GetFieldMappingsResponse.FieldMappingMetadata("field",
                new BytesArray("{\"field\":{\"type\":\"text\"}}"));
        assertThat(fieldMappingMap, equalTo(Collections.singletonMap("field", metadata)));
    }

    public void testGetFieldMappingWithTypes() throws IOException {
        String indexName = "test";
        createIndex(indexName, Settings.EMPTY);

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject().startObject("properties").startObject("field");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject().endObject().endObject();
        putMappingRequest.source(mappingBuilder);

        AcknowledgedResponse putMappingResponse =
            execute(putMappingRequest, highLevelClient().indices()::putMapping, highLevelClient().indices()::putMappingAsync);
        assertTrue(putMappingResponse.isAcknowledged());

        org.havenask.action.admin.indices.mapping.get.GetFieldMappingsRequest getFieldMappingsRequest =
            new org.havenask.action.admin.indices.mapping.get.GetFieldMappingsRequest()
            .indices(indexName)
            .types("_doc")
            .fields("field");

        org.havenask.action.admin.indices.mapping.get.GetFieldMappingsResponse getFieldMappingsResponse =
            execute(getFieldMappingsRequest,
                highLevelClient().indices()::getFieldMapping,
                highLevelClient().indices()::getFieldMappingAsync,
                expectWarnings(RestGetFieldMappingAction.TYPES_DEPRECATION_MESSAGE));

        final Map<String, org.havenask.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata>
            fieldMappingMap = getFieldMappingsResponse.mappings().get(indexName).get("_doc");

        final  org.havenask.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata metadata =
            new  org.havenask.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata("field",
                new BytesArray("{\"field\":{\"type\":\"text\"}}"));
        assertThat(fieldMappingMap, equalTo(Collections.singletonMap("field", metadata)));
    }

    public void testDeleteIndex() throws IOException {
        {
            // Delete index if exists
            String indexName = "test_index";
            createIndex(indexName, Settings.EMPTY);

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            AcknowledgedResponse deleteIndexResponse =
                    execute(deleteIndexRequest, highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync);
            assertTrue(deleteIndexResponse.isAcknowledged());

            assertFalse(indexExists(indexName));
        }
        {
            // Return 404 if index doesn't exist
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(nonExistentIndex);

            HavenaskException exception = expectThrows(HavenaskException.class,
                    () -> execute(deleteIndexRequest, highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    @SuppressWarnings("unchecked")
    public void testUpdateAliases() throws IOException {
        String index = "index";
        String alias = "alias";

        createIndex(index, Settings.EMPTY);
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(alias), equalTo(false));

        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index(index).aliases(alias);
        if (randomBoolean()) {
            addAction.writeIndex(randomBoolean());
        }
        addAction.routing("routing").searchRouting("search_routing").filter("{\"term\":{\"year\":2016}}");
        aliasesAddRequest.addAliasAction(addAction);
        AcknowledgedResponse aliasesAddResponse = execute(aliasesAddRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync);
        assertTrue(aliasesAddResponse.isAcknowledged());
        assertThat(aliasExists(alias), equalTo(true));
        assertThat(aliasExists(index, alias), equalTo(true));
        Map<String, Object> getAlias = getAlias(index, alias);
        assertThat(getAlias.get("index_routing"), equalTo("routing"));
        assertThat(getAlias.get("search_routing"), equalTo("search_routing"));
        Map<String, Object> filter = (Map<String, Object>) getAlias.get("filter");
        Map<String, Object> term = (Map<String, Object>) filter.get("term");
        assertEquals(2016, term.get("year"));
        Boolean isWriteIndex = (Boolean) getAlias.get("is_write_index");
        assertThat(isWriteIndex, equalTo(addAction.writeIndex()));

        String alias2 = "alias2";
        IndicesAliasesRequest aliasesAddRemoveRequest = new IndicesAliasesRequest();
        addAction = new AliasActions(AliasActions.Type.ADD).indices(index).alias(alias2);
        aliasesAddRemoveRequest.addAliasAction(addAction);
        AliasActions removeAction = new AliasActions(AliasActions.Type.REMOVE).index(index).alias(alias);
        aliasesAddRemoveRequest.addAliasAction(removeAction);
        AcknowledgedResponse aliasesAddRemoveResponse = execute(aliasesAddRemoveRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync);
        assertTrue(aliasesAddRemoveResponse.isAcknowledged());
        assertThat(aliasExists(alias), equalTo(false));
        assertThat(aliasExists(alias2), equalTo(true));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(index, alias2), equalTo(true));

        IndicesAliasesRequest aliasesRemoveIndexRequest = new IndicesAliasesRequest();
        AliasActions removeIndexAction = new AliasActions(AliasActions.Type.REMOVE_INDEX).index(index);
        aliasesRemoveIndexRequest.addAliasAction(removeIndexAction);
        AcknowledgedResponse aliasesRemoveIndexResponse = execute(aliasesRemoveIndexRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync);
        assertTrue(aliasesRemoveIndexResponse.isAcknowledged());
        assertThat(aliasExists(alias), equalTo(false));
        assertThat(aliasExists(alias2), equalTo(false));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(index, alias2), equalTo(false));
        assertThat(indexExists(index), equalTo(false));
    }

    public void testAliasesNonExistentIndex() throws IOException {
        String index = "index";
        String alias = "alias";
        String nonExistentIndex = "non_existent_index";

        IndicesAliasesRequest nonExistentIndexRequest = new IndicesAliasesRequest();
        nonExistentIndexRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index(nonExistentIndex).alias(alias));
        HavenaskException exception = expectThrows(HavenaskException.class, () -> execute(nonExistentIndexRequest,
                highLevelClient().indices()::updateAliases, highLevelClient().indices()::updateAliasesAsync));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(),
            equalTo("Havenask exception [type=index_not_found_exception, reason=no such index [non_existent_index]]"));
        assertThat(exception.getMetadata("havenask.index"), hasItem(nonExistentIndex));

        createIndex(index, Settings.EMPTY);
        IndicesAliasesRequest mixedRequest = new IndicesAliasesRequest();
        mixedRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).indices(index).aliases(alias));
        mixedRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).indices(nonExistentIndex).alias(alias));
        exception = expectThrows(HavenaskStatusException.class,
                () -> execute(mixedRequest, highLevelClient().indices()::updateAliases, highLevelClient().indices()::updateAliasesAsync));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(),
            equalTo("Havenask exception [type=index_not_found_exception, reason=no such index [non_existent_index]]"));
        assertThat(exception.getMetadata("havenask.index"), hasItem(nonExistentIndex));
        assertThat(exception.getMetadata("havenask.index"), not(hasItem(index)));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(alias), equalTo(false));

        IndicesAliasesRequest removeIndexRequest = new IndicesAliasesRequest();
        removeIndexRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index(nonExistentIndex).alias(alias));
        removeIndexRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE_INDEX).indices(nonExistentIndex));
        exception = expectThrows(HavenaskException.class, () -> execute(removeIndexRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(),
            equalTo("Havenask exception [type=index_not_found_exception, reason=no such index [non_existent_index]]"));
        assertThat(exception.getMetadata("havenask.index"), hasItem(nonExistentIndex));
        assertThat(exception.getMetadata("havenask.index"), not(hasItem(index)));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(alias), equalTo(false));
    }

    public void testOpenExistingIndex() throws IOException {
        String index = "index";
        createIndex(index, Settings.EMPTY);
        closeIndex(index);
        ResponseException exception = expectThrows(ResponseException.class,
                () -> client().performRequest(new Request(HttpGet.METHOD_NAME, index + "/_search")));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
        assertThat(exception.getMessage().contains(index), equalTo(true));

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(index);
        OpenIndexResponse openIndexResponse = execute(openIndexRequest, highLevelClient().indices()::open,
                highLevelClient().indices()::openAsync);
        assertTrue(openIndexResponse.isAcknowledged());

        Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, index + "/_search"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    public void testOpenNonExistentIndex() throws IOException {
        String nonExistentIndex = "non_existent_index";
        assertFalse(indexExists(nonExistentIndex));

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(nonExistentIndex);
        HavenaskException exception = expectThrows(HavenaskException.class,
                () -> execute(openIndexRequest, highLevelClient().indices()::open, highLevelClient().indices()::openAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());

        OpenIndexRequest lenientOpenIndexRequest = new OpenIndexRequest(nonExistentIndex);
        lenientOpenIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        OpenIndexResponse lenientOpenIndexResponse = execute(lenientOpenIndexRequest, highLevelClient().indices()::open,
                highLevelClient().indices()::openAsync);
        assertThat(lenientOpenIndexResponse.isAcknowledged(), equalTo(true));

        OpenIndexRequest strictOpenIndexRequest = new OpenIndexRequest(nonExistentIndex);
        strictOpenIndexRequest.indicesOptions(IndicesOptions.strictExpandOpen());
        HavenaskException strictException = expectThrows(HavenaskException.class,
                () -> execute(openIndexRequest, highLevelClient().indices()::open, highLevelClient().indices()::openAsync));
        assertEquals(RestStatus.NOT_FOUND, strictException.status());
    }

    public void testCloseExistingIndex() throws IOException {
        final String[] indices = new String[randomIntBetween(1, 5)];
        for (int i = 0; i < indices.length; i++) {
            String index = "index-" + i;
            createIndex(index, Settings.EMPTY);
            indices[i] = index;
        }

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indices);
        CloseIndexResponse closeIndexResponse = execute(closeIndexRequest,
            highLevelClient().indices()::close, highLevelClient().indices()::closeAsync);
        assertTrue(closeIndexResponse.isAcknowledged());
        assertTrue(closeIndexResponse.isShardsAcknowledged());
        assertThat(closeIndexResponse.getIndices(), notNullValue());
        assertThat(closeIndexResponse.getIndices(), hasSize(indices.length));
        closeIndexResponse.getIndices().forEach(indexResult -> {
            assertThat(indexResult.getIndex(), startsWith("index-"));
            assertThat(indexResult.hasFailures(), is(false));

            ResponseException exception = expectThrows(ResponseException.class,
                () -> client().performRequest(new Request(HttpGet.METHOD_NAME, indexResult.getIndex() + "/_search")));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(exception.getMessage().contains(indexResult.getIndex()), equalTo(true));
        });
    }

    public void testCloseNonExistentIndex() throws IOException {
        String nonExistentIndex = "non_existent_index";
        assertFalse(indexExists(nonExistentIndex));

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(nonExistentIndex);
        HavenaskException exception = expectThrows(HavenaskException.class,
                () -> execute(closeIndexRequest, highLevelClient().indices()::close, highLevelClient().indices()::closeAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testCloseEmptyOrNullIndex() {
        String[] indices = randomBoolean() ? Strings.EMPTY_ARRAY : null;
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indices);
        org.havenask.client.ValidationException exception = expectThrows(org.havenask.client.ValidationException.class,
            () -> execute(closeIndexRequest, highLevelClient().indices()::close, highLevelClient().indices()::closeAsync));
        assertThat(exception.validationErrors().get(0), equalTo("index is missing"));
    }

    public void testRefresh() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(index, settings);
            RefreshRequest refreshRequest = new RefreshRequest(index);
            RefreshResponse refreshResponse =
                execute(refreshRequest, highLevelClient().indices()::refresh, highLevelClient().indices()::refreshAsync);
            assertThat(refreshResponse.getTotalShards(), equalTo(1));
            assertThat(refreshResponse.getSuccessfulShards(), equalTo(1));
            assertThat(refreshResponse.getFailedShards(), equalTo(0));
            assertThat(refreshResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            RefreshRequest refreshRequest = new RefreshRequest(nonExistentIndex);
            HavenaskException exception = expectThrows(HavenaskException.class,
                () -> execute(refreshRequest, highLevelClient().indices()::refresh, highLevelClient().indices()::refreshAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testFlush() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", 0)
                    .build();
            createIndex(index, settings);
            FlushRequest flushRequest = new FlushRequest(index);
            FlushResponse flushResponse =
                    execute(flushRequest, highLevelClient().indices()::flush, highLevelClient().indices()::flushAsync);
            assertThat(flushResponse.getTotalShards(), equalTo(1));
            assertThat(flushResponse.getSuccessfulShards(), equalTo(1));
            assertThat(flushResponse.getFailedShards(), equalTo(0));
            assertThat(flushResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            FlushRequest flushRequest = new FlushRequest(nonExistentIndex);
            HavenaskException exception = expectThrows(HavenaskException.class,
                    () -> execute(flushRequest, highLevelClient().indices()::flush, highLevelClient().indices()::flushAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testSyncedFlush() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", 0)
                    .build();
            createIndex(index, settings);
            SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(index);
            SyncedFlushResponse flushResponse =
                    execute(syncedFlushRequest, highLevelClient().indices()::flushSynced, highLevelClient().indices()::flushSyncedAsync,
                        expectWarnings(SyncedFlushService.SYNCED_FLUSH_DEPRECATION_MESSAGE));
            assertThat(flushResponse.totalShards(), equalTo(1));
            assertThat(flushResponse.successfulShards(), equalTo(1));
            assertThat(flushResponse.failedShards(), equalTo(0));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(nonExistentIndex);
            HavenaskException exception = expectThrows(
                HavenaskException.class,
                () ->
                    execute(
                        syncedFlushRequest,
                        highLevelClient().indices()::flushSynced,
                        highLevelClient().indices()::flushSyncedAsync,
                        expectWarnings(SyncedFlushService.SYNCED_FLUSH_DEPRECATION_MESSAGE)
                    )
            );
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }


    public void testClearCache() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", 0)
                    .build();
            createIndex(index, settings);
            ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest(index);
            ClearIndicesCacheResponse clearCacheResponse =
                    execute(clearCacheRequest, highLevelClient().indices()::clearCache, highLevelClient().indices()::clearCacheAsync);
            assertThat(clearCacheResponse.getTotalShards(), equalTo(1));
            assertThat(clearCacheResponse.getSuccessfulShards(), equalTo(1));
            assertThat(clearCacheResponse.getFailedShards(), equalTo(0));
            assertThat(clearCacheResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest(nonExistentIndex);
            HavenaskException exception = expectThrows(HavenaskException.class,
                    () -> execute(clearCacheRequest, highLevelClient().indices()::clearCache,
                            highLevelClient().indices()::clearCacheAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testForceMerge() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(index, settings);
            ForceMergeRequest forceMergeRequest = new ForceMergeRequest(index);
            ForceMergeResponse forceMergeResponse =
                execute(forceMergeRequest, highLevelClient().indices()::forcemerge, highLevelClient().indices()::forcemergeAsync);
            assertThat(forceMergeResponse.getTotalShards(), equalTo(1));
            assertThat(forceMergeResponse.getSuccessfulShards(), equalTo(1));
            assertThat(forceMergeResponse.getFailedShards(), equalTo(0));
            assertThat(forceMergeResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));

            assertThat(forceMergeRequest.getDescription(), containsString(index));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            ForceMergeRequest forceMergeRequest = new ForceMergeRequest(nonExistentIndex);
            HavenaskException exception = expectThrows(HavenaskException.class,
                () -> execute(forceMergeRequest, highLevelClient().indices()::forcemerge, highLevelClient().indices()::forcemergeAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());

            assertThat(forceMergeRequest.getDescription(), containsString(nonExistentIndex));
        }
    }

    public void testExistsAlias() throws IOException {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest("alias");
        assertFalse(execute(getAliasesRequest, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));

        createIndex("index", Settings.EMPTY);
        client().performRequest(new Request(HttpPut.METHOD_NAME, "/index/_alias/alias"));
        assertTrue(execute(getAliasesRequest, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));

        GetAliasesRequest getAliasesRequest2 = new GetAliasesRequest();
        getAliasesRequest2.aliases("alias");
        getAliasesRequest2.indices("index");
        assertTrue(execute(getAliasesRequest2, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));
        getAliasesRequest2.indices("does_not_exist");
        assertFalse(execute(getAliasesRequest2, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));
    }

    @SuppressWarnings("unchecked")
    public void testShrink() throws IOException {
        Map<String, Object> nodes = getAsMap("_nodes");
        String firstNode = ((Map<String, Object>) nodes.get("nodes")).keySet().iterator().next();
        createIndex("source", Settings.builder().put("index.number_of_shards", 4).put("index.number_of_replicas", 0).build());
        updateIndexSettings("source", Settings.builder().put("index.routing.allocation.require._name", firstNode)
                .put("index.blocks.write", true));

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SHRINK);
        Settings targetSettings =
                Settings.builder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .putNull("index.routing.allocation.require._name")
                        .build();
        resizeRequest.setTargetIndex(new org.havenask.action.admin.indices.create.CreateIndexRequest("target")
            .settings(targetSettings)
            .alias(new Alias("alias")));
        ResizeResponse resizeResponse = execute(resizeRequest, highLevelClient().indices()::shrink,
                highLevelClient().indices()::shrinkAsync);
        assertTrue(resizeResponse.isAcknowledged());
        assertTrue(resizeResponse.isShardsAcknowledged());
        Map<String, Object> getIndexResponse = getAsMap("target");
        Map<String, Object> indexSettings = (Map<String, Object>)XContentMapValues.extractValue("target.settings.index", getIndexResponse);
        assertNotNull(indexSettings);
        assertEquals("2", indexSettings.get("number_of_shards"));
        assertEquals("0", indexSettings.get("number_of_replicas"));
        Map<String, Object> aliasData = (Map<String, Object>)XContentMapValues.extractValue("target.aliases.alias", getIndexResponse);
        assertNotNull(aliasData);
    }

    @SuppressWarnings("unchecked")
    public void testSplit() throws IOException {
        createIndex("source", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0)
                .put("index.number_of_routing_shards", 4).build());
        updateIndexSettings("source", Settings.builder().put("index.blocks.write", true));

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", 4).put("index.number_of_replicas", 0).build();
        resizeRequest.setTargetIndex(new org.havenask.action.admin.indices.create.CreateIndexRequest("target")
            .settings(targetSettings)
            .alias(new Alias("alias")));
        ResizeResponse resizeResponse = execute(resizeRequest, highLevelClient().indices()::split, highLevelClient().indices()::splitAsync);
        assertTrue(resizeResponse.isAcknowledged());
        assertTrue(resizeResponse.isShardsAcknowledged());
        Map<String, Object> getIndexResponse = getAsMap("target");
        Map<String, Object> indexSettings = (Map<String, Object>)XContentMapValues.extractValue("target.settings.index", getIndexResponse);
        assertNotNull(indexSettings);
        assertEquals("4", indexSettings.get("number_of_shards"));
        assertEquals("0", indexSettings.get("number_of_replicas"));
        Map<String, Object> aliasData = (Map<String, Object>)XContentMapValues.extractValue("target.aliases.alias", getIndexResponse);
        assertNotNull(aliasData);
    }

    @SuppressWarnings("unchecked")
    public void testClone() throws IOException {
        createIndex("source", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0)
            .put("index.number_of_routing_shards", 4).build());
        updateIndexSettings("source", Settings.builder().put("index.blocks.write", true));

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.CLONE);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build();
        resizeRequest.setTargetIndex(new org.havenask.action.admin.indices.create.CreateIndexRequest("target")
            .settings(targetSettings)
            .alias(new Alias("alias")));
        ResizeResponse resizeResponse = execute(resizeRequest, highLevelClient().indices()::clone, highLevelClient().indices()::cloneAsync);
        assertTrue(resizeResponse.isAcknowledged());
        assertTrue(resizeResponse.isShardsAcknowledged());
        Map<String, Object> getIndexResponse = getAsMap("target");
        Map<String, Object> indexSettings = (Map<String, Object>)XContentMapValues.extractValue("target.settings.index", getIndexResponse);
        assertNotNull(indexSettings);
        assertEquals("2", indexSettings.get("number_of_shards"));
        assertEquals("0", indexSettings.get("number_of_replicas"));
        Map<String, Object> aliasData = (Map<String, Object>)XContentMapValues.extractValue("target.aliases.alias", getIndexResponse);
        assertNotNull(aliasData);
    }

    public void testRollover() throws IOException {
        highLevelClient().indices().create(new CreateIndexRequest("test").alias(new Alias("alias")), RequestOptions.DEFAULT);
        RolloverRequest rolloverRequest = new RolloverRequest("alias", "test_new");
        rolloverRequest.addMaxIndexDocsCondition(1);

        {
            RolloverResponse rolloverResponse = execute(rolloverRequest, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync);
            assertFalse(rolloverResponse.isRolledOver());
            assertFalse(rolloverResponse.isDryRun());
            Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
            assertEquals(1, conditionStatus.size());
            assertFalse(conditionStatus.get("[max_docs: 1]"));
            assertEquals("test", rolloverResponse.getOldIndex());
            assertEquals("test_new", rolloverResponse.getNewIndex());
        }

        highLevelClient().index(new IndexRequest("test").id("1").source("field", "value"), RequestOptions.DEFAULT);
        highLevelClient().index(new IndexRequest("test").id("2").source("field", "value")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL), RequestOptions.DEFAULT);
        //without the refresh the rollover may not happen as the number of docs seen may be off

        {
            rolloverRequest.addMaxIndexAgeCondition(new TimeValue(1));
            rolloverRequest.dryRun(true);
            RolloverResponse rolloverResponse = execute(rolloverRequest, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync);
            assertFalse(rolloverResponse.isRolledOver());
            assertTrue(rolloverResponse.isDryRun());
            Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
            assertEquals(2, conditionStatus.size());
            assertTrue(conditionStatus.get("[max_docs: 1]"));
            assertTrue(conditionStatus.get("[max_age: 1ms]"));
            assertEquals("test", rolloverResponse.getOldIndex());
            assertEquals("test_new", rolloverResponse.getNewIndex());
        }
        {
            String mappings = "{\"properties\":{\"field2\":{\"type\":\"keyword\"}}}";
            rolloverRequest.getCreateIndexRequest().mapping(mappings, XContentType.JSON);
            rolloverRequest.dryRun(false);
            rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(1, ByteSizeUnit.MB));
            RolloverResponse rolloverResponse = execute(rolloverRequest, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync);
            assertTrue(rolloverResponse.isRolledOver());
            assertFalse(rolloverResponse.isDryRun());
            Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
            assertEquals(3, conditionStatus.size());
            assertTrue(conditionStatus.get("[max_docs: 1]"));
            assertTrue(conditionStatus.get("[max_age: 1ms]"));
            assertFalse(conditionStatus.get("[max_size: 1mb]"));
            assertEquals("test", rolloverResponse.getOldIndex());
            assertEquals("test_new", rolloverResponse.getNewIndex());
        }
    }

    public void testRolloverWithTypes() throws IOException {
        highLevelClient().indices().create(new CreateIndexRequest("test").alias(new Alias("alias")), RequestOptions.DEFAULT);
        highLevelClient().index(new IndexRequest("test").id("1").source("field", "value"), RequestOptions.DEFAULT);
        highLevelClient().index(new IndexRequest("test").id("2").source("field", "value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL), RequestOptions.DEFAULT);

        org.havenask.action.admin.indices.rollover.RolloverRequest rolloverRequest =
            new org.havenask.action.admin.indices.rollover.RolloverRequest("alias", "test_new");
        rolloverRequest.addMaxIndexDocsCondition(1);
        rolloverRequest.getCreateIndexRequest().mapping("_doc", "field2", "type=keyword");

        org.havenask.action.admin.indices.rollover.RolloverResponse rolloverResponse = execute(
            rolloverRequest,
            highLevelClient().indices()::rollover,
            highLevelClient().indices()::rolloverAsync,
            expectWarnings(RestRolloverIndexAction.TYPES_DEPRECATION_MESSAGE)
        );
        assertTrue(rolloverResponse.isRolledOver());
        assertFalse(rolloverResponse.isDryRun());
        Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
        assertTrue(conditionStatus.get("[max_docs: 1]"));
        assertEquals("test", rolloverResponse.getOldIndex());
        assertEquals("test_new", rolloverResponse.getNewIndex());
    }

    public void testGetAlias() throws IOException {
        {
            createIndex("index1", Settings.EMPTY);
            client().performRequest(new Request(HttpPut.METHOD_NAME, "/index1/_alias/alias1"));

            createIndex("index2", Settings.EMPTY);
            client().performRequest(new Request(HttpPut.METHOD_NAME, "/index2/_alias/alias2"));

            createIndex("index3", Settings.EMPTY);
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases("alias1");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);

            assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
            assertThat(getAliasesResponse.getAliases().get("index1").size(), equalTo(1));
            AliasMetadata aliasMetadata = getAliasesResponse.getAliases().get("index1").iterator().next();
            assertThat(aliasMetadata, notNullValue());
            assertThat(aliasMetadata.alias(), equalTo("alias1"));
            assertThat(aliasMetadata.getFilter(), nullValue());
            assertThat(aliasMetadata.getIndexRouting(), nullValue());
            assertThat(aliasMetadata.getSearchRouting(), nullValue());
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases("alias*");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);

            assertThat(getAliasesResponse.getAliases().size(), equalTo(2));
            assertThat(getAliasesResponse.getAliases().get("index1").size(), equalTo(1));
            AliasMetadata aliasMetadata1 = getAliasesResponse.getAliases().get("index1").iterator().next();
            assertThat(aliasMetadata1, notNullValue());
            assertThat(aliasMetadata1.alias(), equalTo("alias1"));
            assertThat(getAliasesResponse.getAliases().get("index2").size(), equalTo(1));
            AliasMetadata aliasMetadata2 = getAliasesResponse.getAliases().get("index2").iterator().next();
            assertThat(aliasMetadata2, notNullValue());
            assertThat(aliasMetadata2.alias(), equalTo("alias2"));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases("_all");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);

            assertThat(getAliasesResponse.getAliases().size(), equalTo(2));
            assertThat(getAliasesResponse.getAliases().get("index1").size(), equalTo(1));
            AliasMetadata aliasMetadata1 = getAliasesResponse.getAliases().get("index1").iterator().next();
            assertThat(aliasMetadata1, notNullValue());
            assertThat(aliasMetadata1.alias(), equalTo("alias1"));
            assertThat(getAliasesResponse.getAliases().get("index2").size(), equalTo(1));
            AliasMetadata aliasMetadata2 = getAliasesResponse.getAliases().get("index2").iterator().next();
            assertThat(aliasMetadata2, notNullValue());
            assertThat(aliasMetadata2.alias(), equalTo("alias2"));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases("*");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);

            assertThat(getAliasesResponse.getAliases().size(), equalTo(2));
            assertThat(getAliasesResponse.getAliases().get("index1").size(), equalTo(1));
            AliasMetadata aliasMetadata1 = getAliasesResponse.getAliases().get("index1").iterator().next();
            assertThat(aliasMetadata1, notNullValue());
            assertThat(aliasMetadata1.alias(), equalTo("alias1"));
            assertThat(getAliasesResponse.getAliases().get("index2").size(), equalTo(1));
            AliasMetadata aliasMetadata2 = getAliasesResponse.getAliases().get("index2").iterator().next();
            assertThat(aliasMetadata2, notNullValue());
            assertThat(aliasMetadata2.alias(), equalTo("alias2"));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices("_all");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);

            assertThat("Unexpected number of aliases, got: " + getAliasesResponse.getAliases().toString(),
                    getAliasesResponse.getAliases().size(), equalTo(3));
            assertThat(getAliasesResponse.getAliases().get("index1").size(), equalTo(1));
            AliasMetadata aliasMetadata1 = getAliasesResponse.getAliases().get("index1").iterator().next();
            assertThat(aliasMetadata1, notNullValue());
            assertThat(aliasMetadata1.alias(), equalTo("alias1"));
            assertThat(getAliasesResponse.getAliases().get("index2").size(), equalTo(1));
            AliasMetadata aliasMetadata2 = getAliasesResponse.getAliases().get("index2").iterator().next();
            assertThat(aliasMetadata2, notNullValue());
            assertThat(aliasMetadata2.alias(), equalTo("alias2"));
            assertThat(getAliasesResponse.getAliases().get("index3").size(), equalTo(0));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices("ind*");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);

            assertThat(getAliasesResponse.getAliases().size(), equalTo(3));
            assertThat(getAliasesResponse.getAliases().get("index1").size(), equalTo(1));
            AliasMetadata aliasMetadata1 = getAliasesResponse.getAliases().get("index1").iterator().next();
            assertThat(aliasMetadata1, notNullValue());
            assertThat(aliasMetadata1.alias(), equalTo("alias1"));
            assertThat(getAliasesResponse.getAliases().get("index2").size(), equalTo(1));
            AliasMetadata aliasMetadata2 = getAliasesResponse.getAliases().get("index2").iterator().next();
            assertThat(aliasMetadata2, notNullValue());
            assertThat(aliasMetadata2.alias(), equalTo("alias2"));
            assertThat(getAliasesResponse.getAliases().get("index3").size(), equalTo(0));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);

            assertThat(getAliasesResponse.getAliases().size(), equalTo(3));
            assertThat(getAliasesResponse.getAliases().get("index1").size(), equalTo(1));
            AliasMetadata aliasMetadata1 = getAliasesResponse.getAliases().get("index1").iterator().next();
            assertThat(aliasMetadata1, notNullValue());
            assertThat(aliasMetadata1.alias(), equalTo("alias1"));
            assertThat(getAliasesResponse.getAliases().get("index2").size(), equalTo(1));
            AliasMetadata aliasMetadata2 = getAliasesResponse.getAliases().get("index2").iterator().next();
            assertThat(aliasMetadata2, notNullValue());
            assertThat(aliasMetadata2.alias(), equalTo("alias2"));
            assertThat(getAliasesResponse.getAliases().get("index3").size(), equalTo(0));
        }
    }

    public void testGetAliasesNonExistentIndexOrAlias() throws IOException {
        /*
         * This test is quite extensive as this is the only way we can check that we haven't slid out of sync with the server
         * because the server renders the xcontent in a spot that is difficult for us to access in a unit test.
         */
        String alias = "alias";
        String index = "index";
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(index);
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.getException().getMessage(),
                    equalTo("Havenask exception [type=index_not_found_exception, reason=no such index [index]]"));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest(alias);
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.getError(), equalTo("alias [" + alias + "] missing"));
            assertThat(getAliasesResponse.getException(), nullValue());
        }
        createIndex(index, Settings.EMPTY);
        client().performRequest(new Request(HttpPut.METHOD_NAME, index + "/_alias/" + alias));
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(index, "non_existent_index");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);
            assertThat(getAliasesResponse.getAliases().size(), equalTo(0));
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.getError(), nullValue());
            assertThat(getAliasesResponse.getException().getMessage(),
                    equalTo("Havenask exception [type=index_not_found_exception, reason=no such index [non_existent_index]]"));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(index, "non_existent_index").aliases(alias);
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);
            assertThat(getAliasesResponse.getAliases().size(), equalTo(0));
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.getException().getMessage(),
                    equalTo("Havenask exception [type=index_not_found_exception, reason=no such index [non_existent_index]]"));
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices("non_existent_index*");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.OK));
            assertThat(getAliasesResponse.getAliases().size(), equalTo(0));
            assertThat(getAliasesResponse.getException(), nullValue());
            assertThat(getAliasesResponse.getError(), nullValue());
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(index).aliases(alias, "non_existent_alias");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                    highLevelClient().indices()::getAliasAsync);
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.getError(), equalTo("alias [non_existent_alias] missing"));

            assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
            assertThat(getAliasesResponse.getAliases().get(index).size(), equalTo(1));
            AliasMetadata aliasMetadata = getAliasesResponse.getAliases().get(index).iterator().next();
            assertThat(aliasMetadata, notNullValue());
            assertThat(aliasMetadata.alias(), equalTo(alias));
            /*
            This is the above response in json format:
            {
             "error": "alias [something] missing",
             "status": 404,
             "index": {
               "aliases": {
                 "alias": {}
               }
             }
            }
            */
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().aliases("non_existent_alias*");
            GetAliasesResponse getAliasesResponse = execute(getAliasesRequest, highLevelClient().indices()::getAlias,
                highLevelClient().indices()::getAliasAsync);
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.OK));
            assertThat(getAliasesResponse.getAliases().size(), equalTo(0));
        }
    }

    public void testIndexPutSettings() throws IOException {

        final Setting<Integer> dynamicSetting = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
        final String dynamicSettingKey = IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
        final int dynamicSettingValue = 0;

        final Setting<String> staticSetting = IndexSettings.INDEX_CHECK_ON_STARTUP;
        final String staticSettingKey = IndexSettings.INDEX_CHECK_ON_STARTUP.getKey();
        final String staticSettingValue = "true";

        final Setting<Integer> unmodifiableSetting = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
        final String unmodifiableSettingKey = IndexMetadata.SETTING_NUMBER_OF_SHARDS;
        final int unmodifiableSettingValue = 3;

        String index = "index";
        createIndex(index, Settings.EMPTY);

        assertThat(dynamicSetting.getDefault(Settings.EMPTY), not(dynamicSettingValue));
        UpdateSettingsRequest dynamicSettingRequest = new UpdateSettingsRequest(index);
        dynamicSettingRequest.settings(Settings.builder().put(dynamicSettingKey, dynamicSettingValue).build());
        AcknowledgedResponse response = execute(dynamicSettingRequest, highLevelClient().indices()::putSettings,
                highLevelClient().indices()::putSettingsAsync);

        assertTrue(response.isAcknowledged());
        Map<String, Object> indexSettingsAsMap = getIndexSettingsAsMap(index);
        assertThat(indexSettingsAsMap.get(dynamicSettingKey), equalTo(String.valueOf(dynamicSettingValue)));

        assertThat(staticSetting.getDefault(Settings.EMPTY), not(staticSettingValue));
        UpdateSettingsRequest staticSettingRequest = new UpdateSettingsRequest(index);
        staticSettingRequest.settings(Settings.builder().put(staticSettingKey, staticSettingValue).build());
        HavenaskException exception = expectThrows(HavenaskException.class, () -> execute(staticSettingRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.getMessage(),
                startsWith("Havenask exception [type=illegal_argument_exception, "
                        + "reason=Can't update non dynamic settings [[index.shard.check_on_startup]] for open indices [[index/"));

        indexSettingsAsMap = getIndexSettingsAsMap(index);
        assertNull(indexSettingsAsMap.get(staticSettingKey));

        closeIndex(index);
        response = execute(staticSettingRequest, highLevelClient().indices()::putSettings,
                highLevelClient().indices()::putSettingsAsync);
        assertTrue(response.isAcknowledged());
        openIndex(index);
        indexSettingsAsMap = getIndexSettingsAsMap(index);
        assertThat(indexSettingsAsMap.get(staticSettingKey), equalTo(staticSettingValue));

        assertThat(unmodifiableSetting.getDefault(Settings.EMPTY), not(unmodifiableSettingValue));
        UpdateSettingsRequest unmodifiableSettingRequest = new UpdateSettingsRequest(index);
        unmodifiableSettingRequest.settings(Settings.builder().put(unmodifiableSettingKey, unmodifiableSettingValue).build());
        exception = expectThrows(HavenaskException.class, () -> execute(unmodifiableSettingRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.getMessage(), startsWith(
                "Havenask exception [type=illegal_argument_exception, "
                + "reason=Can't update non dynamic settings [[index.number_of_shards]] for open indices [[index/"));
        closeIndex(index);
        exception = expectThrows(HavenaskException.class, () -> execute(unmodifiableSettingRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.getMessage(), startsWith(
                "Havenask exception [type=illegal_argument_exception, "
                + "reason=final index setting [index.number_of_shards], not updateable"));
    }

    public void testIndexPutSettingNonExistent() throws IOException {

        String index = "index";
        UpdateSettingsRequest indexUpdateSettingsRequest = new UpdateSettingsRequest(index);
        String setting = "no_idea_what_you_are_talking_about";
        int value = 10;
        indexUpdateSettingsRequest.settings(Settings.builder().put(setting, value).build());

        HavenaskException exception = expectThrows(HavenaskException.class, () -> execute(indexUpdateSettingsRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
        assertThat(exception.getMessage(),
            equalTo("Havenask exception [type=index_not_found_exception, reason=no such index [index]]"));

        createIndex(index, Settings.EMPTY);
        exception = expectThrows(HavenaskException.class, () -> execute(indexUpdateSettingsRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), equalTo(
                "Havenask exception [type=illegal_argument_exception, "
                + "reason=unknown setting [index.no_idea_what_you_are_talking_about] please check that any required plugins are installed, "
                + "or check the breaking changes documentation for removed settings]"));
    }

    @SuppressWarnings("unchecked")
    public void testPutTemplateWithTypes() throws Exception {
        org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest putTemplateRequest =
            new org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest()
            .name("my-template")
            .patterns(Arrays.asList("pattern-1", "name-*"))
            .order(10)
            .create(randomBoolean())
            .settings(Settings.builder().put("number_of_shards", "3").put("number_of_replicas", "0"))
            .mapping("doc", "host_name", "type=keyword", "description", "type=text")
            .alias(new Alias("alias-1").indexRouting("abc")).alias(new Alias("{index}-write").searchRouting("xyz"));

        AcknowledgedResponse putTemplateResponse = execute(putTemplateRequest,
            highLevelClient().indices()::putTemplate, highLevelClient().indices()::putTemplateAsync,
            expectWarnings(RestPutIndexTemplateAction.TYPES_DEPRECATION_MESSAGE)
            );
        assertThat(putTemplateResponse.isAcknowledged(), equalTo(true));

        Map<String, Object> templates = getAsMap("/_template/my-template");
        assertThat(templates.keySet(), hasSize(1));
        assertThat(extractValue("my-template.order", templates), equalTo(10));
        assertThat(extractRawValues("my-template.index_patterns", templates), contains("pattern-1", "name-*"));
        assertThat(extractValue("my-template.settings.index.number_of_shards", templates), equalTo("3"));
        assertThat(extractValue("my-template.settings.index.number_of_replicas", templates), equalTo("0"));
        assertThat(extractValue("my-template.mappings.properties.host_name.type", templates), equalTo("keyword"));
        assertThat(extractValue("my-template.mappings.properties.description.type", templates), equalTo("text"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.alias-1", templates), hasEntry("index_routing", "abc"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.{index}-write", templates), hasEntry("search_routing", "xyz"));
    }

    @SuppressWarnings("unchecked")
    public void testPutTemplate() throws Exception {
        PutIndexTemplateRequest putTemplateRequest = new PutIndexTemplateRequest("my-template")
            .patterns(Arrays.asList("pattern-1", "name-*"))
            .order(10)
            .create(randomBoolean())
            .settings(Settings.builder().put("number_of_shards", "3").put("number_of_replicas", "0"))
            .mapping("{ \"properties\": { \"host_name\": { \"type\": \"keyword\" } } }", XContentType.JSON)
            .alias(new Alias("alias-1").indexRouting("abc"))
            .alias(new Alias("alias-1").indexRouting("abc")).alias(new Alias("{index}-write").searchRouting("xyz"));

        AcknowledgedResponse putTemplateResponse = execute(putTemplateRequest,
            highLevelClient().indices()::putTemplate, highLevelClient().indices()::putTemplateAsync);
        assertThat(putTemplateResponse.isAcknowledged(), equalTo(true));

        Map<String, Object> templates = getAsMap("/_template/my-template");
        assertThat(templates.keySet(), hasSize(1));
        assertThat(extractValue("my-template.order", templates), equalTo(10));
        assertThat(extractRawValues("my-template.index_patterns", templates), contains("pattern-1", "name-*"));
        assertThat(extractValue("my-template.settings.index.number_of_shards", templates), equalTo("3"));
        assertThat(extractValue("my-template.settings.index.number_of_replicas", templates), equalTo("0"));
        assertThat(extractValue("my-template.mappings.properties.host_name.type", templates), equalTo("keyword"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.alias-1", templates), hasEntry("index_routing", "abc"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.{index}-write", templates), hasEntry("search_routing", "xyz"));
    }

    public void testPutTemplateWithDeprecatedTemplateField() throws Exception {
        PutIndexTemplateRequest putTemplateRequest = new PutIndexTemplateRequest("my-template")
            .source(XContentFactory.jsonBuilder()
                .startObject()
                    .field("template", "name-*")
                    .field("order", 10)
                    .startObject("settings")
                        .field("number_of_shards", 3)
                        .field("number_of_replicas", 0)
                    .endObject()
                .endObject());

        AcknowledgedResponse putTemplateResponse = execute(putTemplateRequest,
            highLevelClient().indices()::putTemplate,
            highLevelClient().indices()::putTemplateAsync,
            expectWarnings("Deprecated field [template] used, replaced by [index_patterns]"));
        assertThat(putTemplateResponse.isAcknowledged(), equalTo(true));

        Map<String, Object> templates = getAsMap("/_template/my-template");
        assertThat(templates.keySet(), hasSize(1));
        assertThat(extractValue("my-template.order", templates), equalTo(10));
        assertThat(extractRawValues("my-template.index_patterns", templates), contains("name-*"));
        assertThat(extractValue("my-template.settings.index.number_of_shards", templates), equalTo("3"));
        assertThat(extractValue("my-template.settings.index.number_of_replicas", templates), equalTo("0"));
    }

    public void testPutTemplateWithTypesUsingUntypedAPI() throws Exception {
        PutIndexTemplateRequest putTemplateRequest = new PutIndexTemplateRequest("my-template")
            .patterns(Arrays.asList("pattern-1", "name-*"))
            .order(10)
            .create(randomBoolean())
            .settings(Settings.builder().put("number_of_shards", "3").put("number_of_replicas", "0"))
            .mapping(
                "{"
                    + "  \"my_doc_type\": {"
                    + "    \"properties\": {"
                    + "      \"host_name\": {"
                    + "        \"type\": \"keyword\""
                    + "      }"
                    + "    }"
                    + "  }"
                    + "}",
                XContentType.JSON
            )
            .alias(new Alias("alias-1").indexRouting("abc")).alias(new Alias("{index}-write").searchRouting("xyz"));


        HavenaskStatusException badMappingError = expectThrows(HavenaskStatusException.class,
                () -> execute(putTemplateRequest,
                        highLevelClient().indices()::putTemplate, highLevelClient().indices()::putTemplateAsync));
        assertThat(badMappingError.getDetailedMessage(),
                containsString("Root mapping definition has unsupported parameters:  [my_doc_type"));
    }

    @SuppressWarnings("unchecked")
    public void testPutTemplateWithNoTypesUsingTypedApi() throws Exception {
        org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest putTemplateRequest =
            new org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest()
            .name("my-template")
            .patterns(Arrays.asList("pattern-1", "name-*"))
            .order(10)
            .create(randomBoolean())
            .settings(Settings.builder().put("number_of_shards", "3").put("number_of_replicas", "0"))
            .mapping("my_doc_type",
                    // Note that the declared type is missing from the mapping
                    "{ "
                    + "\"properties\":{"
                    + "\"host_name\": {\"type\":\"keyword\"},"
                    + "\"description\": {\"type\":\"text\"}"
                    + "}"
                    + "}", XContentType.JSON)
            .alias(new Alias("alias-1").indexRouting("abc")).alias(new Alias("{index}-write").searchRouting("xyz"));

        AcknowledgedResponse putTemplateResponse = execute(putTemplateRequest,
            highLevelClient().indices()::putTemplate, highLevelClient().indices()::putTemplateAsync,
            expectWarnings(RestPutIndexTemplateAction.TYPES_DEPRECATION_MESSAGE)
            );
        assertThat(putTemplateResponse.isAcknowledged(), equalTo(true));

        Map<String, Object> templates = getAsMap("/_template/my-template");
        assertThat(templates.keySet(), hasSize(1));
        assertThat(extractValue("my-template.order", templates), equalTo(10));
        assertThat(extractRawValues("my-template.index_patterns", templates), contains("pattern-1", "name-*"));
        assertThat(extractValue("my-template.settings.index.number_of_shards", templates), equalTo("3"));
        assertThat(extractValue("my-template.settings.index.number_of_replicas", templates), equalTo("0"));
        assertThat(extractValue("my-template.mappings.properties.host_name.type", templates), equalTo("keyword"));
        assertThat(extractValue("my-template.mappings.properties.description.type", templates), equalTo("text"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.alias-1", templates), hasEntry("index_routing", "abc"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.{index}-write", templates), hasEntry("search_routing", "xyz"));
    }

    public void testPutTemplateBadRequests() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // Failed to validate because index patterns are missing
        PutIndexTemplateRequest withoutPattern = new PutIndexTemplateRequest("t1");
        ValidationException withoutPatternError = expectThrows(ValidationException.class,
            () -> execute(withoutPattern, client.indices()::putTemplate, client.indices()::putTemplateAsync));
        assertThat(withoutPatternError.validationErrors(), contains("index patterns are missing"));

        // Create-only specified but an template exists already
        PutIndexTemplateRequest goodTemplate = new PutIndexTemplateRequest("t2").patterns(Arrays.asList("qa-*", "prod-*"));
        assertTrue(execute(goodTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync).isAcknowledged());
        goodTemplate.create(true);
        HavenaskException alreadyExistsError = expectThrows(HavenaskException.class,
            () -> execute(goodTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync));
        assertThat(alreadyExistsError.getDetailedMessage(),
            containsString("[type=illegal_argument_exception, reason=index_template [t2] already exists]"));
        goodTemplate.create(false);
        assertTrue(execute(goodTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync).isAcknowledged());

        // Rejected due to unknown settings
        PutIndexTemplateRequest unknownSettingTemplate = new PutIndexTemplateRequest("t3")
            .patterns(Collections.singletonList("any"))
            .settings(Settings.builder().put("this-setting-does-not-exist", 100));
        HavenaskStatusException unknownSettingError = expectThrows(HavenaskStatusException.class,
            () -> execute(unknownSettingTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync));
        assertThat(unknownSettingError.getDetailedMessage(), containsString("unknown setting [index.this-setting-does-not-exist]"));
    }

    public void testValidateQuery() throws IOException{
        String index = "some_index";
        createIndex(index, Settings.EMPTY);
        QueryBuilder builder = QueryBuilders
            .boolQuery()
            .must(QueryBuilders.queryStringQuery("*:*"))
            .filter(QueryBuilders.termQuery("user", "foobar"));
        ValidateQueryRequest request = new ValidateQueryRequest(index).query(builder);
        request.explain(randomBoolean());
        ValidateQueryResponse response = execute(request, highLevelClient().indices()::validateQuery,
            highLevelClient().indices()::validateQueryAsync);
        assertTrue(response.isValid());
    }

    public void testInvalidValidateQuery() throws IOException{
        String index = "shakespeare";

        createIndex(index, Settings.EMPTY);
        Request postDoc = new Request(HttpPost.METHOD_NAME, "/" + index + "/_doc");
        postDoc.setJsonEntity(
            "{"
                + "  \"type\": \"act\","
                + "  \"line_id\": 1,"
                + "  \"play_name\": \"Henry IV\","
                + "  \"speech_number\": \"\","
                + "  \"line_number\": \"\","
                + "  \"speaker\": \"\","
                + "  \"text_entry\": \"ACT I\""
                + "}"
        );
        assertOK(client().performRequest(postDoc));

        QueryBuilder builder = QueryBuilders
            .queryStringQuery("line_id:foo")
            .lenient(false);
        ValidateQueryRequest request = new ValidateQueryRequest(index).query(builder);
        request.explain(true);
        ValidateQueryResponse response = execute(request, highLevelClient().indices()::validateQuery,
            highLevelClient().indices()::validateQueryAsync);
        assertFalse(response.isValid());
    }

    // Tests the deprecated form of the API that returns templates with doc types (using the server-side's GetIndexTemplateResponse)
    public void testCRUDIndexTemplateWithTypes() throws Exception {
        RestHighLevelClient client = highLevelClient();

        org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest putTemplate1 =
            new org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest().name("template-1")
            .patterns(Arrays.asList("pattern-1", "name-1")).alias(new Alias("alias-1"));
        assertThat(execute(putTemplate1, client.indices()::putTemplate, client.indices()::putTemplateAsync
                , expectWarnings(RestPutIndexTemplateAction.TYPES_DEPRECATION_MESSAGE))
            .isAcknowledged(), equalTo(true));
        org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest putTemplate2 =
            new org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest().name("template-2")
            .patterns(Arrays.asList("pattern-2", "name-2"))
            .mapping("custom_doc_type", "name", "type=text")
            .settings(Settings.builder().put("number_of_shards", "2").put("number_of_replicas", "0"));
        assertThat(execute(putTemplate2, client.indices()::putTemplate, client.indices()::putTemplateAsync,
                expectWarnings(RestPutIndexTemplateAction.TYPES_DEPRECATION_MESSAGE))
                .isAcknowledged(), equalTo(true));

        org.havenask.action.admin.indices.template.get.GetIndexTemplatesResponse getTemplate1 = execute(
                new GetIndexTemplatesRequest("template-1"),
                client.indices()::getTemplate, client.indices()::getTemplateAsync,
                expectWarnings(RestGetIndexTemplateAction.TYPES_DEPRECATION_MESSAGE));
        assertThat(getTemplate1.getIndexTemplates(), hasSize(1));
        org.havenask.cluster.metadata.IndexTemplateMetadata template1 = getTemplate1.getIndexTemplates().get(0);
        assertThat(template1.name(), equalTo("template-1"));
        assertThat(template1.patterns(), contains("pattern-1", "name-1"));
        assertTrue(template1.aliases().containsKey("alias-1"));

        //Check the typed version of the call
        org.havenask.action.admin.indices.template.get.GetIndexTemplatesResponse getTemplate2 =
            execute(new GetIndexTemplatesRequest("template-2"),
                client.indices()::getTemplate, client.indices()::getTemplateAsync,
                expectWarnings(RestGetIndexTemplateAction.TYPES_DEPRECATION_MESSAGE));
        assertThat(getTemplate2.getIndexTemplates(), hasSize(1));
        org.havenask.cluster.metadata.IndexTemplateMetadata template2 = getTemplate2.getIndexTemplates().get(0);
        assertThat(template2.name(), equalTo("template-2"));
        assertThat(template2.patterns(), contains("pattern-2", "name-2"));
        assertTrue(template2.aliases().isEmpty());
        assertThat(template2.settings().get("index.number_of_shards"), equalTo("2"));
        assertThat(template2.settings().get("index.number_of_replicas"), equalTo("0"));
        // Ugly deprecated form of API requires use of doc type to get at mapping object which is CompressedXContent
        assertTrue(template2.mappings().containsKey("custom_doc_type"));

        List<String> names = randomBoolean()
            ? Arrays.asList("*plate-1", "template-2")
            : Arrays.asList("template-*");
        GetIndexTemplatesRequest getBothRequest = new GetIndexTemplatesRequest(names);
        org.havenask.action.admin.indices.template.get.GetIndexTemplatesResponse getBoth = execute(
                getBothRequest, client.indices()::getTemplate, client.indices()::getTemplateAsync,
                expectWarnings(RestGetIndexTemplateAction.TYPES_DEPRECATION_MESSAGE));
        assertThat(getBoth.getIndexTemplates(), hasSize(2));
        assertThat(getBoth.getIndexTemplates().stream().map(org.havenask.cluster.metadata.IndexTemplateMetadata::getName).toArray(),
            arrayContainingInAnyOrder("template-1", "template-2"));

        GetIndexTemplatesRequest getAllRequest = new GetIndexTemplatesRequest();
        org.havenask.action.admin.indices.template.get.GetIndexTemplatesResponse getAll = execute(
                getAllRequest, client.indices()::getTemplate, client.indices()::getTemplateAsync,
                expectWarnings(RestGetIndexTemplateAction.TYPES_DEPRECATION_MESSAGE));
        assertThat(getAll.getIndexTemplates().size(), greaterThanOrEqualTo(2));
        assertThat(getAll.getIndexTemplates().stream().map(org.havenask.cluster.metadata.IndexTemplateMetadata::getName)
                .collect(Collectors.toList()),
            hasItems("template-1", "template-2"));

        assertTrue(execute(new DeleteIndexTemplateRequest("template-1"),
            client.indices()::deleteTemplate, client.indices()::deleteTemplateAsync).isAcknowledged());
        assertThat(expectThrows(HavenaskException.class, () -> execute(new GetIndexTemplatesRequest("template-1"),
            client.indices()::getTemplate, client.indices()::getTemplateAsync)).status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(expectThrows(HavenaskException.class, () -> execute(new DeleteIndexTemplateRequest("template-1"),
            client.indices()::deleteTemplate, client.indices()::deleteTemplateAsync)).status(), equalTo(RestStatus.NOT_FOUND));

        assertThat(execute(new GetIndexTemplatesRequest("template-*"),
            client.indices()::getTemplate, client.indices()::getTemplateAsync,
              expectWarnings(RestGetIndexTemplateAction.TYPES_DEPRECATION_MESSAGE)).getIndexTemplates(), hasSize(1));
        assertThat(execute(new GetIndexTemplatesRequest("template-*"),
            client.indices()::getTemplate, client.indices()::getTemplateAsync,
              expectWarnings(RestGetIndexTemplateAction.TYPES_DEPRECATION_MESSAGE)).getIndexTemplates()
                .get(0).name(), equalTo("template-2"));

        assertTrue(execute(new DeleteIndexTemplateRequest("template-*"),
            client.indices()::deleteTemplate, client.indices()::deleteTemplateAsync).isAcknowledged());
        assertThat(expectThrows(HavenaskException.class, () -> execute(new GetIndexTemplatesRequest("template-*"),
            client.indices()::getTemplate, client.indices()::getTemplateAsync,
              expectWarnings(RestGetIndexTemplateAction.TYPES_DEPRECATION_MESSAGE))).status(), equalTo(RestStatus.NOT_FOUND));
    }


    public void testCRUDIndexTemplate() throws Exception {
        RestHighLevelClient client = highLevelClient();

        PutIndexTemplateRequest putTemplate1 = new PutIndexTemplateRequest("template-1")
            .patterns(Arrays.asList("pattern-1", "name-1")).alias(new Alias("alias-1"));
        assertThat(execute(putTemplate1, client.indices()::putTemplate, client.indices()::putTemplateAsync).isAcknowledged(),
            equalTo(true));
        PutIndexTemplateRequest putTemplate2 = new PutIndexTemplateRequest("template-2")
            .patterns(Arrays.asList("pattern-2", "name-2"))
            .mapping("{\"properties\": { \"name\": { \"type\": \"text\" }}}", XContentType.JSON)
            .settings(Settings.builder().put("number_of_shards", "2").put("number_of_replicas", "0"));
        assertThat(execute(putTemplate2, client.indices()::putTemplate, client.indices()::putTemplateAsync)
                .isAcknowledged(), equalTo(true));

        GetIndexTemplatesResponse getTemplate1 = execute(
                new GetIndexTemplatesRequest("template-1"),
                client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync);
        assertThat(getTemplate1.getIndexTemplates(), hasSize(1));
        IndexTemplateMetadata template1 = getTemplate1.getIndexTemplates().get(0);
        assertThat(template1.name(), equalTo("template-1"));
        assertThat(template1.patterns(), contains("pattern-1", "name-1"));
        assertTrue(template1.aliases().containsKey("alias-1"));

        GetIndexTemplatesResponse getTemplate2 = execute(new GetIndexTemplatesRequest("template-2"),
            client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync);
        assertThat(getTemplate2.getIndexTemplates(), hasSize(1));
        IndexTemplateMetadata template2 = getTemplate2.getIndexTemplates().get(0);
        assertThat(template2.name(), equalTo("template-2"));
        assertThat(template2.patterns(), contains("pattern-2", "name-2"));
        assertTrue(template2.aliases().isEmpty());
        assertThat(template2.settings().get("index.number_of_shards"), equalTo("2"));
        assertThat(template2.settings().get("index.number_of_replicas"), equalTo("0"));
        // New API returns a MappingMetadata class rather than CompressedXContent for the mapping
        assertTrue(template2.mappings().sourceAsMap().containsKey("properties"));
        @SuppressWarnings("unchecked")
        Map<String, Object> props = (Map<String, Object>) template2.mappings().sourceAsMap().get("properties");
        assertTrue(props.containsKey("name"));



        List<String> names = randomBoolean()
            ? Arrays.asList("*plate-1", "template-2")
            : Arrays.asList("template-*");
        GetIndexTemplatesRequest getBothRequest = new GetIndexTemplatesRequest(names);
        GetIndexTemplatesResponse getBoth = execute(
                getBothRequest, client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync);
        assertThat(getBoth.getIndexTemplates(), hasSize(2));
        assertThat(getBoth.getIndexTemplates().stream().map(IndexTemplateMetadata::name).toArray(),
            arrayContainingInAnyOrder("template-1", "template-2"));

        GetIndexTemplatesRequest getAllRequest = new GetIndexTemplatesRequest();
        GetIndexTemplatesResponse getAll = execute(
                getAllRequest, client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync);
        assertThat(getAll.getIndexTemplates().size(), greaterThanOrEqualTo(2));
        assertThat(getAll.getIndexTemplates().stream().map(IndexTemplateMetadata::name)
                .collect(Collectors.toList()),
            hasItems("template-1", "template-2"));

        assertTrue(execute(new DeleteIndexTemplateRequest("template-1"),
            client.indices()::deleteTemplate, client.indices()::deleteTemplateAsync).isAcknowledged());
        assertThat(expectThrows(HavenaskException.class, () -> execute(new GetIndexTemplatesRequest("template-1"),
            client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync)).status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(expectThrows(HavenaskException.class, () -> execute(new DeleteIndexTemplateRequest("template-1"),
            client.indices()::deleteTemplate, client.indices()::deleteTemplateAsync)).status(), equalTo(RestStatus.NOT_FOUND));

        assertThat(execute(new GetIndexTemplatesRequest("template-*"),
            client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync).getIndexTemplates(), hasSize(1));
        assertThat(execute(new GetIndexTemplatesRequest("template-*"),
            client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync).getIndexTemplates()
                .get(0).name(), equalTo("template-2"));

        assertTrue(execute(new DeleteIndexTemplateRequest("template-*"),
            client.indices()::deleteTemplate, client.indices()::deleteTemplateAsync).isAcknowledged());
        assertThat(expectThrows(HavenaskException.class, () -> execute(new GetIndexTemplatesRequest("template-*"),
            client.indices()::getIndexTemplate, client.indices()::getIndexTemplateAsync)).status(), equalTo(RestStatus.NOT_FOUND));
    }

    public void testIndexTemplatesExist() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            for (String suffix : Arrays.asList("1", "2")) {

                final PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest("template-" + suffix)
                    .patterns(Arrays.asList("pattern-" + suffix, "name-" + suffix))
                    .alias(new Alias("alias-" + suffix));
                assertTrue(execute(putRequest, client.indices()::putTemplate, client.indices()::putTemplateAsync).isAcknowledged());

                final IndexTemplatesExistRequest existsRequest = new IndexTemplatesExistRequest("template-" + suffix);
                assertTrue(execute(existsRequest, client.indices()::existsTemplate, client.indices()::existsTemplateAsync));
            }
        }

        {
            final List<String> templateNames = randomBoolean()
                ? Arrays.asList("*plate-1", "template-2")
                : Arrays.asList("template-*");

            final IndexTemplatesExistRequest bothRequest = new IndexTemplatesExistRequest(templateNames);
            assertTrue(execute(bothRequest, client.indices()::existsTemplate, client.indices()::existsTemplateAsync));
        }

        {
            final IndexTemplatesExistRequest neitherRequest = new IndexTemplatesExistRequest("neither-*");
            assertFalse(execute(neitherRequest, client.indices()::existsTemplate, client.indices()::existsTemplateAsync));
        }
    }

    public void testAnalyze() throws Exception {

        RestHighLevelClient client = highLevelClient();

        AnalyzeRequest noindexRequest = AnalyzeRequest.withGlobalAnalyzer("english", "One two three");
        AnalyzeResponse noindexResponse = execute(noindexRequest, client.indices()::analyze, client.indices()::analyzeAsync);

        assertThat(noindexResponse.getTokens(), hasSize(3));

        AnalyzeRequest detailsRequest = AnalyzeRequest.withGlobalAnalyzer("english", "One two three").explain(true);
        AnalyzeResponse detailsResponse = execute(detailsRequest, client.indices()::analyze, client.indices()::analyzeAsync);

        assertNotNull(detailsResponse.detail());
    }

    public void testDeleteAlias() throws IOException {
        String index = "test";
        createIndex(index, Settings.EMPTY);

        String alias = "alias";
        String alias2 = "alias2";
        IndicesAliasesRequest aliasesAddRemoveRequest = new IndicesAliasesRequest();
        aliasesAddRemoveRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).indices(index).alias(alias));
        aliasesAddRemoveRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).indices(index).alias(alias + "2"));
        AcknowledgedResponse aliasResponse = execute(aliasesAddRemoveRequest, highLevelClient().indices()::updateAliases,
            highLevelClient().indices()::updateAliasesAsync);
        assertTrue(aliasResponse.isAcknowledged());
        assertThat(aliasExists(alias), equalTo(true));
        assertThat(aliasExists(alias2), equalTo(true));
        assertThat(aliasExists(index, alias), equalTo(true));
        assertThat(aliasExists(index, alias2), equalTo(true));

        DeleteAliasRequest request = new DeleteAliasRequest(index, alias);
        org.havenask.client.core.AcknowledgedResponse aliasDeleteResponse = execute(request,
            highLevelClient().indices()::deleteAlias,
            highLevelClient().indices()::deleteAliasAsync);

        assertThat(aliasExists(alias), equalTo(false));
        assertThat(aliasExists(alias2), equalTo(true));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(index, alias2), equalTo(true));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/60461")
    public void testDataStreams() throws Exception {
        String dataStreamName = "data-stream";

        CompressedXContent mappings = new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"}}}");
        Template template = new Template(null, mappings, null);
        ComposableIndexTemplate indexTemplate = new ComposableIndexTemplate(Collections.singletonList(dataStreamName), template,
            Collections.emptyList(), 1L, 1L, new HashMap<>(), new ComposableIndexTemplate.DataStreamTemplate());
        PutComposableIndexTemplateRequest putComposableIndexTemplateRequest =
            new PutComposableIndexTemplateRequest().name("ds-template").create(true).indexTemplate(indexTemplate);
        AcknowledgedResponse response = execute(putComposableIndexTemplateRequest,
            highLevelClient().indices()::putIndexTemplate, highLevelClient().indices()::putIndexTemplateAsync);
        assertThat(response.isAcknowledged(), equalTo(true));

        CreateDataStreamRequest createDataStreamRequest = new CreateDataStreamRequest(dataStreamName);
        IndicesClient indices = highLevelClient().indices();
        response = execute(createDataStreamRequest, indices::createDataStream, indices::createDataStreamAsync);
        assertThat(response.isAcknowledged(), equalTo(true));
        ensureHealth(dataStreamName, (request -> {
            request.addParameter("wait_for_status", "yellow");
        }));

        GetDataStreamRequest getDataStreamRequest = new GetDataStreamRequest(dataStreamName);
        GetDataStreamResponse getDataStreamResponse = execute(getDataStreamRequest, indices::getDataStream, indices::getDataStreamAsync);
        List<DataStream> dataStreams = getDataStreamResponse.getDataStreams();
        assertThat(dataStreams, hasSize(1));
        DataStream dataStream = dataStreams.get(0);
        assertThat(dataStream.getName(), equalTo(dataStreamName));
        assertThat(dataStream.getGeneration(), equalTo(1L));
        assertThat(dataStream.getTimeStampField(), equalTo("@timestamp"));
        assertThat(dataStream.getIndices(), hasSize(1));

        getDataStreamRequest = new GetDataStreamRequest(null);
        getDataStreamResponse = execute(getDataStreamRequest, indices::getDataStream, indices::getDataStreamAsync);
        dataStreams = getDataStreamResponse.getDataStreams();
        assertThat(dataStreams, hasSize(1));
        dataStream = dataStreams.get(0);
        assertThat(dataStream.getName(), equalTo(dataStreamName));
        assertThat(dataStream.getGeneration(), equalTo(1L));
        assertThat(dataStream.getTimeStampField(), equalTo("@timestamp"));
        assertThat(dataStream.getIndices(), hasSize(1));

        DataStreamsStatsRequest dataStreamsStatsRequest = new DataStreamsStatsRequest();
        DataStreamsStatsResponse dataStreamsStatsResponse = execute(dataStreamsStatsRequest, indices::dataStreamsStats,
            indices::dataStreamsStatsAsync);
        int dataStreamsCount = dataStreamsStatsResponse.getDataStreamCount();
        assertThat(dataStreamsCount, equalTo(1));
        int backingIndices = dataStreamsStatsResponse.getBackingIndices();
        assertThat(backingIndices, equalTo(1));
        ByteSizeValue byteSizeValue = dataStreamsStatsResponse.getTotalStoreSize();
        assertThat(byteSizeValue, notNullValue());
        assertThat(byteSizeValue.getBytes(), not(equalTo(0L)));
        Map<String, DataStreamStats> dataStreamsStats = dataStreamsStatsResponse.getDataStreams();
        assertThat(dataStreamsStats, notNullValue());
        assertThat(dataStreamsStats.size(), equalTo(1));
        DataStreamStats dataStreamStat = dataStreamsStats.get(dataStreamName);
        assertThat(dataStreamStat, notNullValue());
        assertThat(dataStreamStat.getDataStream(), equalTo(dataStreamName));
        assertThat(dataStreamStat.getBackingIndices(), equalTo(1));
        assertThat(dataStreamStat.getMaximumTimestamp(), equalTo(0L)); // No data in here
        assertThat(dataStreamStat.getStoreSize().getBytes(), not(equalTo(0L))); // but still takes up some space on disk

        DeleteDataStreamRequest deleteDataStreamRequest = new DeleteDataStreamRequest(dataStreamName);
        response = execute(deleteDataStreamRequest, indices::deleteDataStream, indices::deleteDataStreamAsync);
        assertThat(response.isAcknowledged(), equalTo(true));

        getDataStreamRequest = new GetDataStreamRequest(null);
        getDataStreamResponse = execute(getDataStreamRequest, indices::getDataStream, indices::getDataStreamAsync);
        dataStreams = getDataStreamResponse.getDataStreams();
        assertThat(dataStreams, hasSize(0));

        getDataStreamRequest = new GetDataStreamRequest(dataStreamName);
        GetDataStreamRequest finalGetDataStreamRequest = getDataStreamRequest;
        HavenaskStatusException e = expectThrows(HavenaskStatusException.class, () -> execute(finalGetDataStreamRequest,
            indices::getDataStream, indices::getDataStreamAsync));
        assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
    }

    public void testIndexTemplates() throws Exception {
        String templateName = "my-template";
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        CompressedXContent mappings = new CompressedXContent("{\"properties\":{\"host_name\":{\"type\":\"keyword\"}}}");
        AliasMetadata alias = AliasMetadata.builder("alias").writeIndex(true).build();
        Template template = new Template(settings, mappings, Collections.singletonMap("alias", alias));
        List<String> pattern = Collections.singletonList("pattern");
        ComposableIndexTemplate indexTemplate =
            new ComposableIndexTemplate(pattern, template, Collections.emptyList(), 1L, 1L, new HashMap<>(), null);
        PutComposableIndexTemplateRequest putComposableIndexTemplateRequest =
            new PutComposableIndexTemplateRequest().name(templateName).create(true).indexTemplate(indexTemplate);

        AcknowledgedResponse response = execute(putComposableIndexTemplateRequest,
            highLevelClient().indices()::putIndexTemplate, highLevelClient().indices()::putIndexTemplateAsync);
        assertThat(response.isAcknowledged(), equalTo(true));

        ComposableIndexTemplateExistRequest composableIndexTemplateExistRequest = new ComposableIndexTemplateExistRequest(templateName);
        boolean exist = execute(composableIndexTemplateExistRequest,
            highLevelClient().indices()::existsIndexTemplate, highLevelClient().indices()::existsIndexTemplateAsync);

        assertTrue(exist);

        GetComposableIndexTemplateRequest getComposableIndexTemplateRequest = new GetComposableIndexTemplateRequest(templateName);
        GetComposableIndexTemplatesResponse getResponse = execute(getComposableIndexTemplateRequest,
            highLevelClient().indices()::getIndexTemplate, highLevelClient().indices()::getIndexTemplateAsync);

        assertThat(getResponse.getIndexTemplates().size(), equalTo(1));
        assertThat(getResponse.getIndexTemplates().containsKey(templateName), equalTo(true));
        assertThat(getResponse.getIndexTemplates().get(templateName), equalTo(indexTemplate));

        DeleteComposableIndexTemplateRequest deleteComposableIndexTemplateRequest = new DeleteComposableIndexTemplateRequest(templateName);
        response = execute(deleteComposableIndexTemplateRequest, highLevelClient().indices()::deleteIndexTemplate,
            highLevelClient().indices()::deleteIndexTemplateAsync);
        assertThat(response.isAcknowledged(), equalTo(true));

        HavenaskStatusException statusException = expectThrows(HavenaskStatusException.class,
            () -> execute(getComposableIndexTemplateRequest,
                highLevelClient().indices()::getIndexTemplate, highLevelClient().indices()::getIndexTemplateAsync));

        assertThat(statusException.status(), equalTo(RestStatus.NOT_FOUND));

        exist = execute(composableIndexTemplateExistRequest,
            highLevelClient().indices()::existsIndexTemplate, highLevelClient().indices()::existsIndexTemplateAsync);

        assertFalse(exist);
    }

    public void testSimulateIndexTemplate() throws Exception {
        String templateName = "my-template";
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        CompressedXContent mappings = new CompressedXContent("{\"properties\":{\"host_name\":{\"type\":\"keyword\"}}}");
        AliasMetadata alias = AliasMetadata.builder("alias").writeIndex(true).build();
        Template template = new Template(settings, mappings, org.havenask.common.collect.Map.of("alias", alias));
        List<String> pattern = org.havenask.common.collect.List.of("pattern");
        ComposableIndexTemplate indexTemplate =
            new ComposableIndexTemplate(pattern, template, Collections.emptyList(), 1L, 1L, new HashMap<>(), null);
        PutComposableIndexTemplateRequest putComposableIndexTemplateRequest =
            new PutComposableIndexTemplateRequest().name(templateName).create(true).indexTemplate(indexTemplate);

        AcknowledgedResponse response = execute(putComposableIndexTemplateRequest,
            highLevelClient().indices()::putIndexTemplate, highLevelClient().indices()::putIndexTemplateAsync);
        assertThat(response.isAcknowledged(), equalTo(true));

        SimulateIndexTemplateRequest simulateIndexTemplateRequest = new SimulateIndexTemplateRequest("pattern");
        AliasMetadata simulationAlias = AliasMetadata.builder("simulation-alias").writeIndex(true).build();
        ComposableIndexTemplate simulationTemplate = new ComposableIndexTemplate(pattern, new Template(null, null,
            org.havenask.common.collect.Map.of("simulation-alias", simulationAlias)), Collections.emptyList(), 2L, 1L,
            new HashMap<>(), null);
        PutComposableIndexTemplateRequest newIndexTemplateReq =
            new PutComposableIndexTemplateRequest().name("used-for-simulation").create(true).indexTemplate(indexTemplate);
        newIndexTemplateReq.indexTemplate(simulationTemplate);
        simulateIndexTemplateRequest.indexTemplateV2Request(newIndexTemplateReq);

        SimulateIndexTemplateResponse simulateResponse = execute(simulateIndexTemplateRequest,
            highLevelClient().indices()::simulateIndexTemplate, highLevelClient().indices()::simulateIndexTemplateAsync);

        Map<String, AliasMetadata> aliases = simulateResponse.resolvedTemplate().aliases();
        assertThat(aliases, is(notNullValue()));
        assertThat("the template we provided for the simulation has a higher priority than the one in the system",
            aliases.get("simulation-alias"), is(notNullValue()));
        assertThat(aliases.get("simulation-alias").getAlias(), is("simulation-alias"));
        assertThat("existing template overlaps the higher priority template we provided for the simulation",
            simulateResponse.overlappingTemplates().get("my-template").get(0), is("pattern"));
    }
}
