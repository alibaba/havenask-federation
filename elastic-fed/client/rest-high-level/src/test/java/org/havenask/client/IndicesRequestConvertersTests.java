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

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.lucene.util.LuceneTestCase;
import org.havenask.client.Request;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.admin.indices.alias.IndicesAliasesRequest;
import org.havenask.action.admin.indices.alias.get.GetAliasesRequest;
import org.havenask.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.havenask.action.admin.indices.delete.DeleteIndexRequest;
import org.havenask.action.admin.indices.flush.FlushRequest;
import org.havenask.action.admin.indices.flush.SyncedFlushRequest;
import org.havenask.action.admin.indices.forcemerge.ForceMergeRequest;
import org.havenask.action.admin.indices.open.OpenIndexRequest;
import org.havenask.action.admin.indices.refresh.RefreshRequest;
import org.havenask.action.admin.indices.settings.get.GetSettingsRequest;
import org.havenask.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.havenask.action.admin.indices.shrink.ResizeType;
import org.havenask.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.havenask.action.admin.indices.validate.query.ValidateQueryRequest;
import org.havenask.action.support.master.AcknowledgedRequest;
import org.havenask.client.indices.AnalyzeRequest;
import org.havenask.client.indices.CloseIndexRequest;
import org.havenask.client.indices.CreateDataStreamRequest;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.client.indices.DeleteAliasRequest;
import org.havenask.client.indices.DeleteDataStreamRequest;
import org.havenask.client.indices.GetDataStreamRequest;
import org.havenask.client.indices.GetFieldMappingsRequest;
import org.havenask.client.indices.GetIndexRequest;
import org.havenask.client.indices.GetIndexTemplatesRequest;
import org.havenask.client.indices.GetMappingsRequest;
import org.havenask.client.indices.IndexTemplatesExistRequest;
import org.havenask.client.indices.PutIndexTemplateRequest;
import org.havenask.client.indices.PutMappingRequest;
import org.havenask.client.indices.RandomCreateIndexGenerator;
import org.havenask.client.indices.ResizeRequest;
import org.havenask.client.indices.rollover.RolloverRequest;
import org.havenask.common.CheckedFunction;
import org.havenask.common.Strings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.CollectionUtils;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.havenask.client.indices.RandomCreateIndexGenerator.randomAliases;
import static org.havenask.client.indices.RandomCreateIndexGenerator.randomMapping;
import static org.havenask.index.RandomCreateIndexGenerator.randomAlias;
import static org.havenask.index.RandomCreateIndexGenerator.randomIndexSettings;
import static org.havenask.index.alias.RandomAliasActionsGenerator.randomAliasAction;
import static org.havenask.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndicesRequestConvertersTests extends HavenaskTestCase {

    public void testAnalyzeRequest() throws Exception {
        AnalyzeRequest indexAnalyzeRequest
            = AnalyzeRequest.withIndexAnalyzer("test_index", "test_analyzer", "Here is some text");

        Request request = IndicesRequestConverters.analyze(indexAnalyzeRequest);
        assertThat(request.getEndpoint(), equalTo("/test_index/_analyze"));
        RequestConvertersTests.assertToXContentBody(indexAnalyzeRequest, request.getEntity());

        AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer("test_analyzer", "more text");
        assertThat(IndicesRequestConverters.analyze(analyzeRequest).getEndpoint(), equalTo("/_analyze"));
    }

    public void testIndicesExist() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 10);

        GetIndexRequest getIndexRequest = new GetIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest::local, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest::humanReadable, expectedParams);
        RequestConvertersTests.setRandomIncludeDefaults(getIndexRequest::includeDefaults, expectedParams);

        final Request request = IndicesRequestConverters.indicesExist(getIndexRequest);

        Assert.assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        Assert.assertEquals("/" + String.join(",", indices), request.getEndpoint());
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertNull(request.getEntity());
    }

    public void testIndicesExistEmptyIndices() {
        LuceneTestCase.expectThrows(IllegalArgumentException.class, ()
            -> IndicesRequestConverters.indicesExist(new GetIndexRequest()));
        LuceneTestCase.expectThrows(IllegalArgumentException.class, ()
            -> IndicesRequestConverters.indicesExist(new GetIndexRequest((String[]) null)));
    }

    public void testIndicesExistEmptyIndicesWithTypes() {
        LuceneTestCase.expectThrows(IllegalArgumentException.class,
                () -> IndicesRequestConverters.indicesExist(new org.havenask.action.admin.indices.get.GetIndexRequest()));
        LuceneTestCase.expectThrows(IllegalArgumentException.class, () -> IndicesRequestConverters
                .indicesExist(new org.havenask.action.admin.indices.get.GetIndexRequest().indices((String[]) null)));
    }

    public void testIndicesExistWithTypes() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 10);

        org.havenask.action.admin.indices.get.GetIndexRequest getIndexRequest =
                new org.havenask.action.admin.indices.get.GetIndexRequest().indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest::local, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest::humanReadable, expectedParams);
        RequestConvertersTests.setRandomIncludeDefaults(getIndexRequest::includeDefaults, expectedParams);
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");

        final Request request = IndicesRequestConverters.indicesExist(getIndexRequest);

        Assert.assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        Assert.assertEquals("/" + String.join(",", indices), request.getEndpoint());
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertNull(request.getEntity());
    }

    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = RandomCreateIndexGenerator.randomCreateIndexRequest();

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(createIndexRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(createIndexRequest, expectedParams);
        RequestConvertersTests.setRandomWaitForActiveShards(createIndexRequest::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.createIndex(createIndexRequest);
        Assert.assertEquals("/" + createIndexRequest.index(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(createIndexRequest, request.getEntity());
    }

    public void testCreateIndexWithTypes() throws IOException {
        org.havenask.action.admin.indices.create.CreateIndexRequest createIndexRequest =
            org.havenask.index.RandomCreateIndexGenerator.randomCreateIndexRequest();

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(createIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(createIndexRequest, expectedParams);
        RequestConvertersTests.setRandomWaitForActiveShards(createIndexRequest::waitForActiveShards, expectedParams);
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");

        Request request = IndicesRequestConverters.createIndex(createIndexRequest);
        Assert.assertEquals("/" + createIndexRequest.index(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(createIndexRequest, request.getEntity());
    }

    public void testCreateIndexNullIndex() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new CreateIndexRequest(null));
        assertEquals(e.getMessage(), "The index name cannot be null.");
    }

    public void testUpdateAliases() throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction = randomAliasAction();
        indicesAliasesRequest.addAliasAction(aliasAction);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(indicesAliasesRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(indicesAliasesRequest, expectedParams);

        Request request = IndicesRequestConverters.updateAliases(indicesAliasesRequest);
        Assert.assertEquals("/_aliases", request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        RequestConvertersTests.assertToXContentBody(indicesAliasesRequest, request.getEntity());
    }

    public void testPutMapping() throws IOException {
        String[] indices = RequestConvertersTests.randomIndicesNames(0, 5);
        PutMappingRequest putMappingRequest = new PutMappingRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(putMappingRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(putMappingRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(putMappingRequest::indicesOptions,
            putMappingRequest::indicesOptions, expectedParams);

        Request request = IndicesRequestConverters.putMapping(putMappingRequest);

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");

        Assert.assertEquals(endpoint.toString(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(putMappingRequest, request.getEntity());
    }

    public void testPutMappingWithTypes() throws IOException {
        org.havenask.action.admin.indices.mapping.put.PutMappingRequest putMappingRequest =
            new org.havenask.action.admin.indices.mapping.put.PutMappingRequest();

        String[] indices = RequestConvertersTests.randomIndicesNames(0, 5);
        putMappingRequest.indices(indices);

        String type = HavenaskTestCase.randomAlphaOfLengthBetween(3, 10);
        putMappingRequest.type(type);

        Map<String, String> expectedParams = new HashMap<>();

        RequestConvertersTests.setRandomTimeout(putMappingRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(putMappingRequest, expectedParams);
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");

        Request request = IndicesRequestConverters.putMapping(putMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        endpoint.add(type);
        Assert.assertEquals(endpoint.toString(), request.getEndpoint());

        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(putMappingRequest, request.getEntity());
    }

    public void testGetMapping() {
        GetMappingsRequest getMappingRequest = new GetMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getMappingRequest.indices(indices);
        } else if (randomBoolean()) {
            getMappingRequest.indices((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(getMappingRequest::indicesOptions,
            getMappingRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(getMappingRequest, expectedParams);
        RequestConvertersTests.setRandomLocal(getMappingRequest::local, expectedParams);

        Request request = IndicesRequestConverters.getMappings(getMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");

        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
    }

    public void testGetMappingWithTypes() {
        org.havenask.action.admin.indices.mapping.get.GetMappingsRequest getMappingRequest =
            new org.havenask.action.admin.indices.mapping.get.GetMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getMappingRequest.indices(indices);
        } else if (randomBoolean()) {
            getMappingRequest.indices((String[]) null);
        }

        String type = null;
        if (randomBoolean()) {
            type = randomAlphaOfLengthBetween(3, 10);
            getMappingRequest.types(type);
        } else if (randomBoolean()) {
            getMappingRequest.types((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();

        RequestConvertersTests.setRandomIndicesOptions(getMappingRequest::indicesOptions,
            getMappingRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(getMappingRequest, expectedParams);
        RequestConvertersTests.setRandomLocal(getMappingRequest::local, expectedParams);
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");

        Request request = IndicesRequestConverters.getMappings(getMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        if (type != null) {
            endpoint.add(type);
        }
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));

        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
    }

    public void testGetFieldMapping() {
        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getFieldMappingsRequest.indices(indices);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.indices((String[]) null);
        }

        String[] fields = null;
        if (randomBoolean()) {
            fields = new String[randomIntBetween(1, 5)];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = randomAlphaOfLengthBetween(3, 10);
            }
            getFieldMappingsRequest.fields(fields);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.fields((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(getFieldMappingsRequest::indicesOptions, getFieldMappingsRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.getFieldMapping(getFieldMappingsRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        endpoint.add("field");
        if (fields != null) {
            endpoint.add(String.join(",", fields));
        }
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
    }

    public void testGetFieldMappingWithTypes() {
        org.havenask.action.admin.indices.mapping.get.GetFieldMappingsRequest getFieldMappingsRequest =
            new org.havenask.action.admin.indices.mapping.get.GetFieldMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getFieldMappingsRequest.indices(indices);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.indices((String[]) null);
        }

        String type = null;
        if (randomBoolean()) {
            type = randomAlphaOfLengthBetween(3, 10);
            getFieldMappingsRequest.types(type);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.types((String[]) null);
        }

        String[] fields = null;
        if (randomBoolean()) {
            fields = new String[randomIntBetween(1, 5)];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = randomAlphaOfLengthBetween(3, 10);
            }
            getFieldMappingsRequest.fields(fields);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.fields((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();

        RequestConvertersTests.setRandomIndicesOptions(getFieldMappingsRequest::indicesOptions, getFieldMappingsRequest::indicesOptions,
            expectedParams);
        RequestConvertersTests.setRandomLocal(getFieldMappingsRequest::local, expectedParams);
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");

        Request request = IndicesRequestConverters.getFieldMapping(getFieldMappingsRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        if (type != null) {
            endpoint.add(type);
        }
        endpoint.add("field");
        if (fields != null) {
            endpoint.add(String.join(",", fields));
        }
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));

        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
    }

    public void testPutDataStream() {
        String name = randomAlphaOfLength(10);
        CreateDataStreamRequest createDataStreamRequest = new CreateDataStreamRequest(name);
        Request request = IndicesRequestConverters.putDataStream(createDataStreamRequest);
        Assert.assertEquals("/_data_stream/" + name, request.getEndpoint());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        Assert.assertNull(request.getEntity());
    }

    public void testGetDataStream() {
        String name = randomAlphaOfLength(10);
        GetDataStreamRequest getDataStreamRequest = new GetDataStreamRequest(name);
        Request request = IndicesRequestConverters.getDataStreams(getDataStreamRequest);
        Assert.assertEquals("/_data_stream/" + name, request.getEndpoint());
        Assert.assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        Assert.assertNull(request.getEntity());
    }

    public void testDeleteDataStream() {
        String name = randomAlphaOfLength(10);
        DeleteDataStreamRequest deleteDataStreamRequest = new DeleteDataStreamRequest(name);
        Request request = IndicesRequestConverters.deleteDataStream(deleteDataStreamRequest);
        Assert.assertEquals("/_data_stream/" + name, request.getEndpoint());
        Assert.assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        Assert.assertNull(request.getEntity());
    }

    public void testDeleteIndex() {
        String[] indices = RequestConvertersTests.randomIndicesNames(0, 5);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(deleteIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(deleteIndexRequest, expectedParams);

        RequestConvertersTests.setRandomIndicesOptions(deleteIndexRequest::indicesOptions, deleteIndexRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.deleteIndex(deleteIndexRequest);
        Assert.assertEquals("/" + String.join(",", indices), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        Assert.assertNull(request.getEntity());
    }

    public void testGetSettings() throws IOException {
        String[] indicesUnderTest = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getSettingsRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getSettingsRequest::indicesOptions, getSettingsRequest::indicesOptions,
            expectedParams);

        RequestConvertersTests.setRandomLocal(getSettingsRequest::local, expectedParams);

        if (HavenaskTestCase.randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getSettingsRequest.includeDefaults(HavenaskTestCase.randomBoolean());
            if (getSettingsRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (CollectionUtils.isEmpty(indicesUnderTest) == false) {
            endpoint.add(String.join(",", indicesUnderTest));
        }
        endpoint.add("_settings");

        if (HavenaskTestCase.randomBoolean()) {
            String[] names = HavenaskTestCase.randomBoolean() ? null : new String[HavenaskTestCase.randomIntBetween(0, 3)];
            if (names != null) {
                for (int x = 0; x < names.length; x++) {
                    names[x] = HavenaskTestCase.randomAlphaOfLengthBetween(3, 10);
                }
            }
            getSettingsRequest.names(names);
            if (CollectionUtils.isEmpty(names) == false) {
                endpoint.add(String.join(",", names));
            }
        }

        Request request = IndicesRequestConverters.getSettings(getSettingsRequest);

        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testGetIndex() throws IOException {
        String[] indicesUnderTest = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);

        GetIndexRequest getIndexRequest = new GetIndexRequest(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest::local, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest::humanReadable, expectedParams);

        if (HavenaskTestCase.randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getIndexRequest.includeDefaults(HavenaskTestCase.randomBoolean());
            if (getIndexRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indicesUnderTest != null && indicesUnderTest.length > 0) {
            endpoint.add(String.join(",", indicesUnderTest));
        }

        Request request = IndicesRequestConverters.getIndex(getIndexRequest);

        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testGetIndexWithTypes() throws IOException {
        String[] indicesUnderTest = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);

        org.havenask.action.admin.indices.get.GetIndexRequest getIndexRequest =
                new org.havenask.action.admin.indices.get.GetIndexRequest().indices(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest::local, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest::humanReadable, expectedParams);
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");

        if (HavenaskTestCase.randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getIndexRequest.includeDefaults(HavenaskTestCase.randomBoolean());
            if (getIndexRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indicesUnderTest != null && indicesUnderTest.length > 0) {
            endpoint.add(String.join(",", indicesUnderTest));
        }

        Request request = IndicesRequestConverters.getIndex(getIndexRequest);

        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteIndexEmptyIndices() {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new DeleteIndexRequest(indices).validate();
        Assert.assertNotNull(validationException);
    }

    public void testOpenIndex() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 5);
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indices);
        openIndexRequest.indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(openIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(openIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(openIndexRequest::indicesOptions, openIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomWaitForActiveShards(openIndexRequest::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.openIndex(openIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_open");
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testOpenIndexEmptyIndices() {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new OpenIndexRequest(indices).validate();
        Assert.assertNotNull(validationException);
    }

    public void testCloseIndex() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 5);
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(timeout -> closeIndexRequest.setTimeout(TimeValue.parseTimeValue(timeout, "test")),
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(closeIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(closeIndexRequest::indicesOptions, closeIndexRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.closeIndex(closeIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_close");
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testRefresh() {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        RefreshRequest refreshRequest;
        if (HavenaskTestCase.randomBoolean()) {
            refreshRequest = new RefreshRequest(indices);
        } else {
            refreshRequest = new RefreshRequest();
            refreshRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(refreshRequest::indicesOptions, refreshRequest::indicesOptions, expectedParams);
        Request request = IndicesRequestConverters.refresh(refreshRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_refresh");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testFlush() {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        FlushRequest flushRequest;
        if (HavenaskTestCase.randomBoolean()) {
            flushRequest = new FlushRequest(indices);
        } else {
            flushRequest = new FlushRequest();
            flushRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(flushRequest::indicesOptions, flushRequest::indicesOptions, expectedParams);
        if (HavenaskTestCase.randomBoolean()) {
            flushRequest.force(HavenaskTestCase.randomBoolean());
        }
        expectedParams.put("force", Boolean.toString(flushRequest.force()));
        if (HavenaskTestCase.randomBoolean()) {
            flushRequest.waitIfOngoing(HavenaskTestCase.randomBoolean());
        }
        expectedParams.put("wait_if_ongoing", Boolean.toString(flushRequest.waitIfOngoing()));

        Request request = IndicesRequestConverters.flush(flushRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_flush");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testSyncedFlush() {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        SyncedFlushRequest syncedFlushRequest;
        if (HavenaskTestCase.randomBoolean()) {
            syncedFlushRequest = new SyncedFlushRequest(indices);
        } else {
            syncedFlushRequest = new SyncedFlushRequest();
            syncedFlushRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(syncedFlushRequest::indicesOptions, syncedFlushRequest::indicesOptions,
            expectedParams);
        Request request = IndicesRequestConverters.flushSynced(syncedFlushRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
                endpoint.add(String.join(",", indices));
            }
        endpoint.add("_flush/synced");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testForceMerge() {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        ForceMergeRequest forceMergeRequest;
        if (HavenaskTestCase.randomBoolean()) {
            forceMergeRequest = new ForceMergeRequest(indices);
        } else {
            forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.indices(indices);
        }

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(forceMergeRequest::indicesOptions, forceMergeRequest::indicesOptions,
            expectedParams);
        if (HavenaskTestCase.randomBoolean()) {
            forceMergeRequest.maxNumSegments(HavenaskTestCase.randomInt());
        }
        expectedParams.put("max_num_segments", Integer.toString(forceMergeRequest.maxNumSegments()));
        if (HavenaskTestCase.randomBoolean()) {
            forceMergeRequest.onlyExpungeDeletes(HavenaskTestCase.randomBoolean());
        }
        expectedParams.put("only_expunge_deletes", Boolean.toString(forceMergeRequest.onlyExpungeDeletes()));
        if (HavenaskTestCase.randomBoolean()) {
            forceMergeRequest.flush(HavenaskTestCase.randomBoolean());
        }
        expectedParams.put("flush", Boolean.toString(forceMergeRequest.flush()));

        Request request = IndicesRequestConverters.forceMerge(forceMergeRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_forcemerge");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testClearCache() {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        ClearIndicesCacheRequest clearIndicesCacheRequest;
        if (HavenaskTestCase.randomBoolean()) {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest(indices);
        } else {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest();
            clearIndicesCacheRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(clearIndicesCacheRequest::indicesOptions, clearIndicesCacheRequest::indicesOptions,
            expectedParams);
        if (HavenaskTestCase.randomBoolean()) {
            clearIndicesCacheRequest.queryCache(HavenaskTestCase.randomBoolean());
        }
        expectedParams.put("query", Boolean.toString(clearIndicesCacheRequest.queryCache()));
        if (HavenaskTestCase.randomBoolean()) {
            clearIndicesCacheRequest.fieldDataCache(HavenaskTestCase.randomBoolean());
        }
        expectedParams.put("fielddata", Boolean.toString(clearIndicesCacheRequest.fieldDataCache()));
        if (HavenaskTestCase.randomBoolean()) {
            clearIndicesCacheRequest.requestCache(HavenaskTestCase.randomBoolean());
        }
        expectedParams.put("request", Boolean.toString(clearIndicesCacheRequest.requestCache()));
        if (HavenaskTestCase.randomBoolean()) {
            clearIndicesCacheRequest.fields(RequestConvertersTests.randomIndicesNames(1, 5));
            expectedParams.put("fields", String.join(",", clearIndicesCacheRequest.fields()));
        }

        Request request = IndicesRequestConverters.clearCache(clearIndicesCacheRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_cache/clear");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testExistsAlias() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        getAliasesRequest.indices(indices);
        // the HEAD endpoint requires at least an alias or an index
        boolean hasIndices = indices != null && indices.length > 0;
        String[] aliases;
        if (hasIndices) {
            aliases = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        } else {
            aliases = RequestConvertersTests.randomIndicesNames(1, 5);
        }
        getAliasesRequest.aliases(aliases);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomLocal(getAliasesRequest::local, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getAliasesRequest::indicesOptions, getAliasesRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.existsAlias(getAliasesRequest);
        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            expectedEndpoint.add(String.join(",", indices));
        }
        expectedEndpoint.add("_alias");
        if (aliases != null && aliases.length > 0) {
            expectedEndpoint.add(String.join(",", aliases));
        }
        Assert.assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        Assert.assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertNull(request.getEntity());
    }

    public void testExistsAliasNoAliasNoIndex() {
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
            IllegalArgumentException iae = LuceneTestCase.expectThrows(IllegalArgumentException.class,
                    () -> IndicesRequestConverters.existsAlias(getAliasesRequest));
            Assert.assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest((String[]) null);
            getAliasesRequest.indices((String[]) null);
            IllegalArgumentException iae = LuceneTestCase.expectThrows(IllegalArgumentException.class,
                    () -> IndicesRequestConverters.existsAlias(getAliasesRequest));
            Assert.assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
    }

    public void testSplit() throws IOException {
        resizeTest(ResizeType.SPLIT, IndicesRequestConverters::split);
    }

    public void testClone() throws IOException {
        resizeTest(ResizeType.CLONE, IndicesRequestConverters::clone);
    }

    public void testShrink() throws IOException {
        resizeTest(ResizeType.SHRINK, IndicesRequestConverters::shrink);
    }

    private void resizeTest(ResizeType resizeType, CheckedFunction<ResizeRequest, Request, IOException> function)
            throws IOException {
        String[] indices = RequestConvertersTests.randomIndicesNames(2, 2);
        ResizeRequest resizeRequest = new ResizeRequest(indices[0], indices[1]);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(resizeRequest, expectedParams);
        RequestConvertersTests.setRandomTimeout(s -> resizeRequest.setTimeout(TimeValue.parseTimeValue(s, "timeout")),
            resizeRequest.timeout(), expectedParams);

        if (HavenaskTestCase.randomBoolean()) {
            if (HavenaskTestCase.randomBoolean()) {
                resizeRequest.setSettings(randomIndexSettings());
            }
            if (HavenaskTestCase.randomBoolean()) {
                int count = randomIntBetween(0, 2);
                for (int i = 0; i < count; i++) {
                    resizeRequest.setAliases(singletonList(randomAlias()));
                }
            }
        }
        RequestConvertersTests.setRandomWaitForActiveShards(resizeRequest::setWaitForActiveShards, expectedParams);
        if (resizeType == ResizeType.SPLIT) {
            resizeRequest.setSettings(Settings.builder().put("index.number_of_shards", 2).build());
        }

        Request request = function.apply(resizeRequest);
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        String expectedEndpoint = "/" + resizeRequest.getSourceIndex() + "/_" + resizeType.name().toLowerCase(Locale.ROOT) + "/"
                + resizeRequest.getTargetIndex();
        Assert.assertEquals(expectedEndpoint, request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        RequestConvertersTests.assertToXContentBody(resizeRequest, request.getEntity());
    }

    public void testRollover() throws IOException {
        RolloverRequest rolloverRequest = new RolloverRequest(HavenaskTestCase.randomAlphaOfLengthBetween(3, 10),
                HavenaskTestCase.randomBoolean() ? null : HavenaskTestCase.randomAlphaOfLengthBetween(3, 10));
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(rolloverRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(rolloverRequest, expectedParams);
        if (HavenaskTestCase.randomBoolean()) {
            rolloverRequest.dryRun(HavenaskTestCase.randomBoolean());
            if (rolloverRequest.isDryRun()) {
                expectedParams.put("dry_run", "true");
            }
        }
        if (HavenaskTestCase.randomBoolean()) {
            rolloverRequest.addMaxIndexAgeCondition(new TimeValue(HavenaskTestCase.randomNonNegativeLong()));
        }
        if (HavenaskTestCase.randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().mapping(randomMapping());
        }
        if (HavenaskTestCase.randomBoolean()) {
            randomAliases(rolloverRequest.getCreateIndexRequest());
        }
        if (HavenaskTestCase.randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().settings(
                org.havenask.index.RandomCreateIndexGenerator.randomIndexSettings());
        }
        RequestConvertersTests.setRandomWaitForActiveShards(rolloverRequest.getCreateIndexRequest()::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.rollover(rolloverRequest);
        if (rolloverRequest.getNewIndexName() == null) {
            Assert.assertEquals("/" + rolloverRequest.getAlias() + "/_rollover", request.getEndpoint());
        } else {
            Assert.assertEquals("/" + rolloverRequest.getAlias() + "/_rollover/" + rolloverRequest.getNewIndexName(),
                request.getEndpoint());
        }
        Assert.assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(rolloverRequest, request.getEntity());
        Assert.assertEquals(expectedParams, request.getParameters());
    }

    public void testRolloverWithTypes() throws IOException {
        org.havenask.action.admin.indices.rollover.RolloverRequest rolloverRequest =
            new org.havenask.action.admin.indices.rollover.RolloverRequest(HavenaskTestCase.randomAlphaOfLengthBetween(3, 10),
            HavenaskTestCase.randomBoolean() ? null : HavenaskTestCase.randomAlphaOfLengthBetween(3, 10));
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(rolloverRequest::timeout, rolloverRequest.timeout(), expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(rolloverRequest, expectedParams);
        if (HavenaskTestCase.randomBoolean()) {
            rolloverRequest.dryRun(HavenaskTestCase.randomBoolean());
            if (rolloverRequest.isDryRun()) {
                expectedParams.put("dry_run", "true");
            }
        }
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");
        if (HavenaskTestCase.randomBoolean()) {
            rolloverRequest.addMaxIndexAgeCondition(new TimeValue(HavenaskTestCase.randomNonNegativeLong()));
        }
        if (HavenaskTestCase.randomBoolean()) {
            String type = HavenaskTestCase.randomAlphaOfLengthBetween(3, 10);
            rolloverRequest.getCreateIndexRequest().mapping(type,
                org.havenask.index.RandomCreateIndexGenerator.randomMapping(type));
        }
        if (HavenaskTestCase.randomBoolean()) {
            org.havenask.index.RandomCreateIndexGenerator.randomAliases(rolloverRequest.getCreateIndexRequest());
        }
        if (HavenaskTestCase.randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().settings(
                org.havenask.index.RandomCreateIndexGenerator.randomIndexSettings());
        }
        RequestConvertersTests.setRandomWaitForActiveShards(rolloverRequest.getCreateIndexRequest()::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.rollover(rolloverRequest);
        if (rolloverRequest.getNewIndexName() == null) {
            Assert.assertEquals("/" + rolloverRequest.getRolloverTarget() + "/_rollover", request.getEndpoint());
        } else {
            Assert.assertEquals("/" + rolloverRequest.getRolloverTarget() + "/_rollover/" + rolloverRequest.getNewIndexName(),
                request.getEndpoint());
        }
        Assert.assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(rolloverRequest, request.getEntity());
        Assert.assertEquals(expectedParams, request.getParameters());
    }

    public void testGetAlias() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomLocal(getAliasesRequest::local, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getAliasesRequest::indicesOptions, getAliasesRequest::indicesOptions,
            expectedParams);

        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 2);
        String[] aliases = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 2);
        getAliasesRequest.indices(indices);
        getAliasesRequest.aliases(aliases);

        Request request = IndicesRequestConverters.getAlias(getAliasesRequest);
        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "");

        if (false == CollectionUtils.isEmpty(indices)) {
            expectedEndpoint.add(String.join(",", indices));
        }
        expectedEndpoint.add("_alias");

        if (false == CollectionUtils.isEmpty(aliases)) {
            expectedEndpoint.add(String.join(",", aliases));
        }

        Assert.assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        Assert.assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertNull(request.getEntity());
    }

    public void testIndexPutSettings() throws IOException {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 2);
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indices);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(updateSettingsRequest, expectedParams);
        RequestConvertersTests.setRandomTimeout(updateSettingsRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(updateSettingsRequest::indicesOptions, updateSettingsRequest::indicesOptions,
            expectedParams);
        if (HavenaskTestCase.randomBoolean()) {
            updateSettingsRequest.setPreserveExisting(HavenaskTestCase.randomBoolean());
            if (updateSettingsRequest.isPreserveExisting()) {
                expectedParams.put("preserve_existing", "true");
            }
        }

        Request request = IndicesRequestConverters.indexPutSettings(updateSettingsRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_settings");
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(updateSettingsRequest, request.getEntity());
        Assert.assertEquals(expectedParams, request.getParameters());
    }

    public void testPutTemplateRequestWithTypes() throws Exception {
        Map<String, String> names = new HashMap<>();
        names.put("log", "log");
        names.put("template#1", "template%231");
        names.put("-#template", "-%23template");
        names.put("foo^bar", "foo%5Ebar");

        org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest putTemplateRequest =
                new org.havenask.action.admin.indices.template.put.PutIndexTemplateRequest()
                .name(HavenaskTestCase.randomFrom(names.keySet()))
                .patterns(Arrays.asList(HavenaskTestCase.generateRandomStringArray(20, 100, false, false)));
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.order(HavenaskTestCase.randomInt());
        }
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.version(HavenaskTestCase.randomInt());
        }
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.settings(
                Settings.builder().put("setting-" + HavenaskTestCase.randomInt(), HavenaskTestCase.randomTimeValue()));
        }
        Map<String, String> expectedParams = new HashMap<>();
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.mapping("doc-" + HavenaskTestCase.randomInt(),
                "field-" + HavenaskTestCase.randomInt(), "type=" + HavenaskTestCase.randomFrom("text", "keyword"));
        }
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.alias(new Alias("alias-" + HavenaskTestCase.randomInt()));
        }
        if (HavenaskTestCase.randomBoolean()) {
            expectedParams.put("create", Boolean.TRUE.toString());
            putTemplateRequest.create(true);
        }
        if (HavenaskTestCase.randomBoolean()) {
            String cause = HavenaskTestCase.randomUnicodeOfCodepointLengthBetween(1, 50);
            putTemplateRequest.cause(cause);
            expectedParams.put("cause", cause);
        }
        RequestConvertersTests.setRandomMasterTimeout(putTemplateRequest, expectedParams);

        Request request = IndicesRequestConverters.putTemplate(putTemplateRequest);
        Assert.assertThat(request.getEndpoint(), equalTo("/_template/" + names.get(putTemplateRequest.name())));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        RequestConvertersTests.assertToXContentBody(putTemplateRequest, request.getEntity());
    }

    public void testPutTemplateRequest() throws Exception {
        Map<String, String> names = new HashMap<>();
        names.put("log", "log");
        names.put("template#1", "template%231");
        names.put("-#template", "-%23template");
        names.put("foo^bar", "foo%5Ebar");

        PutIndexTemplateRequest putTemplateRequest =
                new PutIndexTemplateRequest(HavenaskTestCase.randomFrom(names.keySet()))
                .patterns(Arrays.asList(HavenaskTestCase.generateRandomStringArray(20, 100, false, false)));
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.order(HavenaskTestCase.randomInt());
        }
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.version(HavenaskTestCase.randomInt());
        }
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.settings(
                Settings.builder().put("setting-" + HavenaskTestCase.randomInt(), HavenaskTestCase.randomTimeValue()));
        }
        Map<String, String> expectedParams = new HashMap<>();
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.mapping("{ \"properties\": { \"field-" + HavenaskTestCase.randomInt() +
                    "\" : { \"type\" : \"" + HavenaskTestCase.randomFrom("text", "keyword") + "\" }}}", XContentType.JSON);
        }
        if (HavenaskTestCase.randomBoolean()) {
            putTemplateRequest.alias(new Alias("alias-" + HavenaskTestCase.randomInt()));
        }
        if (HavenaskTestCase.randomBoolean()) {
            expectedParams.put("create", Boolean.TRUE.toString());
            putTemplateRequest.create(true);
        }
        if (HavenaskTestCase.randomBoolean()) {
            String cause = HavenaskTestCase.randomUnicodeOfCodepointLengthBetween(1, 50);
            putTemplateRequest.cause(cause);
            expectedParams.put("cause", cause);
        }
        RequestConvertersTests.setRandomMasterTimeout(putTemplateRequest, expectedParams);

        Request request = IndicesRequestConverters.putTemplate(putTemplateRequest);
        Assert.assertThat(request.getEndpoint(), equalTo("/_template/" + names.get(putTemplateRequest.name())));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        RequestConvertersTests.assertToXContentBody(putTemplateRequest, request.getEntity());
    }
    public void testValidateQuery() throws Exception {
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        String[] types = HavenaskTestCase.randomBoolean() ? HavenaskTestCase.generateRandomStringArray(5, 5, false, false) : null;
        ValidateQueryRequest validateQueryRequest;
        if (HavenaskTestCase.randomBoolean()) {
            validateQueryRequest = new ValidateQueryRequest(indices);
        } else {
            validateQueryRequest = new ValidateQueryRequest();
            validateQueryRequest.indices(indices);
        }
        validateQueryRequest.types(types);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(validateQueryRequest::indicesOptions, validateQueryRequest::indicesOptions,
            expectedParams);
        validateQueryRequest.explain(HavenaskTestCase.randomBoolean());
        validateQueryRequest.rewrite(HavenaskTestCase.randomBoolean());
        validateQueryRequest.allShards(HavenaskTestCase.randomBoolean());
        expectedParams.put("explain", Boolean.toString(validateQueryRequest.explain()));
        expectedParams.put("rewrite", Boolean.toString(validateQueryRequest.rewrite()));
        expectedParams.put("all_shards", Boolean.toString(validateQueryRequest.allShards()));
        Request request = IndicesRequestConverters.validateQuery(validateQueryRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
            if (types != null && types.length > 0) {
                endpoint.add(String.join(",", types));
            }
        }
        endpoint.add("_validate/query");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        RequestConvertersTests.assertToXContentBody(validateQueryRequest, request.getEntity());
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
    }

    public void testGetTemplateRequest() throws Exception {
        Map<String, String> encodes = new HashMap<>();
        encodes.put("log", "log");
        encodes.put("1", "1");
        encodes.put("template#1", "template%231");
        encodes.put("template-*", "template-*");
        encodes.put("foo^bar", "foo%5Ebar");
        List<String> names = HavenaskTestCase.randomSubsetOf(1, encodes.keySet());
        GetIndexTemplatesRequest getTemplatesRequest = new GetIndexTemplatesRequest(names);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getTemplatesRequest::setMasterNodeTimeout, expectedParams);
        RequestConvertersTests.setRandomLocal(getTemplatesRequest::setLocal, expectedParams);

        Request request = IndicesRequestConverters.getTemplatesWithDocumentTypes(getTemplatesRequest);
        expectedParams.put(INCLUDE_TYPE_NAME_PARAMETER, "true");
        Assert.assertThat(request.getEndpoint(),
            equalTo("/_template/" + names.stream().map(encodes::get).collect(Collectors.joining(","))));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());

        expectThrows(NullPointerException.class, () -> new GetIndexTemplatesRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new GetIndexTemplatesRequest((List<String>) null));
        expectThrows(IllegalArgumentException.class, () -> new GetIndexTemplatesRequest(singletonList(randomBoolean() ? "" : null)));
        expectThrows(IllegalArgumentException.class, () -> new GetIndexTemplatesRequest(new String[] { (randomBoolean() ? "" : null) }));
    }

    public void testTemplatesExistRequest() {
        final int numberOfNames = HavenaskTestCase.usually()
            ? 1
            : HavenaskTestCase.randomIntBetween(2, 20);
        final List<String> names = Arrays.asList(HavenaskTestCase.randomArray(numberOfNames, numberOfNames, String[]::new,
            () -> HavenaskTestCase.randomAlphaOfLengthBetween(1, 100)));
        final Map<String, String> expectedParams = new HashMap<>();
        final IndexTemplatesExistRequest indexTemplatesExistRequest = new IndexTemplatesExistRequest(names);
        RequestConvertersTests.setRandomMasterTimeout(indexTemplatesExistRequest::setMasterNodeTimeout, expectedParams);
        RequestConvertersTests.setRandomLocal(indexTemplatesExistRequest::setLocal, expectedParams);
        assertThat(indexTemplatesExistRequest.names(), equalTo(names));

        final Request request = IndicesRequestConverters.templatesExist(indexTemplatesExistRequest);
        assertThat(request.getMethod(), equalTo(HttpHead.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_template/" + String.join(",", names)));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());

        expectThrows(NullPointerException.class, () -> new IndexTemplatesExistRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new IndexTemplatesExistRequest((List<String>) null));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(new String[] { (randomBoolean() ? "" : null) }));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(singletonList(randomBoolean() ? "" : null)));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(new String[] {}));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(emptyList()));
    }

    public void testDeleteTemplateRequest() {
        Map<String, String> encodes = new HashMap<>();
        encodes.put("log", "log");
        encodes.put("1", "1");
        encodes.put("template#1", "template%231");
        encodes.put("template-*", "template-*");
        encodes.put("foo^bar", "foo%5Ebar");
        DeleteIndexTemplateRequest deleteTemplateRequest = new DeleteIndexTemplateRequest().name(randomFrom(encodes.keySet()));
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(deleteTemplateRequest, expectedParams);
        Request request = IndicesRequestConverters.deleteTemplate(deleteTemplateRequest);
        Assert.assertThat(request.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        Assert.assertThat(request.getEndpoint(), equalTo("/_template/" + encodes.get(deleteTemplateRequest.name())));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteAlias() {
        DeleteAliasRequest deleteAliasRequest = new DeleteAliasRequest(randomAlphaOfLength(4), randomAlphaOfLength(4));

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(deleteAliasRequest, expectedParams);
        RequestConvertersTests.setRandomTimeout(deleteAliasRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request request = IndicesRequestConverters.deleteAlias(deleteAliasRequest);
        Assert.assertThat(request.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        Assert.assertThat(request.getEndpoint(), equalTo("/" + deleteAliasRequest.getIndex() + "/_alias/" + deleteAliasRequest.getAlias()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
    }
}
