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

package org.havenask.ingest;

import org.havenask.HavenaskException;
import org.havenask.HavenaskParseException;
import org.havenask.ExceptionsHelper;
import org.havenask.action.DocWriteResponse;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.bulk.BulkResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.index.IndexResponse;
import org.havenask.action.ingest.DeletePipelineRequest;
import org.havenask.action.ingest.GetPipelineRequest;
import org.havenask.action.ingest.GetPipelineResponse;
import org.havenask.action.ingest.PutPipelineRequest;
import org.havenask.action.ingest.SimulateDocumentBaseResult;
import org.havenask.action.ingest.SimulatePipelineRequest;
import org.havenask.action.ingest.SimulatePipelineResponse;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.action.update.UpdateRequest;
import org.havenask.client.Requests;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentType;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.test.NodeRoles.nonIngestNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@HavenaskIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class IngestClientIT extends HavenaskIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (nodeOrdinal % 2 == 0) {
            return Settings.builder().put(nonIngestNode()).put(super.nodeSettings(nodeOrdinal)).build();
        }
        return super.nodeSettings(nodeOrdinal);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ExtendedIngestTestPlugin.class);
    }

    public void testSimulate() throws Exception {
        BytesReference pipelineSource = BytesReference.bytes(jsonBuilder().startObject()
            .field("description", "my_pipeline")
            .startArray("processors")
            .startObject()
            .startObject("test")
            .endObject()
            .endObject()
            .endArray()
            .endObject());
        client().admin().cluster().preparePutPipeline("_id", pipelineSource, XContentType.JSON)
                .get();
        GetPipelineResponse getResponse = client().admin().cluster().prepareGetPipeline("_id")
                .get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        BytesReference bytes = BytesReference.bytes(jsonBuilder().startObject()
            .startArray("docs")
            .startObject()
            .field("_index", "index")
            .field("_type", "type")
            .field("_id", "id")
            .startObject("_source")
            .field("foo", "bar")
            .field("fail", false)
            .endObject()
            .endObject()
            .endArray()
            .endObject());
        SimulatePipelineResponse response;
        if (randomBoolean()) {
            response = client().admin().cluster().prepareSimulatePipeline(bytes, XContentType.JSON)
                .setId("_id").get();
        } else {
            SimulatePipelineRequest request = new SimulatePipelineRequest(bytes, XContentType.JSON);
            request.setId("_id");
            response = client().admin().cluster().simulatePipeline(request).get();
        }
        assertThat(response.isVerbose(), equalTo(false));
        assertThat(response.getPipelineId(), equalTo("_id"));
        assertThat(response.getResults().size(), equalTo(1));
        assertThat(response.getResults().get(0), instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) response.getResults().get(0);
        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
        source.put("fail", false);
        source.put("processed", true);
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", null, null, null, source);
        assertThat(simulateDocumentBaseResult.getIngestDocument().getSourceAndMetadata(), equalTo(ingestDocument.getSourceAndMetadata()));
        assertThat(simulateDocumentBaseResult.getFailure(), nullValue());

        // cleanup
        AcknowledgedResponse deletePipelineResponse = client().admin().cluster().prepareDeletePipeline("_id").get();
        assertTrue(deletePipelineResponse.isAcknowledged());
    }

    public void testBulkWithIngestFailures() throws Exception {
        createIndex("index");

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
            .field("description", "my_pipeline")
            .startArray("processors")
            .startObject()
            .startObject("test")
            .endObject()
            .endObject()
            .endArray()
            .endObject());
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        int numRequests = scaledRandomIntBetween(32, 128);
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("index", "type", Integer.toString(i)).setPipeline("_id");
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field", "value", "fail", i % 2 == 0);
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        for (int i = 0; i < bulkRequest.requests().size(); i++) {
            BulkItemResponse itemResponse = response.getItems()[i];
            if (i % 2 == 0) {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                HavenaskException compoundProcessorException = (HavenaskException) failure.getCause();
                assertThat(compoundProcessorException.getRootCause().getMessage(), equalTo("test processor failed"));
            } else {
                IndexResponse indexResponse = itemResponse.getResponse();
                assertThat("Expected a successful response but found failure [" + itemResponse.getFailure() + "].",
                    itemResponse.isFailed(), is(false));
                assertThat(indexResponse, notNullValue());
                assertThat(indexResponse.getId(), equalTo(Integer.toString(i)));
                assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
            }
        }

        // cleanup
        AcknowledgedResponse deletePipelineResponse = client().admin().cluster().prepareDeletePipeline("_id").get();
        assertTrue(deletePipelineResponse.isAcknowledged());
    }

    public void testBulkWithUpsert() throws Exception {
        createIndex("index");

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
            .field("description", "my_pipeline")
            .startArray("processors")
            .startObject()
            .startObject("test")
            .endObject()
            .endObject()
            .endArray()
            .endObject());
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("index", "type", "1").setPipeline("_id");
        indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "val1");
        bulkRequest.add(indexRequest);
        UpdateRequest updateRequest = new UpdateRequest("index", "type", "2");
        updateRequest.doc("{}", Requests.INDEX_CONTENT_TYPE);
        updateRequest.upsert("{\"field1\":\"upserted_val\"}", XContentType.JSON).upsertRequest().setPipeline("_id");
        bulkRequest.add(updateRequest);

        BulkResponse response = client().bulk(bulkRequest).actionGet();

        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        Map<String, Object> inserted = client().prepareGet("index", "type", "1")
            .get().getSourceAsMap();
        assertThat(inserted.get("field1"), equalTo("val1"));
        assertThat(inserted.get("processed"), equalTo(true));
        Map<String, Object> upserted = client().prepareGet("index", "type", "2")
            .get().getSourceAsMap();
        assertThat(upserted.get("field1"), equalTo("upserted_val"));
        assertThat(upserted.get("processed"), equalTo(true));
    }

    public void test() throws Exception {
        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
            .field("description", "my_pipeline")
            .startArray("processors")
            .startObject()
            .startObject("test")
            .endObject()
            .endObject()
            .endArray()
            .endObject());
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        GetPipelineRequest getPipelineRequest = new GetPipelineRequest("_id");
        GetPipelineResponse getResponse = client().admin().cluster().getPipeline(getPipelineRequest).get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        client().prepareIndex("test", "type", "1").setPipeline("_id").setSource("field", "value", "fail", false).get();

        Map<String, Object> doc = client().prepareGet("test", "type", "1")
                .get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value"));
        assertThat(doc.get("processed"), equalTo(true));

        client().prepareBulk().add(
                client().prepareIndex("test", "type", "2").setSource("field", "value2", "fail", false).setPipeline("_id")).get();
        doc = client().prepareGet("test", "type", "2").get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value2"));
        assertThat(doc.get("processed"), equalTo(true));

        DeletePipelineRequest deletePipelineRequest = new DeletePipelineRequest("_id");
        AcknowledgedResponse response = client().admin().cluster().deletePipeline(deletePipelineRequest).get();
        assertThat(response.isAcknowledged(), is(true));

        getResponse = client().admin().cluster().prepareGetPipeline("_id").get();
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.pipelines().size(), equalTo(0));
    }

    public void testPutWithPipelineFactoryError() throws Exception {
        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .field("unused", ":sad_face:")
                .endObject()
                .endObject()
                .endArray()
                .endObject());
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id2", source, XContentType.JSON);
        Exception e = expectThrows(HavenaskParseException.class,
            () -> client().admin().cluster().putPipeline(putPipelineRequest).actionGet());
        assertThat(e.getMessage(), equalTo("processor [test] doesn't support one or more provided configuration parameters [unused]"));

        GetPipelineResponse response = client().admin().cluster().prepareGetPipeline("_id2").get();
        assertFalse(response.isFound());
    }

    public void testWithDedicatedMaster() throws Exception {
        String masterOnlyNode = internalCluster().startMasterOnlyNode();
        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
            .field("description", "my_pipeline")
            .startArray("processors")
            .startObject()
            .startObject("test")
            .endObject()
            .endObject()
            .endArray()
            .endObject());
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, XContentType.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        BulkItemResponse item = client(masterOnlyNode).prepareBulk().add(
            client().prepareIndex("test", "type").setSource("field", "value2", "drop", true).setPipeline("_id")).get()
            .getItems()[0];
        assertFalse(item.isFailed());
        assertEquals("auto-generated", item.getResponse().getId());
    }

    public void testPipelineOriginHeader() throws Exception {
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "2");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest =
                new PutPipelineRequest("1", BytesReference.bytes(source), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "3");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest =
                new PutPipelineRequest("2", BytesReference.bytes(source), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("fail");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest =
                new PutPipelineRequest("3", BytesReference.bytes(source), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }

        Exception e = expectThrows(Exception.class, () -> {
            IndexRequest indexRequest = new IndexRequest("test");
            indexRequest.source("{}", XContentType.JSON);
            indexRequest.setPipeline("1");
            client().index(indexRequest).get();
        });
        IngestProcessorException ingestException = (IngestProcessorException) ExceptionsHelper.unwrap(e, IngestProcessorException.class);
        assertThat(ingestException.getHeader("processor_type"), equalTo(Collections.singletonList("fail")));
        assertThat(ingestException.getHeader("pipeline_origin"), equalTo(Arrays.asList("3", "2", "1")));
    }

    public void testPipelineProcessorOnFailure() throws Exception {
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "2");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            {
                source.startArray("on_failure");
                source.startObject();
                {
                    source.startObject("onfailure_processor");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest =
                new PutPipelineRequest("1", BytesReference.bytes(source), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "3");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest =
                new PutPipelineRequest("2", BytesReference.bytes(source), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("fail");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest =
                new PutPipelineRequest("3", BytesReference.bytes(source), XContentType.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }

        client().prepareIndex("test", "_doc").setId("1").setSource("{}", XContentType.JSON).setPipeline("1").get();
        Map<String, Object> inserted = client().prepareGet("test", "_doc", "1")
            .get().getSourceAsMap();
        assertThat(inserted.get("readme"), equalTo("pipeline with id [3] is a bad pipeline"));
    }

    public static class ExtendedIngestTestPlugin extends IngestTestPlugin {

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> factories = new HashMap<>(super.getProcessors(parameters));
            factories.put(PipelineProcessor.TYPE, new PipelineProcessor.Factory(parameters.ingestService));
            factories.put("fail", (processorFactories, tag, description, config) ->
                new TestProcessor(tag, "fail", description, new RuntimeException()));
            factories.put("onfailure_processor", (processorFactories, tag, description, config) -> new TestProcessor(tag, "fail",
                description, document -> {
                String onFailurePipeline = document.getFieldValue("_ingest.on_failure_pipeline", String.class);
                document.setFieldValue("readme", "pipeline with id [" + onFailurePipeline + "] is a bad pipeline");
            }));
            return factories;
        }
    }

}
