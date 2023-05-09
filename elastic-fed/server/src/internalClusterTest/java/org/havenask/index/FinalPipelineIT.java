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

package org.havenask.index;

import org.havenask.action.get.GetRequestBuilder;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.action.index.IndexResponse;
import org.havenask.action.ingest.DeletePipelineRequest;
import org.havenask.action.ingest.GetPipelineRequest;
import org.havenask.action.ingest.GetPipelineResponse;
import org.havenask.action.ingest.PutPipelineRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.support.WriteRequest;
import org.havenask.client.Client;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentType;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.ingest.AbstractProcessor;
import org.havenask.ingest.IngestDocument;
import org.havenask.ingest.PipelineConfiguration;
import org.havenask.ingest.Processor;
import org.havenask.plugins.IngestPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.RepositoriesService;
import org.havenask.rest.RestStatus;
import org.havenask.script.ScriptService;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.threadpool.ThreadPool;
import org.havenask.watcher.ResourceWatcherService;

import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasToString;

public class FinalPipelineIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    @After
    public void cleanUpPipelines() {
        final GetPipelineResponse response = client().admin()
            .cluster()
            .getPipeline(new GetPipelineRequest("default_pipeline", "final_pipeline", "request_pipeline"))
            .actionGet();
        for (final PipelineConfiguration pipeline : response.pipelines()) {
            client().admin().cluster().deletePipeline(new DeletePipelineRequest(pipeline.getId())).actionGet();
        }
    }

    public void testFinalPipelineCantChangeDestination(){
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);

        final BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"changing_dest\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();

        final IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> client().prepareIndex("index", "_doc").setId("1").setSource(Collections.singletonMap("field", "value")).get());
        assertThat(e, hasToString(containsString("final pipeline [final_pipeline] can't change the target index")));
    }

    public void testFinalPipelineOfOldDestinationIsNotInvoked(){
        Settings settings = Settings.builder()
            .put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline")
            .put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline")
            .build();
        createIndex("index", settings);

        BytesReference defaultPipelineBody = new BytesArray("{\"processors\": [{\"changing_dest\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("default_pipeline", defaultPipelineBody, XContentType.JSON))
            .actionGet();

        BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {\"exists\":\"no_such_field\"}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();

        IndexResponse indexResponse = client()
            .prepareIndex("index", "_doc")
            .setId("1")
            .setSource(Collections.singletonMap("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        SearchResponse target = client().prepareSearch("target").get();
        assertEquals(1, target.getHits().getTotalHits().value);
        assertFalse(target.getHits().getAt(0).getSourceAsMap().containsKey("final"));
    }

    public void testFinalPipelineOfNewDestinationIsInvoked(){
        Settings settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        createIndex("index", settings);

        settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("target", settings);

        BytesReference defaultPipelineBody = new BytesArray("{\"processors\": [{\"changing_dest\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("default_pipeline", defaultPipelineBody, XContentType.JSON))
            .actionGet();

        BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();

        IndexResponse indexResponse = client()
            .prepareIndex("index", "_doc")
            .setId("1")
            .setSource(Collections.singletonMap("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        SearchResponse target = client().prepareSearch("target").get();
        assertEquals(1, target.getHits().getTotalHits().value);
        assertEquals(true, target.getHits().getAt(0).getSourceAsMap().get("final"));
    }

    public void testDefaultPipelineOfNewDestinationIsNotInvoked(){
        Settings settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        createIndex("index", settings);

        settings = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "target_default_pipeline").build();
        createIndex("target", settings);

        BytesReference defaultPipelineBody = new BytesArray("{\"processors\": [{\"changing_dest\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("default_pipeline", defaultPipelineBody, XContentType.JSON))
            .actionGet();

        BytesReference targetPipeline = new BytesArray("{\"processors\": [{\"final\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("target_default_pipeline", targetPipeline, XContentType.JSON))
            .actionGet();

        IndexResponse indexResponse = client()
            .prepareIndex("index", "_doc")
            .setId("1")
            .setSource(Collections.singletonMap("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        SearchResponse target = client().prepareSearch("target").get();
        assertEquals(1, target.getHits().getTotalHits().value);
        assertFalse(target.getHits().getAt(0).getSourceAsMap().containsKey("final"));
    }

    public void testFinalPipeline() {
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);

        // this asserts that the final_pipeline was used, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("index", "_doc", "1").setSource(Collections.singletonMap("field", "value")).get());
        assertThat(e, hasToString(containsString("pipeline with id [final_pipeline] does not exist")));
    }

    public void testRequestPipelineAndFinalPipeline() {
        final BytesReference requestPipelineBody = new BytesArray("{\"processors\": [{\"request\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("request_pipeline", requestPipelineBody, XContentType.JSON))
            .actionGet();
        final BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {\"exists\":\"request\"}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();
        final Settings settings = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        createIndex("index", settings);
        final IndexRequestBuilder index = client().prepareIndex("index", "_doc", "1");
        index.setSource(Collections.singletonMap("field", "value"));
        index.setPipeline("request_pipeline");
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final IndexResponse response = index.get();
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        final GetRequestBuilder get = client().prepareGet("index", "_doc", "1");
        final GetResponse getResponse = get.get();
        assertTrue(getResponse.isExists());
        final Map<String, Object> source = getResponse.getSourceAsMap();
        assertThat(source, hasKey("request"));
        assertTrue((boolean) source.get("request"));
        assertThat(source, hasKey("final"));
        assertTrue((boolean) source.get("final"));
    }

    public void testDefaultAndFinalPipeline() {
        final BytesReference defaultPipelineBody = new BytesArray("{\"processors\": [{\"default\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("default_pipeline", defaultPipelineBody, XContentType.JSON))
            .actionGet();
        final BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {\"exists\":\"default\"}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();
        final Settings settings = Settings.builder()
            .put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline")
            .put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline")
            .build();
        createIndex("index", settings);
        final IndexRequestBuilder index = client().prepareIndex("index", "_doc", "1");
        index.setSource(Collections.singletonMap("field", "value"));
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final IndexResponse response = index.get();
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        final GetRequestBuilder get = client().prepareGet("index", "_doc", "1");
        final GetResponse getResponse = get.get();
        assertTrue(getResponse.isExists());
        final Map<String, Object> source = getResponse.getSourceAsMap();
        assertThat(source, hasKey("default"));
        assertTrue((boolean) source.get("default"));
        assertThat(source, hasKey("final"));
        assertTrue((boolean) source.get("final"));
    }

    public void testDefaultAndFinalPipelineFromTemplates() {
        final BytesReference defaultPipelineBody = new BytesArray("{\"processors\": [{\"default\": {}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("default_pipeline", defaultPipelineBody, XContentType.JSON))
            .actionGet();
        final BytesReference finalPipelineBody = new BytesArray("{\"processors\": [{\"final\": {\"exists\":\"default\"}}]}");
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("final_pipeline", finalPipelineBody, XContentType.JSON))
            .actionGet();
        final int lowOrder = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final int highOrder = randomIntBetween(lowOrder + 1, Integer.MAX_VALUE);
        final int finalPipelineOrder;
        final int defaultPipelineOrder;
        if (randomBoolean()) {
            defaultPipelineOrder = lowOrder;
            finalPipelineOrder = highOrder;
        } else {
            defaultPipelineOrder = highOrder;
            finalPipelineOrder = lowOrder;
        }
        final Settings defaultPipelineSettings =
            Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build();
        admin().indices()
            .preparePutTemplate("default")
            .setPatterns(Collections.singletonList("index*"))
            .setOrder(defaultPipelineOrder)
            .setSettings(defaultPipelineSettings)
            .get();
        final Settings finalPipelineSettings =
            Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "final_pipeline").build();
        admin().indices()
            .preparePutTemplate("final")
            .setPatterns(Collections.singletonList("index*"))
            .setOrder(finalPipelineOrder)
            .setSettings(finalPipelineSettings)
            .get();
        final IndexRequestBuilder index = client().prepareIndex("index", "_doc", "1");
        index.setSource(Collections.singletonMap("field", "value"));
        index.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final IndexResponse response = index.get();
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        final GetRequestBuilder get = client().prepareGet("index", "_doc", "1");
        final GetResponse getResponse = get.get();
        assertTrue(getResponse.isExists());
        final Map<String, Object> source = getResponse.getSourceAsMap();
        assertThat(source, hasKey("default"));
        assertTrue((boolean) source.get("default"));
        assertThat(source, hasKey("final"));
        assertTrue((boolean) source.get("final"));
    }

    public void testHighOrderFinalPipelinePreferred() throws IOException {
        final int lowOrder = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final int highOrder = randomIntBetween(lowOrder + 1, Integer.MAX_VALUE);
        final Settings lowOrderFinalPipelineSettings =
            Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "low_order_final_pipeline").build();
        admin().indices()
            .preparePutTemplate("low_order")
            .setPatterns(Collections.singletonList("index*"))
            .setOrder(lowOrder)
            .setSettings(lowOrderFinalPipelineSettings)
            .get();
        final Settings highOrderFinalPipelineSettings =
            Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "high_order_final_pipeline").build();
        admin().indices()
            .preparePutTemplate("high_order")
            .setPatterns(Collections.singletonList("index*"))
            .setOrder(highOrder)
            .setSettings(highOrderFinalPipelineSettings)
            .get();

        // this asserts that the high_order_final_pipeline was selected, without us having to actually create the pipeline etc.
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("index", "_doc", "1").setSource(Collections.singletonMap("field", "value")).get());
        assertThat(e, hasToString(containsString("pipeline with id [high_order_final_pipeline] does not exist")));
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        @Override
        public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry,
            final IndexNameExpressionResolver expressionResolver,
            final Supplier<RepositoriesService> repositoriesServiceSupplier) {
            return Collections.emptyList();
        }

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            final HashMap<String, Processor.Factory> map = new HashMap<>(3);
            map.put(
                "default",
                    (factories, tag, description, config) ->
                    new AbstractProcessor(tag, description) {

                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            ingestDocument.setFieldValue("default", true);
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "default";
                        }
                    });
            map.put(
                "final",
                    (processorFactories, tag, description, config) -> {
                    final String exists = (String) config.remove("exists");
                    return new AbstractProcessor(tag, description) {
                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            // this asserts that this pipeline is the final pipeline executed
                            if (exists != null) {
                                if (ingestDocument.getSourceAndMetadata().containsKey(exists) == false) {
                                    throw new AssertionError(
                                        "expected document to contain [" + exists + "] but was [" + ingestDocument.getSourceAndMetadata());
                                }
                            }
                            ingestDocument.setFieldValue("final", true);
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "final";
                        }
                    };
                });
            map.put(
                "request",
                    (processorFactories, tag, description, config) ->
                    new AbstractProcessor(tag, description) {
                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            ingestDocument.setFieldValue("request", true);
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "request";
                        }
                    });
            map.put(
                "changing_dest", (processorFactories, tag, description, config) ->
                    new AbstractProcessor(tag, description) {
                        @Override
                        public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                            ingestDocument.setFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), "target");
                            return ingestDocument;
                        }

                        @Override
                        public String getType() {
                            return "changing_dest";
                        }

                    }
            );
            return map;
        }
    }

}
