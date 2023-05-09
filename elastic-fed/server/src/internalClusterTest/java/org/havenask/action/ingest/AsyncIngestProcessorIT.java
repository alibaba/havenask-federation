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

package org.havenask.action.ingest;

import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.bulk.BulkResponse;
import org.havenask.action.get.GetRequest;
import org.havenask.action.get.GetResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.client.Client;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentType;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.ingest.AbstractProcessor;
import org.havenask.ingest.IngestDocument;
import org.havenask.ingest.Processor;
import org.havenask.plugins.IngestPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.RepositoriesService;
import org.havenask.script.ScriptService;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.havenask.threadpool.ThreadPool;
import org.havenask.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * The purpose of this test is to verify that when a processor executes an operation asynchronously that
 * the expected result is the same as if the same operation happens synchronously.
 *
 * In this test two test processor are defined that basically do the same operation, but a single processor
 * executes asynchronously. The result of the operation should be the same and also the order in which the
 * bulk responses are returned should be the same as how the corresponding index requests were defined.
 */
public class AsyncIngestProcessorIT extends HavenaskSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TestPlugin.class);
    }

    public void testAsyncProcessorImplementation() {
        // A pipeline with 2 processors: the test async processor and sync test processor.
        BytesReference pipelineBody = new BytesArray("{\"processors\": [{\"test-async\": {}, \"test\": {}}]}");
        client().admin().cluster().putPipeline(new PutPipelineRequest("_id", pipelineBody, XContentType.JSON)).actionGet();

        BulkRequest bulkRequest = new BulkRequest();
        int numDocs = randomIntBetween(8, 256);
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest("foobar")
                .id(Integer.toString(i))
                .source("{}", XContentType.JSON)
                .setPipeline("_id")
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertThat(bulkResponse.getItems()[i].getId(), equalTo(id));
            GetResponse getResponse = client().get(new GetRequest("foobar", id)).actionGet();
            // The expected result of async test processor:
            assertThat(getResponse.getSource().get("foo"), equalTo("bar-" + id));
            // The expected result of sync test processor:
            assertThat(getResponse.getSource().get("bar"), equalTo("baz-" + id));
        }
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        private ThreadPool threadPool;

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                                   IndexNameExpressionResolver expressionResolver,
                                                   Supplier<RepositoriesService> repositoriesServiceSupplier) {
            this.threadPool = threadPool;
            return Collections.emptyList();
        }

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put("test-async", (factories, tag, description, config) -> {
                return new AbstractProcessor(tag, description) {

                    @Override
                    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                        threadPool.generic().execute(() -> {
                            String id = (String) ingestDocument.getSourceAndMetadata().get("_id");
                            if (usually()) {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    // ignore
                                }
                            }
                            ingestDocument.setFieldValue("foo", "bar-" + id);
                            handler.accept(ingestDocument, null);
                        });
                    }

                    @Override
                    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getType() {
                        return "test-async";
                    }
                };
            });
            processors.put("test", (processorFactories, tag, description, config) -> {
                return new AbstractProcessor(tag, description) {
                    @Override
                    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                        String id = (String) ingestDocument.getSourceAndMetadata().get("_id");
                        ingestDocument.setFieldValue("bar", "baz-" + id);
                        return ingestDocument;
                    }

                    @Override
                    public String getType() {
                        return "test";
                    }
                };
            });
            return processors;
        }
    }

}
