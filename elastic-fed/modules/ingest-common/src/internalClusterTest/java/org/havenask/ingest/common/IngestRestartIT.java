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

package org.havenask.ingest.common;

import org.havenask.action.admin.cluster.node.stats.NodesStatsResponse;
import org.havenask.action.support.WriteRequest;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.ingest.IngestStats;
import org.havenask.plugins.Plugin;
import org.havenask.script.MockScriptEngine;
import org.havenask.script.MockScriptPlugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.havenask.action.admin.cluster.node.stats.NodesStatsRequest.Metric.INGEST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Ideally I like this test to live in the server module, but otherwise a large part of the ScriptProcessor
// ends up being copied into this test.
@HavenaskIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = HavenaskIntegTestCase.Scope.TEST)
public class IngestRestartIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonPlugin.class, CustomScriptPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> pluginScripts = new HashMap<>();
            pluginScripts.put("my_script", ctx -> {
                ctx.put("z", 0);
                return null;
            });
            pluginScripts.put("throwing_script", ctx -> {
                throw new RuntimeException("this script always fails");
            });
            return pluginScripts;
        }
    }

    public void testFailureInConditionalProcessor() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        internalCluster().startMasterOnlyNode();
        final String pipelineId = "foo";
        client().admin().cluster().preparePutPipeline(pipelineId,
            new BytesArray("{\n" +
                "  \"processors\" : [\n" +
                "  {\"set\" : {\"field\": \"any_field\", \"value\": \"any_value\"}},\n" +
                "  {\"set\" : {" + "" +
                "    \"if\" : " + "{\"lang\": \"" + MockScriptEngine.NAME + "\", \"source\": \"throwing_script\"}," +
                "    \"field\": \"any_field2\"," +
                "    \"value\": \"any_value2\"}" +
                "  }\n" +
                "  ]\n" +
                "}"), XContentType.JSON).get();

        Exception e = expectThrows(
            Exception.class,
            () ->
                client().prepareIndex("index", "doc").setId("1")
                    .setSource("x", 0)
                    .setPipeline(pipelineId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get()
        );
        assertTrue(e.getMessage().contains("this script always fails"));

        NodesStatsResponse r = client().admin().cluster().prepareNodesStats(internalCluster().getNodeNames())
            .addMetric(INGEST.metricName())
            .get();
        int nodeCount = r.getNodes().size();
        for (int k = 0; k < nodeCount; k++) {
            List<IngestStats.ProcessorStat> stats = r.getNodes().get(k).getIngestStats().getProcessorStats().get(pipelineId);
            for (IngestStats.ProcessorStat st : stats) {
                assertThat(st.getStats().getIngestCurrent(), greaterThanOrEqualTo(0L));
            }
        }
    }

    public void testScriptDisabled() throws Exception {
        String pipelineIdWithoutScript = randomAlphaOfLengthBetween(5, 10);
        String pipelineIdWithScript = pipelineIdWithoutScript + "_script";
        internalCluster().startNode();

        BytesReference pipelineWithScript = new BytesArray("{\n" +
            "  \"processors\" : [\n" +
            "      {\"script\" : {\"lang\": \"" + MockScriptEngine.NAME + "\", \"source\": \"my_script\"}}\n" +
            "  ]\n" +
            "}");
        BytesReference pipelineWithoutScript = new BytesArray("{\n" +
            "  \"processors\" : [\n" +
            "      {\"set\" : {\"field\": \"y\", \"value\": 0}}\n" +
            "  ]\n" +
            "}");

        Consumer<String> checkPipelineExists = (id) -> assertThat(client().admin().cluster().prepareGetPipeline(id)
                .get().pipelines().get(0).getId(), equalTo(id));

        client().admin().cluster().preparePutPipeline(pipelineIdWithScript, pipelineWithScript, XContentType.JSON).get();
        client().admin().cluster().preparePutPipeline(pipelineIdWithoutScript, pipelineWithoutScript, XContentType.JSON).get();

        checkPipelineExists.accept(pipelineIdWithScript);
        checkPipelineExists.accept(pipelineIdWithoutScript);


        internalCluster().restartNode(internalCluster().getMasterName(), new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put("script.allowed_types", "none").build();
            }

        });

        checkPipelineExists.accept(pipelineIdWithoutScript);
        checkPipelineExists.accept(pipelineIdWithScript);

        client().prepareIndex("index", "doc", "1")
            .setSource("x", 0)
            .setPipeline(pipelineIdWithoutScript)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> client().prepareIndex("index", "doc", "2")
                .setSource("x", 0)
                .setPipeline(pipelineIdWithScript)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get());
        assertThat(exception.getMessage(),
            equalTo("pipeline with id [" + pipelineIdWithScript + "] could not be loaded, caused by " +
                "[HavenaskParseException[Error updating pipeline with id [" + pipelineIdWithScript + "]]; " +
                "nested: HavenaskException[java.lang.IllegalArgumentException: cannot execute [inline] scripts]; " +
                "nested: IllegalArgumentException[cannot execute [inline] scripts];; " +
                "HavenaskException[java.lang.IllegalArgumentException: cannot execute [inline] scripts]; " +
                "nested: IllegalArgumentException[cannot execute [inline] scripts];; java.lang.IllegalArgumentException: " +
                "cannot execute [inline] scripts]"));

        Map<String, Object> source = client().prepareGet("index", "doc", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

    public void testPipelineWithScriptProcessorThatHasStoredScript() throws Exception {
        internalCluster().startNode();

        client().admin().cluster().preparePutStoredScript()
                .setId("1")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"" + MockScriptEngine.NAME +
                        "\", \"source\": \"my_script\"} }"), XContentType.JSON)
                .get();
        BytesReference pipeline = new BytesArray("{\n" +
                "  \"processors\" : [\n" +
                "      {\"set\" : {\"field\": \"y\", \"value\": 0}},\n" +
                "      {\"script\" : {\"id\": \"1\"}}\n" +
                "  ]\n" +
                "}");
        client().admin().cluster().preparePutPipeline("_id", pipeline, XContentType.JSON).get();

        client().prepareIndex("index", "doc", "1")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        Map<String, Object> source = client().prepareGet("index", "doc", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));

        // Prior to making this ScriptService implement ClusterStateApplier instead of ClusterStateListener,
        // pipelines with a script processor failed to load causing these pipelines and pipelines that were
        // supposed to load after these pipelines to not be available during ingestion, which then causes
        // the next index request in this test to fail.
        internalCluster().fullRestart();
        ensureYellow("index");

        client().prepareIndex("index", "doc", "2")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        source = client().prepareGet("index", "doc", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));
    }

    public void testWithDedicatedIngestNode() throws Exception {
        String node = internalCluster().startNode();
        String ingestNode = internalCluster().startNode(Settings.builder()
                .put("node.master", false)
                .put("node.data", false)
        );

        BytesReference pipeline = new BytesArray("{\n" +
                "  \"processors\" : [\n" +
                "      {\"set\" : {\"field\": \"y\", \"value\": 0}}\n" +
                "  ]\n" +
                "}");
        client().admin().cluster().preparePutPipeline("_id", pipeline, XContentType.JSON).get();

        client().prepareIndex("index", "doc", "1")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        Map<String, Object> source = client().prepareGet("index", "doc", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));

        logger.info("Stopping");
        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback());

        client(ingestNode).prepareIndex("index", "doc", "2")
                .setSource("x", 0)
                .setPipeline("_id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        source = client(ingestNode).prepareGet("index", "doc", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

}
