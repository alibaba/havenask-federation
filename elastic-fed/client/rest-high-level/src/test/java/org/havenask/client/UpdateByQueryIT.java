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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.havenask.action.ActionListener;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.get.GetRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.support.WriteRequest;
import org.havenask.client.tasks.TaskSubmissionResponse;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.query.IdsQueryBuilder;
import org.havenask.index.reindex.BulkByScrollResponse;
import org.havenask.index.reindex.UpdateByQueryAction;
import org.havenask.index.reindex.UpdateByQueryRequest;
import org.havenask.rest.RestStatus;
import org.havenask.script.Script;
import org.havenask.tasks.RawTaskStatus;
import org.havenask.tasks.TaskId;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class UpdateByQueryIT extends HavenaskRestHighLevelClientTestCase {

    public void testUpdateByQuery() throws Exception {
        final String sourceIndex = "source1";
        {
            // Prepare
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(sourceIndex, settings);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    new BulkRequest()
                        .add(new IndexRequest(sourceIndex).id("1")
                            .source(Collections.singletonMap("foo", 1), XContentType.JSON))
                        .add(new IndexRequest(sourceIndex).id("2")
                            .source(Collections.singletonMap("foo", 2), XContentType.JSON))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // test1: create one doc in dest
            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
            updateByQueryRequest.indices(sourceIndex);
            updateByQueryRequest.setQuery(new IdsQueryBuilder().addIds("1"));
            updateByQueryRequest.setRefresh(true);
            BulkByScrollResponse bulkResponse =
                execute(updateByQueryRequest, highLevelClient()::updateByQuery, highLevelClient()::updateByQueryAsync);
            assertEquals(1, bulkResponse.getTotal());
            assertEquals(1, bulkResponse.getUpdated());
            assertEquals(0, bulkResponse.getNoops());
            assertEquals(0, bulkResponse.getVersionConflicts());
            assertEquals(1, bulkResponse.getBatches());
            assertTrue(bulkResponse.getTook().getMillis() > 0);
            assertEquals(1, bulkResponse.getBatches());
            assertEquals(0, bulkResponse.getBulkFailures().size());
            assertEquals(0, bulkResponse.getSearchFailures().size());
        }
        {
            // test2: update using script
            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
            updateByQueryRequest.indices(sourceIndex);
            updateByQueryRequest.setScript(new Script("if (ctx._source.foo == 2) ctx._source.foo++;"));
            updateByQueryRequest.setRefresh(true);
            BulkByScrollResponse bulkResponse =
                execute(updateByQueryRequest, highLevelClient()::updateByQuery, highLevelClient()::updateByQueryAsync);
            assertEquals(2, bulkResponse.getTotal());
            assertEquals(2, bulkResponse.getUpdated());
            assertEquals(0, bulkResponse.getDeleted());
            assertEquals(0, bulkResponse.getNoops());
            assertEquals(0, bulkResponse.getVersionConflicts());
            assertEquals(1, bulkResponse.getBatches());
            assertTrue(bulkResponse.getTook().getMillis() > 0);
            assertEquals(1, bulkResponse.getBatches());
            assertEquals(0, bulkResponse.getBulkFailures().size());
            assertEquals(0, bulkResponse.getSearchFailures().size());
            assertEquals(
                3,
                (int) (highLevelClient().get(new GetRequest(sourceIndex, "2"), RequestOptions.DEFAULT)
                    .getSourceAsMap().get("foo"))
            );
        }
        {
            // test update-by-query rethrottling
            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
            updateByQueryRequest.indices(sourceIndex);
            updateByQueryRequest.setQuery(new IdsQueryBuilder().addIds("1"));
            updateByQueryRequest.setRefresh(true);

            // this following settings are supposed to halt reindexing after first document
            updateByQueryRequest.setBatchSize(1);
            updateByQueryRequest.setRequestsPerSecond(0.00001f);
            final CountDownLatch taskFinished = new CountDownLatch(1);
            highLevelClient().updateByQueryAsync(updateByQueryRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {

                @Override
                public void onResponse(BulkByScrollResponse response) {
                    taskFinished.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.toString());
                }
            });

            TaskId taskIdToRethrottle = findTaskToRethrottle(UpdateByQueryAction.NAME, updateByQueryRequest.getDescription());
            float requestsPerSecond = 1000f;
            ListTasksResponse response = execute(new RethrottleRequest(taskIdToRethrottle, requestsPerSecond),
                highLevelClient()::updateByQueryRethrottle, highLevelClient()::updateByQueryRethrottleAsync);
            assertThat(response.getTasks(), hasSize(1));
            assertEquals(taskIdToRethrottle, response.getTasks().get(0).getTaskId());
            assertThat(response.getTasks().get(0).getStatus(), instanceOf(RawTaskStatus.class));
            assertEquals(Float.toString(requestsPerSecond),
                ((RawTaskStatus) response.getTasks().get(0).getStatus()).toMap().get("requests_per_second").toString());
            assertTrue(taskFinished.await(10, TimeUnit.SECONDS));

            // any rethrottling after the update-by-query is done performed with the same taskId should result in a failure
            response = execute(new RethrottleRequest(taskIdToRethrottle, requestsPerSecond),
                highLevelClient()::updateByQueryRethrottle, highLevelClient()::updateByQueryRethrottleAsync);
            assertTrue(response.getTasks().isEmpty());
            assertFalse(response.getNodeFailures().isEmpty());
            assertEquals(1, response.getNodeFailures().size());
            assertEquals("Havenask exception [type=resource_not_found_exception, reason=task [" + taskIdToRethrottle + "] is missing]",
                response.getNodeFailures().get(0).getCause().getMessage());
        }
    }

    public void testUpdateByQueryTask() throws Exception {
        final String sourceIndex = "testupdatebyquerytask";
        {
            // Prepare
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(sourceIndex, settings);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    new BulkRequest()
                        .add(new IndexRequest(sourceIndex).id("1")
                            .source(Collections.singletonMap("foo", 1), XContentType.JSON))
                        .add(new IndexRequest(sourceIndex).id("2")
                            .source(Collections.singletonMap("foo", 2), XContentType.JSON))
                        .add(new IndexRequest(sourceIndex).id("3")
                            .source(Collections.singletonMap("foo", 3), XContentType.JSON))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // tag::submit-update_by_query-task
            UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
            updateByQueryRequest.indices(sourceIndex);
            updateByQueryRequest.setScript(
                new Script("if (ctx._source.foo == 2) ctx._source.foo++;"));
            updateByQueryRequest.setRefresh(true);

            TaskSubmissionResponse updateByQuerySubmission = highLevelClient()
                .submitUpdateByQueryTask(updateByQueryRequest, RequestOptions.DEFAULT);

            String taskId = updateByQuerySubmission.getTask();
            // end::submit-update_by_query-task

            assertBusy(checkTaskCompletionStatus(client(), taskId));
        }
    }

    public void testUpdateByQueryConflict() throws IOException {
        final String index = "testupdatebyqueryconflict";

        final Settings settings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(index, settings);
        final BulkRequest bulkRequest = new BulkRequest()
            .add(new IndexRequest(index).id("1").source(Collections.singletonMap("foo", "bar"), XContentType.JSON))
            .add(new IndexRequest(index).id("2").source(Collections.singletonMap("foo", "bar"), XContentType.JSON))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertThat(highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT).status(), equalTo(RestStatus.OK));

        putConflictPipeline();

        final UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(index);
        updateByQueryRequest.setRefresh(true);
        updateByQueryRequest.setPipeline(CONFLICT_PIPELINE_ID);
        final BulkByScrollResponse response = highLevelClient().updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);

        assertThat(response.getVersionConflicts(), equalTo(1L));
        assertThat(response.getSearchFailures(), empty());
        assertThat(response.getBulkFailures(), hasSize(1));
        assertThat(
            response.getBulkFailures().stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.toSet()),
            everyItem(containsString("version conflict"))
        );

        assertThat(response.getTotal(), equalTo(2L));
        assertThat(response.getCreated(), equalTo(0L));
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(response.getDeleted(), equalTo(0L));
        assertThat(response.getNoops(), equalTo(0L));
        assertThat(response.getBatches(), equalTo(1));
        assertTrue(response.getTook().getMillis() > 0);
    }
}
