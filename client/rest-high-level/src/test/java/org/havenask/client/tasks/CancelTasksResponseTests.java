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

package org.havenask.client.tasks;

import org.havenask.HavenaskException;
import org.havenask.Version;
import org.havenask.action.TaskOperationFailure;
import org.havenask.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.havenask.client.AbstractResponseTestCase;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.transport.TransportAddress;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.tasks.Task;
import org.havenask.tasks.TaskId;
import org.havenask.tasks.TaskInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class CancelTasksResponseTests extends AbstractResponseTestCase<CancelTasksResponseTests.ByNodeCancelTasksResponse,
    org.havenask.client.tasks.CancelTasksResponse> {

    private static String NODE_ID = "node_id";

    @Override
    protected CancelTasksResponseTests.ByNodeCancelTasksResponse createServerTestInstance(XContentType xContentType) {
        List<org.havenask.tasks.TaskInfo> tasks = new ArrayList<>();
        List<TaskOperationFailure> taskFailures = new ArrayList<>();
        List<HavenaskException> nodeFailures = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            taskFailures.add(new TaskOperationFailure(randomAlphaOfLength(4), (long) i,
                new RuntimeException(randomAlphaOfLength(4))));
        }
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            nodeFailures.add(new HavenaskException(new RuntimeException(randomAlphaOfLength(10))));
        }

        for (int i = 0; i < 4; i++) {
            tasks.add(new org.havenask.tasks.TaskInfo(
                new TaskId(NODE_ID, (long) i),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomAlphaOfLength(10),
                new FakeTaskStatus(randomAlphaOfLength(4), randomInt()),
                randomLongBetween(1, 3),
                randomIntBetween(5, 10),
                false,
                new TaskId("node1", randomLong()),
                Collections.singletonMap("x-header-of", "some-value")));
        }

        return new ByNodeCancelTasksResponse(tasks, taskFailures, nodeFailures);
    }

    @Override
    protected org.havenask.client.tasks.CancelTasksResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return org.havenask.client.tasks.CancelTasksResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(ByNodeCancelTasksResponse serverTestInstance,
                                   org.havenask.client.tasks.CancelTasksResponse clientInstance) {

        // checking tasks
        List<TaskInfo> sTasks = serverTestInstance.getTasks();
        List<org.havenask.client.tasks.TaskInfo> cTasks = clientInstance.getTasks();
        Map<org.havenask.client.tasks.TaskId, org.havenask.client.tasks.TaskInfo> cTasksMap =
            cTasks.stream().collect(Collectors.toMap(org.havenask.client.tasks.TaskInfo::getTaskId,
                Function.identity()));
        for (TaskInfo ti : sTasks) {
            org.havenask.client.tasks.TaskInfo taskInfo = cTasksMap.get(
                new org.havenask.client.tasks.TaskId(ti.getTaskId().getNodeId(), ti.getTaskId().getId())
            );
            assertEquals(ti.getAction(), taskInfo.getAction());
            assertEquals(ti.getDescription(), taskInfo.getDescription());
            assertEquals(new HashMap<>(ti.getHeaders()), new HashMap<>(taskInfo.getHeaders()));
            assertEquals(ti.getType(), taskInfo.getType());
            assertEquals(ti.getStartTime(), taskInfo.getStartTime());
            assertEquals(ti.getRunningTimeNanos(), taskInfo.getRunningTimeNanos());
            assertEquals(ti.isCancellable(), taskInfo.isCancellable());
            assertEquals(ti.getParentTaskId().getNodeId(), taskInfo.getParentTaskId().getNodeId());
            assertEquals(ti.getParentTaskId().getId(), taskInfo.getParentTaskId().getId());
            FakeTaskStatus status = (FakeTaskStatus) ti.getStatus();
            assertEquals(status.code, taskInfo.getStatus().get("code"));
            assertEquals(status.status, taskInfo.getStatus().get("status"));

        }

        //checking failures
        List<HavenaskException> serverNodeFailures = serverTestInstance.getNodeFailures();
        List<org.havenask.client.tasks.HavenaskException> cNodeFailures = clientInstance.getNodeFailures();
        List<String> sExceptionsMessages = serverNodeFailures.stream().map(x ->
            org.havenask.client.tasks.HavenaskException.buildMessage(
                "exception", x.getMessage(), null)
        ).collect(Collectors.toList()
        );

        List<String> cExceptionsMessages = cNodeFailures.stream().map(
            org.havenask.client.tasks.HavenaskException::getMsg
        ).collect(Collectors.toList());
        assertEquals(new HashSet<>(sExceptionsMessages), new HashSet<>(cExceptionsMessages));

        List<TaskOperationFailure> sTaskFailures = serverTestInstance.getTaskFailures();
        List<org.havenask.client.tasks.TaskOperationFailure> cTaskFailures = clientInstance.getTaskFailures();

        Map<Long, org.havenask.client.tasks.TaskOperationFailure> cTasksFailuresMap =
            cTaskFailures.stream().collect(Collectors.toMap(
                org.havenask.client.tasks.TaskOperationFailure::getTaskId,
                Function.identity()));
        for (TaskOperationFailure tof : sTaskFailures) {
            org.havenask.client.tasks.TaskOperationFailure failure = cTasksFailuresMap.get(tof.getTaskId());
            assertEquals(tof.getNodeId(), failure.getNodeId());
            assertTrue(failure.getReason().getMsg().contains("runtime_exception"));
            assertTrue(failure.getStatus().contains("" + tof.getStatus().name()));
        }
    }

    public static class FakeTaskStatus implements Task.Status {

        final String status;
        final int code;

        public FakeTaskStatus(String status, int code) {
            this.status = status;
            this.code = code;
        }

        @Override
        public String getWriteableName() {
            return "faker";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(status);
            out.writeInt(code);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("status", status);
            builder.field("code", code);
            return builder.endObject();
        }
    }

    /**
     * tasks are grouped under nodes, and in order to create DiscoveryNodes we need different
     * IP addresses.
     * So in this test we assume all tasks are issued by a single node whose name and IP address is hardcoded.
     */
    static class ByNodeCancelTasksResponse extends CancelTasksResponse {

        ByNodeCancelTasksResponse(StreamInput in) throws IOException {
            super(in);
        }

        ByNodeCancelTasksResponse(
            List<TaskInfo> tasks,
            List<TaskOperationFailure> taskFailures,
            List<? extends HavenaskException> nodeFailures) {
            super(tasks, taskFailures, nodeFailures);
        }


        // it knows the hardcoded address space.
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

            DiscoveryNodes.Builder dnBuilder = new DiscoveryNodes.Builder();
            InetAddress inetAddress = InetAddress.getByAddress(new byte[]{(byte) 192, (byte) 168, (byte) 0, (byte) 1});
            TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

            dnBuilder.add(new DiscoveryNode(NODE_ID, NODE_ID, transportAddress, emptyMap(), emptySet(), Version.CURRENT));

            DiscoveryNodes build = dnBuilder.build();
            builder.startObject();
            super.toXContentGroupedByNode(builder, params, build);
            builder.endObject();
            return builder;
        }
    }
}


