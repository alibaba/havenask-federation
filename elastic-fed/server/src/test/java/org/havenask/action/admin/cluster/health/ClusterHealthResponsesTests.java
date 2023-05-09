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

package org.havenask.action.admin.cluster.health;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.health.ClusterIndexHealth;
import org.havenask.cluster.health.ClusterIndexHealthTests;
import org.havenask.cluster.health.ClusterStateHealth;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.rest.RestStatus;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.test.HavenaskTestCase;

import org.hamcrest.Matchers;
import org.havenask.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ClusterHealthResponsesTests extends AbstractSerializingTestCase<ClusterHealthResponse> {
    private final ClusterHealthRequest.Level level = randomFrom(ClusterHealthRequest.Level.values());

    public void testIsTimeout() {
        ClusterHealthResponse res = new ClusterHealthResponse();
        for (int i = 0; i < 5; i++) {
            res.setTimedOut(randomBoolean());
            if (res.isTimedOut()) {
                assertEquals(RestStatus.REQUEST_TIMEOUT, res.status());
            } else {
                assertEquals(RestStatus.OK, res.status());
            }
        }
    }

    public void testClusterHealth() throws IOException {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();
        int pendingTasks = randomIntBetween(0, 200);
        int inFlight = randomIntBetween(0, 200);
        int delayedUnassigned = randomIntBetween(0, 200);
        TimeValue pendingTaskInQueueTime = TimeValue.timeValueMillis(randomIntBetween(1000, 100000));
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse("bla", new String[] {Metadata.ALL},
            clusterState, pendingTasks, inFlight, delayedUnassigned, pendingTaskInQueueTime);
        clusterHealth = maybeSerialize(clusterHealth);
        assertClusterHealth(clusterHealth);
        assertThat(clusterHealth.getNumberOfPendingTasks(), Matchers.equalTo(pendingTasks));
        assertThat(clusterHealth.getNumberOfInFlightFetch(), Matchers.equalTo(inFlight));
        assertThat(clusterHealth.getDelayedUnassignedShards(), Matchers.equalTo(delayedUnassigned));
        assertThat(clusterHealth.getTaskMaxWaitingTime().millis(), is(pendingTaskInQueueTime.millis()));
        assertThat(clusterHealth.getActiveShardsPercent(), is(allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0))));
    }

    public void testClusterHealthVerifyMasterNodeDiscovery() throws IOException {
        DiscoveryNode localNode = new DiscoveryNode("node", HavenaskTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        //set the node information to verify master_node discovery in ClusterHealthResponse
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(localNode.getId()))
            .build();
        int pendingTasks = randomIntBetween(0, 200);
        int inFlight = randomIntBetween(0, 200);
        int delayedUnassigned = randomIntBetween(0, 200);
        TimeValue pendingTaskInQueueTime = TimeValue.timeValueMillis(randomIntBetween(1000, 100000));
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse("bla", new String[] {Metadata.ALL},
            clusterState, pendingTasks, inFlight, delayedUnassigned, pendingTaskInQueueTime);
        clusterHealth = maybeSerialize(clusterHealth);
        assertThat(clusterHealth.getClusterStateHealth().hasDiscoveredMaster(), Matchers.equalTo(true));
        assertClusterHealth(clusterHealth);
    }

    private void assertClusterHealth(ClusterHealthResponse clusterHealth) {
        ClusterStateHealth clusterStateHealth = clusterHealth.getClusterStateHealth();

        assertThat(clusterHealth.getActiveShards(), Matchers.equalTo(clusterStateHealth.getActiveShards()));
        assertThat(clusterHealth.getRelocatingShards(), Matchers.equalTo(clusterStateHealth.getRelocatingShards()));
        assertThat(clusterHealth.getActivePrimaryShards(), Matchers.equalTo(clusterStateHealth.getActivePrimaryShards()));
        assertThat(clusterHealth.getInitializingShards(), Matchers.equalTo(clusterStateHealth.getInitializingShards()));
        assertThat(clusterHealth.getUnassignedShards(), Matchers.equalTo(clusterStateHealth.getUnassignedShards()));
        assertThat(clusterHealth.getNumberOfNodes(), Matchers.equalTo(clusterStateHealth.getNumberOfNodes()));
        assertThat(clusterHealth.getNumberOfDataNodes(), Matchers.equalTo(clusterStateHealth.getNumberOfDataNodes()));
        assertThat(clusterHealth.hasDiscoveredMaster(), Matchers.equalTo(clusterStateHealth.hasDiscoveredMaster()));
    }

    public void testVersionCompatibleSerialization() throws IOException {
        boolean hasDiscoveredMaster = false;
        int indicesSize = randomInt(20);
        Map<String, ClusterIndexHealth> indices = new HashMap<>(indicesSize);
        if ("indices".equals(level) || "shards".equals(level)) {
            for (int i = 0; i < indicesSize; i++) {
                String indexName = randomAlphaOfLengthBetween(1, 5) + i;
                indices.put(indexName, ClusterIndexHealthTests.randomIndexHealth(indexName, level));
            }
        }
        ClusterStateHealth stateHealth = new ClusterStateHealth(randomInt(100), randomInt(100), randomInt(100),
            randomInt(100), randomInt(100), randomInt(100), randomInt(100), hasDiscoveredMaster,
            randomDoubleBetween(0d, 100d, true), randomFrom(ClusterHealthStatus.values()), indices);
        //Create the Cluster Health Response object with discovered master as false,
        //to verify serialization puts default value for the field
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse("test-cluster", randomInt(100), randomInt(100),
            randomInt(100), TimeValue.timeValueMillis(randomInt(10000)), randomBoolean(), stateHealth);

        BytesStreamOutput out_lt_1_0 = new BytesStreamOutput();
        Version old_version = VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_6_0_0, LegacyESVersion.V_6_8_0);
        out_lt_1_0.setVersion(old_version);
        clusterHealth.writeTo(out_lt_1_0);

        BytesStreamOutput out_gt_1_0 = new BytesStreamOutput();
        Version new_version = VersionUtils.randomVersionBetween(random(), Version.V_1_0_0, Version.CURRENT);
        out_gt_1_0.setVersion(new_version);
        clusterHealth.writeTo(out_gt_1_0);

        //The serialized output byte stream will not be same; and different by a boolean field "discovered_master"
        assertNotEquals(out_lt_1_0.size(), out_gt_1_0.size());
        assertThat(out_gt_1_0.size() - out_lt_1_0.size(), Matchers.equalTo(1));

        //Input stream constructed from Version 6_8 or less will not have field "discovered_master";
        //hence fallback to default as no value retained
        StreamInput in_lt_6_8 = out_lt_1_0.bytes().streamInput();
        in_lt_6_8.setVersion(old_version);
        clusterHealth = ClusterHealthResponse.readResponseFrom(in_lt_6_8);
        assertThat(clusterHealth.hasDiscoveredMaster(), Matchers.equalTo(true));

        //Input stream constructed from Version 7_0 and above will have field "discovered_master"; hence value will be retained
        StreamInput in_gt_7_0 = out_gt_1_0.bytes().streamInput();
        in_gt_7_0.setVersion(new_version);
        clusterHealth = ClusterHealthResponse.readResponseFrom(in_gt_7_0);
        assertThat(clusterHealth.hasDiscoveredMaster(), Matchers.equalTo(hasDiscoveredMaster));
    }

    ClusterHealthResponse maybeSerialize(ClusterHealthResponse clusterHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterHealth.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            clusterHealth = ClusterHealthResponse.readResponseFrom(in);
        }
        return clusterHealth;
    }

    public void testParseFromXContentWithDiscoveredMasterField() throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{\"cluster_name\":\"535799904437:7-1-3-node\",\"status\":\"green\"," +
                "\"timed_out\":false,\"number_of_nodes\":6,\"number_of_data_nodes\":3,\"discovered_master\":false," +
                "\"active_primary_shards\":4,\"active_shards\":5,\"relocating_shards\":0,\"initializing_shards\":0," +
                "\"unassigned_shards\":0,\"delayed_unassigned_shards\":0,\"number_of_pending_tasks\":0," +
                "\"number_of_in_flight_fetch\":0,\"task_max_waiting_in_queue_millis\":0," +
                "\"active_shards_percent_as_number\":100}")) {

            ClusterHealthResponse clusterHealth = ClusterHealthResponse.fromXContent(parser);
            assertNotNull(clusterHealth);
            assertThat(clusterHealth.getClusterName(), Matchers.equalTo("535799904437:7-1-3-node"));
            assertThat(clusterHealth.getNumberOfNodes(), Matchers.equalTo(6));
            assertThat(clusterHealth.hasDiscoveredMaster(), Matchers.equalTo(false));
        }
    }

    @Override
    protected ClusterHealthResponse doParseInstance(XContentParser parser) {
        return ClusterHealthResponse.fromXContent(parser);
    }

    @Override
    protected ClusterHealthResponse createTestInstance() {
        int indicesSize = randomInt(20);
        Map<String, ClusterIndexHealth> indices = new HashMap<>(indicesSize);
        if (ClusterHealthRequest.Level.INDICES.equals(level) || ClusterHealthRequest.Level.SHARDS.equals(level)) {
            for (int i = 0; i < indicesSize; i++) {
                String indexName = randomAlphaOfLengthBetween(1, 5) + i;
                indices.put(indexName, ClusterIndexHealthTests.randomIndexHealth(indexName, level));
            }
        }
        ClusterStateHealth stateHealth = new ClusterStateHealth(randomInt(100), randomInt(100), randomInt(100),
                randomInt(100), randomInt(100), randomInt(100), randomInt(100), randomBoolean(),
                randomDoubleBetween(0d, 100d, true), randomFrom(ClusterHealthStatus.values()), indices);

        return new ClusterHealthResponse(randomAlphaOfLengthBetween(1, 10), randomInt(100), randomInt(100), randomInt(100),
                TimeValue.timeValueMillis(randomInt(10000)), randomBoolean(), stateHealth);
    }

    @Override
    protected Writeable.Reader<ClusterHealthResponse> instanceReader() {
        return ClusterHealthResponse::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap("level", level.name().toLowerCase(Locale.ROOT)));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    // Ignore all paths which looks like "indices.RANDOMINDEXNAME.shards"
    private static final Pattern SHARDS_IN_XCONTENT = Pattern.compile("^indices\\.\\w+\\.shards$");

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> "indices".equals(field) || SHARDS_IN_XCONTENT.matcher(field).find();
    }

    @Override
    protected ClusterHealthResponse mutateInstance(ClusterHealthResponse instance) {
        String mutate = randomFrom("clusterName", "numberOfPendingTasks","numberOfInFlightFetch", "delayedUnassignedShards",
                "taskMaxWaitingTime", "timedOut", "clusterStateHealth");
        switch (mutate) {
            case "clusterName":
                return new ClusterHealthResponse(instance.getClusterName() + randomAlphaOfLengthBetween(2, 5),
                        instance.getNumberOfPendingTasks(), instance.getNumberOfInFlightFetch(),
                        instance.getDelayedUnassignedShards(), instance.getTaskMaxWaitingTime(),
                        instance.isTimedOut(), instance.getClusterStateHealth());
            case "numberOfPendingTasks":
                return new ClusterHealthResponse(instance.getClusterName(),
                        instance.getNumberOfPendingTasks() + between(1, 10), instance.getNumberOfInFlightFetch(),
                        instance.getDelayedUnassignedShards(), instance.getTaskMaxWaitingTime(),
                        instance.isTimedOut(), instance.getClusterStateHealth());
            case "numberOfInFlightFetch":
                return new ClusterHealthResponse(instance.getClusterName(),
                        instance.getNumberOfPendingTasks(), instance.getNumberOfInFlightFetch() + between(1, 10),
                        instance.getDelayedUnassignedShards(), instance.getTaskMaxWaitingTime(),
                        instance.isTimedOut(), instance.getClusterStateHealth());
            case "delayedUnassignedShards":
                return new ClusterHealthResponse(instance.getClusterName(),
                        instance.getNumberOfPendingTasks(), instance.getNumberOfInFlightFetch(),
                        instance.getDelayedUnassignedShards() + between(1, 10), instance.getTaskMaxWaitingTime(),
                        instance.isTimedOut(), instance.getClusterStateHealth());
            case "taskMaxWaitingTime":

                return new ClusterHealthResponse(instance.getClusterName(),
                        instance.getNumberOfPendingTasks(), instance.getNumberOfInFlightFetch(),
                        instance.getDelayedUnassignedShards(), new TimeValue(instance.getTaskMaxWaitingTime().millis() + between(1, 10)),
                        instance.isTimedOut(), instance.getClusterStateHealth());
            case "timedOut":
                return new ClusterHealthResponse(instance.getClusterName(),
                        instance.getNumberOfPendingTasks(), instance.getNumberOfInFlightFetch(),
                        instance.getDelayedUnassignedShards(), instance.getTaskMaxWaitingTime(),
                        instance.isTimedOut() == false, instance.getClusterStateHealth());
            case "clusterStateHealth":
                ClusterStateHealth state = instance.getClusterStateHealth();
                ClusterStateHealth newState = new ClusterStateHealth(state.getActivePrimaryShards() + between(1, 10),
                        state.getActiveShards(), state.getRelocatingShards(), state.getInitializingShards(), state.getUnassignedShards(),
                        state.getNumberOfNodes(), state.getNumberOfDataNodes(), state.hasDiscoveredMaster(), state.getActiveShardsPercent(),
                        state.getStatus(), state.getIndices());
                return new ClusterHealthResponse(instance.getClusterName(),
                        instance.getNumberOfPendingTasks(), instance.getNumberOfInFlightFetch(),
                        instance.getDelayedUnassignedShards(), instance.getTaskMaxWaitingTime(),
                        instance.isTimedOut(), newState);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
