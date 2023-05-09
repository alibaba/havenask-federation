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

package org.havenask.cluster.action.shard;

import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateTaskExecutor;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.common.Priority;
import org.havenask.common.settings.Settings;
import org.havenask.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.havenask.action.support.replication.ClusterStateCreationUtils.state;
import static org.havenask.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.havenask.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas;
import static org.havenask.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.havenask.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ShardStartedClusterStateTaskExecutorTests extends HavenaskAllocationTestCase {

    private ShardStateAction.ShardStartedClusterStateTaskExecutor executor;

    @SuppressWarnings("unused")
    private static void neverReroutes(String reason, Priority priority, ActionListener<ClusterState> listener) {
        fail("unexpectedly ran a deferred reroute");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AllocationService allocationService = createAllocationService(Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .build());
        executor = new ShardStateAction.ShardStartedClusterStateTaskExecutor(allocationService,
            ShardStartedClusterStateTaskExecutorTests::neverReroutes, () -> Priority.NORMAL, logger);
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, Collections.emptyList());
        assertSame(clusterState, result.resultingState);
    }

    public void testNonExistentIndexMarkedAsSuccessful() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        final StartedShardEntry entry = new StartedShardEntry(new ShardId("test", "_na", 0), "aId", randomNonNegativeLong(), "test");

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(entry));
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(1));
        assertThat(result.executionResults.containsKey(entry), is(true));
        assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(entry)).isSuccess(), is(true));
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithActivePrimary(indexName, true, randomInt(2), randomInt(2));

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final List<StartedShardEntry> tasks = Stream.concat(
            // Existent shard id but different allocation id
            IntStream.range(0, randomIntBetween(1, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetadata.getIndex(), 0), String.valueOf(i), 0L, "allocation id")),
            // Non existent shard id
            IntStream.range(1, randomIntBetween(2, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetadata.getIndex(), i), String.valueOf(i), 0L, "shard id"))

        ).collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
        });
    }

    public void testNonInitializingShardAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithAssignedPrimariesAndReplicas(new String[]{indexName}, randomIntBetween(2, 10), 1);

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final List<StartedShardEntry> tasks = IntStream.range(0, randomIntBetween(1, indexMetadata.getNumberOfShards()))
            .mapToObj(i -> {
                final ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                final IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().shardRoutingTable(shardId);
                final String allocationId;
                if (randomBoolean()) {
                    allocationId = shardRoutingTable.primaryShard().allocationId().getId();
                } else {
                    allocationId = shardRoutingTable.replicaShards().iterator().next().allocationId().getId();
                }
                final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
                return new StartedShardEntry(shardId, allocationId, primaryTerm, "test");
            }).collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
        });
    }

    public void testStartedShards() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String primaryAllocationId = primaryShard.allocationId().getId();

        final List<StartedShardEntry> tasks = new ArrayList<>();
        tasks.add(new StartedShardEntry(shardId, primaryAllocationId, primaryTerm, "test"));
        if (randomBoolean()) {
            final ShardRouting replicaShard = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next();
            final String replicaAllocationId = replicaShard.allocationId().getId();
            tasks.add(new StartedShardEntry(shardId, replicaAllocationId, primaryTerm, "test"));
        }
        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertNotSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

            final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
        });
    }

    public void testDuplicateStartsAreOkay() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardRouting shardRouting = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String allocationId = shardRouting.allocationId().getId();
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());

        final List<StartedShardEntry> tasks = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(i -> new StartedShardEntry(shardId, allocationId, primaryTerm, "test"))
            .collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertNotSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

            final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
        });
    }

    public void testPrimaryTermsMismatch() throws Exception {
        final String indexName = "test";
        final int shard = 0;
        final int primaryTerm = 2 + randomInt(200);

        ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata())
                .put(IndexMetadata.builder(clusterState.metadata().index(indexName))
                    .primaryTerm(shard, primaryTerm)
                    .build(), true)
                .build())
            .build();
        final ShardId shardId = new ShardId(clusterState.metadata().index(indexName).getIndex(), shard);
        final String primaryAllocationId = clusterState.routingTable().shardRoutingTable(shardId).primaryShard().allocationId().getId();
        {
            final StartedShardEntry task =
                new StartedShardEntry(shardId, primaryAllocationId, primaryTerm - 1, "primary terms does not match on primary");

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.INITIALIZING));
            assertSame(clusterState, result.resultingState);
        }
        {
            final StartedShardEntry task =
                new StartedShardEntry(shardId, primaryAllocationId, primaryTerm, "primary terms match on primary");

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            assertNotSame(clusterState, result.resultingState);
            clusterState = result.resultingState;
        }
        {
            final long replicaPrimaryTerm = randomBoolean() ? primaryTerm : primaryTerm - 1;
            final String replicaAllocationId = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next()
                .allocationId().getId();

            final StartedShardEntry task = new StartedShardEntry(shardId, replicaAllocationId, replicaPrimaryTerm, "test on replica");

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            assertNotSame(clusterState, result.resultingState);
        }
    }

    private ClusterStateTaskExecutor.ClusterTasksResult executeTasks(final ClusterState state,
                                                                     final List<StartedShardEntry> tasks) throws Exception {
        final ClusterStateTaskExecutor.ClusterTasksResult<StartedShardEntry> result = executor.execute(state, tasks);
        assertThat(result, notNullValue());
        return result;
    }
}
