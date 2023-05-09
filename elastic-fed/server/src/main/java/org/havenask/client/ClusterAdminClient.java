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

import org.havenask.action.ActionFuture;
import org.havenask.action.ActionListener;
import org.havenask.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.havenask.action.admin.cluster.allocation.ClusterAllocationExplainRequestBuilder;
import org.havenask.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.havenask.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.havenask.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.havenask.action.admin.cluster.node.info.NodesInfoRequest;
import org.havenask.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.havenask.action.admin.cluster.node.info.NodesInfoResponse;
import org.havenask.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequestBuilder;
import org.havenask.action.admin.cluster.node.stats.NodesStatsRequest;
import org.havenask.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.havenask.action.admin.cluster.node.stats.NodesStatsResponse;
import org.havenask.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.havenask.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.havenask.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.havenask.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.havenask.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.havenask.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.action.admin.cluster.node.usage.NodesUsageRequest;
import org.havenask.action.admin.cluster.node.usage.NodesUsageRequestBuilder;
import org.havenask.action.admin.cluster.node.usage.NodesUsageResponse;
import org.havenask.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.havenask.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequestBuilder;
import org.havenask.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.havenask.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.havenask.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.havenask.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.havenask.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.havenask.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.havenask.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.havenask.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.havenask.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.havenask.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.havenask.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.havenask.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.havenask.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.havenask.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.havenask.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.havenask.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.havenask.action.admin.cluster.snapshots.clone.CloneSnapshotRequestBuilder;
import org.havenask.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.havenask.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.havenask.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.havenask.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.havenask.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.havenask.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.havenask.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.havenask.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.havenask.action.admin.cluster.state.ClusterStateRequest;
import org.havenask.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.havenask.action.admin.cluster.state.ClusterStateResponse;
import org.havenask.action.admin.cluster.stats.ClusterStatsRequest;
import org.havenask.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.havenask.action.admin.cluster.stats.ClusterStatsResponse;
import org.havenask.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.havenask.action.admin.cluster.storedscripts.DeleteStoredScriptRequestBuilder;
import org.havenask.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.havenask.action.admin.cluster.storedscripts.GetStoredScriptRequestBuilder;
import org.havenask.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.havenask.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.havenask.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.havenask.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.havenask.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.havenask.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.havenask.action.admin.indices.dangling.delete.DeleteDanglingIndexRequest;
import org.havenask.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.havenask.action.ingest.DeletePipelineRequest;
import org.havenask.action.ingest.DeletePipelineRequestBuilder;
import org.havenask.action.ingest.GetPipelineRequest;
import org.havenask.action.ingest.GetPipelineRequestBuilder;
import org.havenask.action.ingest.GetPipelineResponse;
import org.havenask.action.ingest.PutPipelineRequest;
import org.havenask.action.ingest.PutPipelineRequestBuilder;
import org.havenask.action.ingest.SimulatePipelineRequest;
import org.havenask.action.ingest.SimulatePipelineRequestBuilder;
import org.havenask.action.ingest.SimulatePipelineResponse;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.XContentType;
import org.havenask.tasks.TaskId;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 */
public interface ClusterAdminClient extends HavenaskClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     * @see Requests#clusterHealthRequest(String...)
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     *
     * @param request  The cluster state request
     * @param listener A listener to be notified with a result
     * @see Requests#clusterHealthRequest(String...)
     */
    void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener);

    /**
     * The health of the cluster.
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * The state of the cluster.
     *
     * @param request The cluster state request.
     * @return The result future
     * @see Requests#clusterStateRequest()
     */
    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    /**
     * The state of the cluster.
     *
     * @param request  The cluster state request.
     * @param listener A listener to be notified with a result
     * @see Requests#clusterStateRequest()
     */
    void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    /**
     * The state of the cluster.
     */
    ClusterStateRequestBuilder prepareState();

    /**
     * Updates settings in the cluster.
     */
    ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request);

    /**
     * Update settings in the cluster.
     */
    void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterUpdateSettingsRequestBuilder prepareUpdateSettings();

    /**
     * Re initialize each cluster node and pass them the secret store password.
     */
    NodesReloadSecureSettingsRequestBuilder prepareReloadSecureSettings();

    /**
     * Reroutes allocation of shards. Advance API.
     */
    ActionFuture<ClusterRerouteResponse> reroute(ClusterRerouteRequest request);

    /**
     * Reroutes allocation of shards. Advance API.
     */
    void reroute(ClusterRerouteRequest request, ActionListener<ClusterRerouteResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterRerouteRequestBuilder prepareReroute();

    /**
     * Nodes info of the cluster.
     *
     * @param request The nodes info request
     * @return The result future
     * @see org.havenask.client.Requests#nodesInfoRequest(String...)
     */
    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request);

    /**
     * Nodes info of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see org.havenask.client.Requests#nodesInfoRequest(String...)
     */
    void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    /**
     * Nodes info of the cluster.
     */
    NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds);

    /**
     * Cluster wide aggregated stats.
     *
     * @param request The cluster stats request
     * @return The result future
     * @see org.havenask.client.Requests#clusterStatsRequest
     */
    ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request);

    /**
     * Cluster wide aggregated stats
     *
     * @param request  The cluster stats request
     * @param listener A listener to be notified with a result
     * @see org.havenask.client.Requests#clusterStatsRequest()
     */
    void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener);

    ClusterStatsRequestBuilder prepareClusterStats();

    /**
     * Nodes stats of the cluster.
     *
     * @param request The nodes stats request
     * @return The result future
     * @see org.havenask.client.Requests#nodesStatsRequest(String...)
     */
    ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see org.havenask.client.Requests#nodesStatsRequest(String...)
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    /**
     * Nodes stats of the cluster.
     */
    NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request. Nodes usage of the
     * cluster.
     *
     * @param request
     *            The nodes usage request
     * @return The result future
     * @see org.havenask.client.Requests#nodesUsageRequest(String...)
     */
    ActionFuture<NodesUsageResponse> nodesUsage(NodesUsageRequest request);

    /**
     * Nodes usage of the cluster.
     *
     * @param request
     *            The nodes usage request
     * @param listener
     *            A listener to be notified with a result
     * @see org.havenask.client.Requests#nodesUsageRequest(String...)
     */
    void nodesUsage(NodesUsageRequest request, ActionListener<NodesUsageResponse> listener);

    /**
     * Nodes usage of the cluster.
     */
    NodesUsageRequestBuilder prepareNodesUsage(String... nodesIds);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request.
     *
     */
    ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids specified in the request.
     */
    void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener);

    /**
     * Returns a request builder to fetch top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids provided. Note: Use {@code *} to fetch samples for all nodes
     */
    NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds);

    /**
     * List tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     * @see org.havenask.client.Requests#listTasksRequest()
     */
    ActionFuture<ListTasksResponse> listTasks(ListTasksRequest request);

    /**
     * List active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     * @see org.havenask.client.Requests#listTasksRequest()
     */
    void listTasks(ListTasksRequest request, ActionListener<ListTasksResponse> listener);

    /**
     * List active tasks
     */
    ListTasksRequestBuilder prepareListTasks(String... nodesIds);

    /**
     * Get a task.
     *
     * @param request the request
     * @return the result future
     * @see org.havenask.client.Requests#getTaskRequest()
     */
    ActionFuture<GetTaskResponse> getTask(GetTaskRequest request);

    /**
     * Get a task.
     *
     * @param request the request
     * @param listener A listener to be notified with the result
     * @see org.havenask.client.Requests#getTaskRequest()
     */
    void getTask(GetTaskRequest request, ActionListener<GetTaskResponse> listener);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(String taskId);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(TaskId taskId);

    /**
     * Cancel tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     * @see org.havenask.client.Requests#cancelTasksRequest()
     */
    ActionFuture<CancelTasksResponse> cancelTasks(CancelTasksRequest request);

    /**
     * Cancel active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     * @see org.havenask.client.Requests#cancelTasksRequest()
     */
    void cancelTasks(CancelTasksRequest request, ActionListener<CancelTasksResponse> listener);

    /**
     * Cancel active tasks
     */
    CancelTasksRequestBuilder prepareCancelTasks(String... nodesIds);

    /**
     * Returns list of shards the given search would be executed on.
     */
    ActionFuture<ClusterSearchShardsResponse> searchShards(ClusterSearchShardsRequest request);

    /**
     * Returns list of shards the given search would be executed on.
     */
    void searchShards(ClusterSearchShardsRequest request, ActionListener<ClusterSearchShardsResponse> listener);

    /**
     * Returns list of shards the given search would be executed on.
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards();

    /**
     * Returns list of shards the given search would be executed on.
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices);

    /**
     * Registers a snapshot repository.
     */
    ActionFuture<AcknowledgedResponse> putRepository(PutRepositoryRequest request);

    /**
     * Registers a snapshot repository.
     */
    void putRepository(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Registers a snapshot repository.
     */
    PutRepositoryRequestBuilder preparePutRepository(String name);

    /**
     * Unregisters a repository.
     */
    ActionFuture<AcknowledgedResponse> deleteRepository(DeleteRepositoryRequest request);

    /**
     * Unregisters a repository.
     */
    void deleteRepository(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Unregisters a repository.
     */
    DeleteRepositoryRequestBuilder prepareDeleteRepository(String name);

    /**
     * Gets repositories.
     */
    ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request);

    /**
     * Gets repositories.
     */
    void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener);

    /**
     * Gets repositories.
     */
    GetRepositoriesRequestBuilder prepareGetRepositories(String... name);

    /**
     * Cleans up repository.
     */
    CleanupRepositoryRequestBuilder prepareCleanupRepository(String repository);

    /**
     * Cleans up repository.
     */
    ActionFuture<CleanupRepositoryResponse> cleanupRepository(CleanupRepositoryRequest repository);

    /**
     * Cleans up repository.
     */
    void cleanupRepository(CleanupRepositoryRequest repository, ActionListener<CleanupRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request);

    /**
     * Verifies a repository.
     */
    void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    VerifyRepositoryRequestBuilder prepareVerifyRepository(String name);

    /**
     * Creates a new snapshot.
     */
    ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request);

    /**
     * Creates a new snapshot.
     */
    void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener);

    /**
     * Creates a new snapshot.
     */
    CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name);

    /**
     * Clones a snapshot.
     */
    CloneSnapshotRequestBuilder prepareCloneSnapshot(String repository, String source, String target);

    /**
     * Clones a snapshot.
     */
    ActionFuture<AcknowledgedResponse> cloneSnapshot(CloneSnapshotRequest request);

    /**
     * Clones a snapshot.
     */
    void cloneSnapshot(CloneSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Get snapshots.
     */
    ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request);

    /**
     * Get snapshot.
     */
    void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener);

    /**
     * Get snapshot.
     */
    GetSnapshotsRequestBuilder prepareGetSnapshots(String repository);

    /**
     * Delete snapshot.
     */
    ActionFuture<AcknowledgedResponse> deleteSnapshot(DeleteSnapshotRequest request);

    /**
     * Delete snapshot.
     */
    void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete snapshot.
     */
    DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String... snapshot);

    /**
     * Restores a snapshot.
     */
    ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request);

    /**
     * Restores a snapshot.
     */
    void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener);

    /**
     * Restores a snapshot.
     */
    RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    PendingClusterTasksRequestBuilder preparePendingClusterTasks();

    /**
     * Get snapshot status.
     */
    ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request);

    /**
     * Get snapshot status.
     */
    void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus();

    /**
     * Stores an ingest pipeline
     */
    void putPipeline(PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Stores an ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> putPipeline(PutPipelineRequest request);

    /**
     * Stores an ingest pipeline
     */
    PutPipelineRequestBuilder preparePutPipeline(String id, BytesReference source, XContentType xContentType);

    /**
     * Deletes a stored ingest pipeline
     */
    void deletePipeline(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes a stored ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> deletePipeline(DeletePipelineRequest request);

    /**
     * Deletes a stored ingest pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline();

    /**
     * Deletes a stored ingest pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline(String id);

    /**
     * Returns a stored ingest pipeline
     */
    void getPipeline(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener);

    /**
     * Returns a stored ingest pipeline
     */
    ActionFuture<GetPipelineResponse> getPipeline(GetPipelineRequest request);

    /**
     * Returns a stored ingest pipeline
     */
    GetPipelineRequestBuilder prepareGetPipeline(String... ids);

    /**
     * Simulates an ingest pipeline
     */
    void simulatePipeline(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener);

    /**
     * Simulates an ingest pipeline
     */
    ActionFuture<SimulatePipelineResponse> simulatePipeline(SimulatePipelineRequest request);

    /**
     * Simulates an ingest pipeline
     */
    SimulatePipelineRequestBuilder prepareSimulatePipeline(BytesReference source, XContentType xContentType);

    /**
     * Explain the allocation of a shard
     */
    void allocationExplain(ClusterAllocationExplainRequest request, ActionListener<ClusterAllocationExplainResponse> listener);

    /**
     * Explain the allocation of a shard
     */
    ActionFuture<ClusterAllocationExplainResponse> allocationExplain(ClusterAllocationExplainRequest request);

    /**
     * Explain the allocation of a shard
     */
    ClusterAllocationExplainRequestBuilder prepareAllocationExplain();

    /**
     * Store a script in the cluster state
     */
    PutStoredScriptRequestBuilder preparePutStoredScript();

    /**
     * Delete a script from the cluster state
     */
    void deleteStoredScript(DeleteStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete a script from the cluster state
     */
    ActionFuture<AcknowledgedResponse> deleteStoredScript(DeleteStoredScriptRequest request);

    /**
     * Delete a script from the cluster state
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript();

    /**
     * Delete a script from the cluster state
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript(String id);

    /**
     * Store a script in the cluster state
     */
    void putStoredScript(PutStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Store a script in the cluster state
     */
    ActionFuture<AcknowledgedResponse> putStoredScript(PutStoredScriptRequest request);

    /**
     * Get a script from the cluster state
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript();

    /**
     * Get a script from the cluster state
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript(String id);

    /**
     * Get a script from the cluster state
     */
    void getStoredScript(GetStoredScriptRequest request, ActionListener<GetStoredScriptResponse> listener);

    /**
     * Get a script from the cluster state
     */
    ActionFuture<GetStoredScriptResponse> getStoredScript(GetStoredScriptRequest request);

    /**
     * List dangling indices on all nodes.
     */
    void listDanglingIndices(ListDanglingIndicesRequest request, ActionListener<ListDanglingIndicesResponse> listener);

    /**
     * List dangling indices on all nodes.
     */
    ActionFuture<ListDanglingIndicesResponse> listDanglingIndices(ListDanglingIndicesRequest request);

    /**
     * Restore specified dangling indices.
     */
    void importDanglingIndex(ImportDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Restore specified dangling indices.
     */
    ActionFuture<AcknowledgedResponse> importDanglingIndex(ImportDanglingIndexRequest request);

    /**
     * Delete specified dangling indices.
     */
    void deleteDanglingIndex(DeleteDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete specified dangling indices.
     */
    ActionFuture<AcknowledgedResponse> deleteDanglingIndex(DeleteDanglingIndexRequest request);
}
