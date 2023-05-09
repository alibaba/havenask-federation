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

package org.havenask.repositories;

import org.apache.lucene.index.IndexCommit;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.component.Lifecycle;
import org.havenask.common.component.LifecycleListener;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.shard.ShardId;
import org.havenask.index.snapshots.IndexShardSnapshotStatus;
import org.havenask.index.store.Store;
import org.havenask.indices.recovery.RecoveryState;
import org.havenask.snapshots.SnapshotId;
import org.havenask.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class FilterRepository implements Repository {

    private final Repository in;

    public FilterRepository(Repository in) {
        this.in = in;
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return in.getMetadata();
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        return in.getSnapshotInfo(snapshotId);
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        return in.getSnapshotGlobalMetadata(snapshotId);
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException {
        return in.getSnapshotIndexMetaData(repositoryData, snapshotId, index);
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        in.getRepositoryData(listener);
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, Metadata metadata) {
        in.initializeSnapshot(snapshotId, indices, metadata);
    }

    @Override
    public void finalizeSnapshot(ShardGenerations shardGenerations, long repositoryStateId, Metadata clusterMetadata,
                                 SnapshotInfo snapshotInfo, Version repositoryMetaVersion,
                                 Function<ClusterState, ClusterState> stateTransformer, ActionListener<RepositoryData> listener) {
        in.finalizeSnapshot(shardGenerations, repositoryStateId, clusterMetadata, snapshotInfo, repositoryMetaVersion, stateTransformer,
            listener);
    }

    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                ActionListener<RepositoryData> listener) {
        in.deleteSnapshots(snapshotIds, repositoryStateId, repositoryMetaVersion, listener);
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return in.getSnapshotThrottleTimeInNanos();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return in.getRestoreThrottleTimeInNanos();
    }

    @Override
    public String startVerification() {
        return in.startVerification();
    }

    @Override
    public void endVerification(String verificationToken) {
        in.endVerification(verificationToken);
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
        in.verify(verificationToken, localNode);
    }

    @Override
    public boolean isReadOnly() {
        return in.isReadOnly();
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, String shardStateIdentifier, IndexShardSnapshotStatus snapshotStatus,
                              Version repositoryMetaVersion, Map<String, Object> userMetadata, ActionListener<String> listener) {
        in.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, shardStateIdentifier, snapshotStatus,
            repositoryMetaVersion, userMetadata, listener);
    }
    @Override
    public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState,
                             ActionListener<Void> listener) {
        in.restoreShard(store, snapshotId, indexId, snapshotShardId, recoveryState, listener);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        return in.getShardSnapshotStatus(snapshotId, indexId, shardId);
    }

    @Override
    public void updateState(ClusterState state) {
        in.updateState(state);
    }

    @Override
    public void executeConsistentStateUpdate(Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask, String source,
                                             Consumer<Exception> onFailure) {
        in.executeConsistentStateUpdate(createUpdateTask, source, onFailure);
    }

    @Override
    public void cloneShardSnapshot(SnapshotId source, SnapshotId target, RepositoryShardId shardId, String shardGeneration,
                                   ActionListener<String> listener) {
        in.cloneShardSnapshot(source, target, shardId, shardGeneration, listener);
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return in.lifecycleState();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        in.addLifecycleListener(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        in.removeLifecycleListener(listener);
    }

    @Override
    public void start() {
        in.start();
    }

    @Override
    public void stop() {
        in.stop();
    }

    @Override
    public void close() {
        in.close();
    }
}
