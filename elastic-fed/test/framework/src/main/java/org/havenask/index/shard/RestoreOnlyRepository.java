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

package org.havenask.index.shard;

import org.apache.lucene.index.IndexCommit;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.snapshots.IndexShardSnapshotStatus;
import org.havenask.index.store.Store;
import org.havenask.repositories.IndexId;
import org.havenask.repositories.IndexMetaDataGenerations;
import org.havenask.repositories.Repository;
import org.havenask.repositories.RepositoryData;
import org.havenask.repositories.RepositoryShardId;
import org.havenask.repositories.ShardGenerations;
import org.havenask.snapshots.SnapshotId;
import org.havenask.snapshots.SnapshotInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.havenask.repositories.RepositoryData.EMPTY_REPO_GEN;

/** A dummy repository for testing which just needs restore overridden */
public abstract class RestoreOnlyRepository extends AbstractLifecycleComponent implements Repository {
    private final String indexName;

    public RestoreOnlyRepository(String indexName) {
        this.indexName = indexName;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return null;
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        return null;
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        return null;
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
        return null;
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        final IndexId indexId = new IndexId(indexName, "blah");
        listener.onResponse(new RepositoryData(EMPTY_REPO_GEN, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            Collections.singletonMap(indexId, emptyList()), ShardGenerations.EMPTY, IndexMetaDataGenerations.EMPTY));
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, Metadata metadata) {
    }

    @Override
    public void finalizeSnapshot(ShardGenerations shardGenerations, long repositoryStateId,
                                 Metadata clusterMetadata, SnapshotInfo snapshotInfo, Version repositoryMetaVersion,
                                 Function<ClusterState, ClusterState> stateTransformer,
                                 ActionListener<RepositoryData> listener) {
        listener.onResponse(null);
    }

    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                ActionListener<RepositoryData> listener) {
        listener.onResponse(null);
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return 0;
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return 0;
    }

    @Override
    public String startVerification() {
        return null;
    }

    @Override
    public void endVerification(String verificationToken) {
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, String shardStateIdentifier, IndexShardSnapshotStatus snapshotStatus,
                              Version repositoryMetaVersion, Map<String, Object> userMetadata, ActionListener<String> listener) {
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        return null;
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
    }

    @Override
    public void updateState(final ClusterState state) {
    }

    @Override
    public void executeConsistentStateUpdate(Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask, String source,
                                             Consumer<Exception> onFailure) {
        throw new UnsupportedOperationException("Unsupported for restore-only repository");
    }

    @Override
    public void cloneShardSnapshot(SnapshotId source, SnapshotId target, RepositoryShardId repositoryShardId, String shardGeneration,
                                   ActionListener<String> listener) {
        throw new UnsupportedOperationException("Unsupported for restore-only repository");
    }
}
