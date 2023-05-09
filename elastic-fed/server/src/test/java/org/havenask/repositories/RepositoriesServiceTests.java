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
import org.havenask.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.havenask.cluster.ClusterChangedEvent;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.RepositoriesMetadata;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterApplierService;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.UUIDs;
import org.havenask.common.blobstore.BlobPath;
import org.havenask.common.blobstore.BlobStore;
import org.havenask.common.component.Lifecycle;
import org.havenask.common.component.LifecycleListener;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.shard.ShardId;
import org.havenask.index.snapshots.IndexShardSnapshotStatus;
import org.havenask.index.store.Store;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.indices.recovery.RecoveryState;
import org.havenask.repositories.blobstore.MeteredBlobStoreRepository;
import org.havenask.snapshots.SnapshotId;
import org.havenask.snapshots.SnapshotInfo;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.Transport;
import org.havenask.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoriesServiceTests extends HavenaskTestCase {

    private RepositoriesService repositoriesService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock(ThreadPool.class);
        final TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(Settings.EMPTY, boundAddress.publishAddress(), UUIDs.randomBase64UUID()), null,
            Collections.emptySet());
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        Map<String, Repository.Factory> typesRegistry =
            org.havenask.common.collect.Map.of(TestRepository.TYPE, TestRepository::new,
                MeteredRepositoryTypeA.TYPE, metadata -> new MeteredRepositoryTypeA(metadata, clusterService),
                MeteredRepositoryTypeB.TYPE, metadata -> new MeteredRepositoryTypeB(metadata, clusterService));
        repositoriesService = new RepositoriesService(Settings.EMPTY, mock(ClusterService.class),
            transportService, typesRegistry, typesRegistry, threadPool);
        repositoriesService.start();
    }

    public void testRegisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertEquals(repoName, repository.getMetadata().name());
        assertEquals(TestRepository.TYPE, repository.getMetadata().type());
        assertEquals(Settings.EMPTY, repository.getMetadata().settings());
        assertTrue(((TestRepository) repository).isStarted);
    }

    public void testUnregisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.unregisterInternalRepository(repoName);
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        assertTrue(((TestRepository) repository).isClosed);
    }

    public void testRegisterWillNotUpdateIfInternalRepositoryWithNameExists() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        assertFalse(((TestRepository) repository).isClosed);
        Repository repository2 = repositoriesService.repository(repoName);
        assertSame(repository, repository2);
    }

    public void testRegisterRejectsInvalidRepositoryNames() {
        assertThrowsOnRegister("");
        assertThrowsOnRegister("contains#InvalidCharacter");
        for (char c : Strings.INVALID_FILENAME_CHARS) {
            assertThrowsOnRegister("contains" + c + "InvalidCharacters");
        }
    }

    public void testRepositoriesStatsCanHaveTheSameNameAndDifferentTypeOverTime() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));

        ClusterState clusterStateWithRepoTypeA = createClusterStateWithRepo(repoName, MeteredRepositoryTypeA.TYPE);

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", emptyState(), clusterStateWithRepoTypeA));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));

        ClusterState clusterStateWithRepoTypeB = createClusterStateWithRepo(repoName, MeteredRepositoryTypeB.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeB, emptyState()));

        List<RepositoryStatsSnapshot> repositoriesStats = repositoriesService.repositoriesStats();
        assertThat(repositoriesStats.size(), equalTo(2));
        RepositoryStatsSnapshot repositoryStatsTypeA = repositoriesStats.get(0);
        assertThat(repositoryStatsTypeA.getRepositoryInfo().type, equalTo(MeteredRepositoryTypeA.TYPE));
        assertThat(repositoryStatsTypeA.getRepositoryStats(), equalTo(MeteredRepositoryTypeA.STATS));

        RepositoryStatsSnapshot repositoryStatsTypeB = repositoriesStats.get(1);
        assertThat(repositoryStatsTypeB.getRepositoryInfo().type, equalTo(MeteredRepositoryTypeB.TYPE));
        assertThat(repositoryStatsTypeB.getRepositoryStats(), equalTo(MeteredRepositoryTypeB.STATS));
    }

    private ClusterState createClusterStateWithRepo(String repoName, String repoType) {
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.putCustom(RepositoriesMetadata.TYPE,
            new RepositoriesMetadata(Collections.singletonList(new RepositoryMetadata(repoName, repoType, Settings.EMPTY))));
        state.metadata(mdBuilder);

        return state.build();
    }

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }

    private void assertThrowsOnRegister(String repoName) {
        PutRepositoryRequest request = new PutRepositoryRequest(repoName);
        expectThrows(RepositoryException.class, () -> repositoriesService.registerRepository(request, null));
    }

    private static class TestRepository implements Repository {

        private static final String TYPE = "internal";
        private boolean isClosed;
        private boolean isStarted;

        private final RepositoryMetadata metadata;

        private TestRepository(RepositoryMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public RepositoryMetadata getMetadata() {
            return metadata;
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
            listener.onResponse(null);
        }

        @Override
        public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, Metadata metadata) {

        }

        @Override
        public void finalizeSnapshot(ShardGenerations shardGenerations, long repositoryStateId, Metadata clusterMetadata,
                                     SnapshotInfo snapshotInfo, Version repositoryMetaVersion,
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
        public void verify(String verificationToken, DiscoveryNode localNode) {

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
        public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId,
                                 RecoveryState recoveryState, ActionListener<Void> listener) {

        }

        @Override
        public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
            return null;
        }

        @Override
        public void updateState(final ClusterState state) {
        }

        @Override
        public void executeConsistentStateUpdate(Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask, String source,
                                                 Consumer<Exception> onFailure) {
        }

        @Override
        public void cloneShardSnapshot(SnapshotId source, SnapshotId target, RepositoryShardId shardId, String shardGeneration,
                                       ActionListener<String> listener) {

        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void start() {
            isStarted = true;
        }

        @Override
        public void stop() {

        }

        @Override
        public void close() {
            isClosed = true;
        }
    }

    private static class MeteredRepositoryTypeA extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-a";
        private static final RepositoryStats STATS = new RepositoryStats(org.havenask.common.collect.Map.of("GET", 10L));

        private MeteredRepositoryTypeA(RepositoryMetadata metadata, ClusterService clusterService) {
            super(metadata,
                false,
                mock(NamedXContentRegistry.class),
                clusterService,
                mock(RecoverySettings.class),
                org.havenask.common.collect.Map.of("bucket", "bucket-a"));
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }

        @Override
        public BlobPath basePath() {
            return BlobPath.cleanPath();
        }
    }

    private static class MeteredRepositoryTypeB extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-b";
        private static final RepositoryStats STATS = new RepositoryStats(org.havenask.common.collect.Map.of("LIST", 20L));

        private MeteredRepositoryTypeB(RepositoryMetadata metadata, ClusterService clusterService) {
            super(metadata,
                false,
                mock(NamedXContentRegistry.class),
                clusterService,
                mock(RecoverySettings.class),
                org.havenask.common.collect.Map.of("bucket", "bucket-b"));
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }

        @Override
        public BlobPath basePath() {
            return BlobPath.cleanPath();
        }
    }
}
