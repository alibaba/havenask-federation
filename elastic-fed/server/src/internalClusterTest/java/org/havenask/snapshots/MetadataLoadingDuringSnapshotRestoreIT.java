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

package org.havenask.snapshots;

import org.havenask.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.havenask.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.Environment;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.IndexId;
import org.havenask.repositories.RepositoriesService;
import org.havenask.repositories.Repository;
import org.havenask.repositories.RepositoryData;
import org.havenask.rest.RestStatus;
import org.havenask.snapshots.mockstore.MockRepository;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * This class tests whether global and index metadata are only loaded from the repository when needed.
*/
public class MetadataLoadingDuringSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /// This test uses a snapshot/restore plugin implementation that
        // counts the number of times metadata are loaded
        return Collections.singletonList(CountingMockRepositoryPlugin.class);
    }

    public void testWhenMetadataAreLoaded() throws Exception {
        createIndex("docs");
        indexRandom(true,
            client().prepareIndex("docs", "doc", "1").setSource("rank", 1),
            client().prepareIndex("docs", "doc", "2").setSource("rank", 2),
            client().prepareIndex("docs", "doc", "3").setSource("rank", 3),
            client().prepareIndex("others", "other").setSource("rank", 4),
            client().prepareIndex("others", "other").setSource("rank", 5));

        createRepository("repository", CountingMockRepositoryPlugin.TYPE);

        // Creating a snapshot does not load any metadata
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("repository", "snap")
                                                                                    .setIncludeGlobalState(true)
                                                                                    .setWaitForCompletion(true)
                                                                                    .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().failedShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 0);
        assertIndexMetadataLoads("snap", "others", 0);

        // Getting a snapshot does not load any metadata
        GetSnapshotsResponse getSnapshotsResponse =
            client().admin().cluster().prepareGetSnapshots("repository").addSnapshots("snap").setVerbose(randomBoolean()).get();
        assertThat(getSnapshotsResponse.getSnapshots(), hasSize(1));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 0);
        assertIndexMetadataLoads("snap", "others", 0);

        // Getting the status of a snapshot loads indices metadata but not global metadata
        SnapshotsStatusResponse snapshotStatusResponse =
            client().admin().cluster().prepareSnapshotStatus("repository").setSnapshots("snap").get();
        assertThat(snapshotStatusResponse.getSnapshots(), hasSize(1));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 1);
        assertIndexMetadataLoads("snap", "others", 1);

        assertAcked(client().admin().indices().prepareDelete("docs", "others"));

        // Restoring a snapshot loads indices metadata but not the global state
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("repository", "snap")
                                                                                    .setWaitForCompletion(true)
                                                                                    .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 2);
        assertIndexMetadataLoads("snap", "others", 2);

        assertAcked(client().admin().indices().prepareDelete("docs"));

        // Restoring a snapshot with selective indices loads only required index metadata
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("repository", "snap")
                                                            .setIndices("docs")
                                                            .setWaitForCompletion(true)
                                                            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 3);
        assertIndexMetadataLoads("snap", "others", 2);

        assertAcked(client().admin().indices().prepareDelete("docs", "others"));

        // Restoring a snapshot including the global state loads it with the index metadata
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("repository", "snap")
            .setIndices("docs", "oth*")
            .setRestoreGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 1);
        assertIndexMetadataLoads("snap", "docs", 4);
        assertIndexMetadataLoads("snap", "others", 3);

        // Deleting a snapshot does not load the global metadata state but loads each index metadata
        assertAcked(client().admin().cluster().prepareDeleteSnapshot("repository", "snap").get());
        assertGlobalMetadataLoads("snap", 1);
        assertIndexMetadataLoads("snap", "docs", 4);
        assertIndexMetadataLoads("snap", "others", 3);
    }

    private void assertGlobalMetadataLoads(final String snapshot, final int times) {
        AtomicInteger count = getCountingMockRepository().globalMetadata.get(snapshot);
        if (times == 0) {
            assertThat("Global metadata for " + snapshot + " must not have been loaded", count, nullValue());
        } else {
            assertThat("Global metadata for " + snapshot + " must have been loaded " + times + " times", count.get(), equalTo(times));
        }
    }

    private void assertIndexMetadataLoads(final String snapshot, final String index, final int times) {
        final String key = key(snapshot, index);
        AtomicInteger count = getCountingMockRepository().indicesMetadata.get(key);
        if (times == 0) {
            assertThat("Index metadata for " + key + " must not have been loaded", count, nullValue());
        } else {
            assertThat("Index metadata for " + key + " must have been loaded " + times + " times", count.get(), equalTo(times));
        }
    }

    private CountingMockRepository getCountingMockRepository() {
        String master = internalCluster().getMasterName();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, master);
        Repository repository = repositoriesService.repository("repository");
        assertThat(repository, instanceOf(CountingMockRepository.class));
        return  (CountingMockRepository) repository;
    }

    /** Compute a map key for the given snapshot and index names **/
    private static String key(final String snapshot, final String index) {
        return snapshot + ":" + index;
    }

    /** A mocked repository that counts the number of times global/index metadata are accessed **/
    public static class CountingMockRepository extends MockRepository {

        final Map<String, AtomicInteger> globalMetadata = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> indicesMetadata = new ConcurrentHashMap<>();

        public CountingMockRepository(final RepositoryMetadata metadata,
                                      final Environment environment,
                                      final NamedXContentRegistry namedXContentRegistry, ClusterService clusterService,
                                      RecoverySettings recoverySettings) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
            globalMetadata.computeIfAbsent(snapshotId.getName(), (s) -> new AtomicInteger(0)).incrementAndGet();
            return super.getSnapshotGlobalMetadata(snapshotId);
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId,
                                                      IndexId indexId) throws IOException {
            indicesMetadata.computeIfAbsent(key(snapshotId.getName(), indexId.getName()), (s) -> new AtomicInteger(0)).incrementAndGet();
            return super.getSnapshotIndexMetaData(PlainActionFuture.get(this::getRepositoryData), snapshotId, indexId);
        }
    }

    /** A plugin that uses CountingMockRepository as implementation of the Repository **/
    public static class CountingMockRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = "countingmock";

        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                               ClusterService clusterService, RecoverySettings recoverySettings) {
            return Collections.singletonMap(TYPE,
                metadata -> new CountingMockRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings));
        }
    }
}
