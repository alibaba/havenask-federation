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

import org.havenask.Version;
import org.havenask.action.ActionFuture;
import org.havenask.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.havenask.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.havenask.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotIndexShardStage;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotStats;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.havenask.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.havenask.client.Client;
import org.havenask.cluster.SnapshotsInProgress;
import org.havenask.common.Strings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.TimeValue;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.repositories.blobstore.BlobStoreRepository;
import org.havenask.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class SnapshotStatusApisIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
                .build();
    }

    public void testStatusApiConsistency() {
        createRepository("test-repo", "fs");

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "_doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "_doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        createFullSnapshot("test-repo", "test-snap");

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        final List<SnapshotStatus> snapshotStatus = clusterAdmin().snapshotsStatus(
            new SnapshotsStatusRequest("test-repo", new String[]{"test-snap"})).actionGet().getSnapshots();
        assertThat(snapshotStatus.size(), equalTo(1));
        final SnapshotStatus snStatus = snapshotStatus.get(0);
        assertEquals(snStatus.getStats().getStartTime(), snapshotInfo.startTime());
        assertEquals(snStatus.getStats().getTime(), snapshotInfo.endTime() - snapshotInfo.startTime());
    }

    public void testStatusAPICallInProgressSnapshot() throws Exception {
        createRepository("test-repo", "mock", Settings.builder().put("location", randomRepoPath()).put("block_on_data", true));

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture = startFullSnapshot("test-repo", "test-snap");

        logger.info("--> wait for data nodes to get blocked");
        waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));
        awaitNumberOfSnapshotsInProgress(1);
        assertEquals(SnapshotsInProgress.State.STARTED, client().admin().cluster().prepareSnapshotStatus("test-repo")
                .setSnapshots("test-snap").get().getSnapshots().get(0).getState());

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes("test-repo");

        logger.info("--> wait for snapshot to finish");
        createSnapshotResponseActionFuture.actionGet();
    }

    public void testExceptionOnMissingSnapBlob() throws IOException {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository");

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");
        logger.info("--> delete snap-${uuid}.dat file for this snapshot to simulate concurrent delete");
        IOUtils.rm(repoPath.resolve(BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat"));

        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster()
            .getSnapshots(new GetSnapshotsRequest("test-repo", new String[] {"test-snap"})).actionGet());
    }

    public void testExceptionOnMissingShardLevelSnapBlob() throws IOException {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository");

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");

        logger.info("--> delete shard-level snap-${uuid}.dat file for one shard in this snapshot to simulate concurrent delete");
        final String indexRepoId = getRepositoryData("test-repo").resolveIndexId(snapshotInfo.indices().get(0)).getId();
        IOUtils.rm(repoPath.resolve("indices").resolve(indexRepoId).resolve("0").resolve(
            BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat"));

        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster()
            .prepareSnapshotStatus("test-repo").setSnapshots("test-snap").execute().actionGet());
    }

    public void testGetSnapshotsWithoutIndices() throws Exception {
        createRepository("test-repo", "fs");

        logger.info("--> snapshot");
        final SnapshotInfo snapshotInfo = assertSuccessful(client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                .setIndices().setWaitForCompletion(true).execute());
        assertThat(snapshotInfo.totalShards(), is(0));

        logger.info("--> verify that snapshot without index shows up in non-verbose listing");
        final List<SnapshotInfo> snapshotInfos =
            client().admin().cluster().prepareGetSnapshots("test-repo").setVerbose(false).get().getSnapshots();
        assertThat(snapshotInfos, hasSize(1));
        final SnapshotInfo found = snapshotInfos.get(0);
        assertThat(found.snapshotId(), is(snapshotInfo.snapshotId()));
        assertThat(found.state(), is(SnapshotState.SUCCESS));
    }

    /**
     * Tests the following sequence of steps:
     * 1. Start snapshot of two shards (both located on separate data nodes).
     * 2. Have one of the shards snapshot completely and the other block
     * 3. Restart the data node that completed its shard snapshot
     * 4. Make sure that snapshot status APIs show correct file-counts and -sizes
     *
     * @throws Exception on failure
     */
    public void testCorrectCountsForDoneShards() throws Exception {
        final String indexOne = "index-1";
        final String indexTwo = "index-2";
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String dataNodeOne = dataNodes.get(0);
        final String dataNodeTwo = dataNodes.get(1);

        createIndex(indexOne, singleShardOneNode(dataNodeOne));
        index(indexOne, "_doc", "some_doc_id", "foo", "bar");
        createIndex(indexTwo, singleShardOneNode(dataNodeTwo));
        index(indexTwo, "_doc", "some_doc_id", "foo", "bar");

        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        blockDataNode(repoName, dataNodeOne);

        final String snapshotOne = "snap-1";
        // restarting a data node below so using a master client here
        final ActionFuture<CreateSnapshotResponse> responseSnapshotOne = internalCluster().masterClient().admin()
            .cluster().prepareCreateSnapshot(repoName, snapshotOne).setWaitForCompletion(true).execute();

        assertBusy(() -> {
            final SnapshotStatus snapshotStatusOne = getSnapshotStatus(repoName, snapshotOne);
            assertThat(snapshotStatusOne.getState(), is(SnapshotsInProgress.State.STARTED));
            final SnapshotIndexShardStatus snapshotShardState = stateFirstShard(snapshotStatusOne, indexTwo);
            assertThat(snapshotShardState.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardState.getStats().getTotalFileCount(), greaterThan(0));
            assertThat(snapshotShardState.getStats().getTotalSize(), greaterThan(0L));
        }, 30L, TimeUnit.SECONDS);

        final SnapshotStats snapshotShardStats =
            stateFirstShard(getSnapshotStatus(repoName, snapshotOne), indexTwo).getStats();
        final int totalFiles = snapshotShardStats.getTotalFileCount();
        final long totalFileSize = snapshotShardStats.getTotalSize();

        internalCluster().restartNode(dataNodeTwo);

        final SnapshotIndexShardStatus snapshotShardStateAfterNodeRestart =
            stateFirstShard(getSnapshotStatus(repoName, snapshotOne), indexTwo);
        assertThat(snapshotShardStateAfterNodeRestart.getStage(), is(SnapshotIndexShardStage.DONE));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalFileCount(), equalTo(totalFiles));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalSize(), equalTo(totalFileSize));

        unblockAllDataNodes(repoName);
        assertThat(responseSnapshotOne.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));

        // indexing another document to the second index so it will do writes during the snapshot and we can block on those writes
        index(indexTwo, "_doc", "some_other_doc_id", "foo", "other_bar");

        blockDataNode(repoName, dataNodeTwo);

        final String snapshotTwo = "snap-2";
        final ActionFuture<CreateSnapshotResponse> responseSnapshotTwo =
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshotTwo).setWaitForCompletion(true).execute();

        waitForBlock(dataNodeTwo, repoName, TimeValue.timeValueSeconds(30L));

        assertBusy(() -> {
            final SnapshotStatus snapshotStatusOne = getSnapshotStatus(repoName, snapshotOne);
            final SnapshotStatus snapshotStatusTwo = getSnapshotStatus(repoName, snapshotTwo);
            final SnapshotIndexShardStatus snapshotShardStateOne = stateFirstShard(snapshotStatusOne, indexOne);
            final SnapshotIndexShardStatus snapshotShardStateTwo = stateFirstShard(snapshotStatusTwo, indexOne);
            assertThat(snapshotShardStateOne.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardStateTwo.getStage(), is(SnapshotIndexShardStage.DONE));
            final int totalFilesShardOne = snapshotShardStateOne.getStats().getTotalFileCount();
            final long totalSizeShardOne = snapshotShardStateOne.getStats().getTotalSize();
            assertThat(totalFilesShardOne, greaterThan(0));
            assertThat(totalSizeShardOne, greaterThan(0L));
            assertThat(totalFilesShardOne, equalTo(snapshotShardStateTwo.getStats().getTotalFileCount()));
            assertThat(totalSizeShardOne, equalTo(snapshotShardStateTwo.getStats().getTotalSize()));
            assertThat(snapshotShardStateTwo.getStats().getIncrementalFileCount(), equalTo(0));
            assertThat(snapshotShardStateTwo.getStats().getIncrementalSize(), equalTo(0L));
        }, 30L, TimeUnit.SECONDS);

        unblockAllDataNodes(repoName);
        assertThat(responseSnapshotTwo.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
    }

    public void testSnapshotStatusOnFailedSnapshot() throws Exception {
        String repoName = "test-repo";
        createRepository(repoName, "fs");
        final String snapshot = "test-snap-1";
        addBwCFailedSnapshot(repoName, snapshot, Collections.emptyMap());

        logger.info("--> creating good index");
        assertAcked(prepareCreate("test-idx-good").setSettings(indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs("test-idx-good", randomIntBetween(1, 5));

        final SnapshotsStatusResponse snapshotsStatusResponse =
                client().admin().cluster().prepareSnapshotStatus(repoName).setSnapshots(snapshot).get();
        assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
        assertEquals(SnapshotsInProgress.State.FAILED, snapshotsStatusResponse.getSnapshots().get(0).getState());
    }

    public void testGetSnapshotsRequest() throws Exception {
        final String repositoryName = "test-repo";
        final String indexName = "test-idx";
        final Client client = client();

        createRepository(repositoryName, "mock", Settings.builder()
                .put("location", randomRepoPath()).put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES).put("wait_after_unblock", 200));

        logger.info("--> get snapshots on an empty repository");
        expectThrows(SnapshotMissingException.class, () -> client.admin()
                .cluster()
                .prepareGetSnapshots(repositoryName)
                .addSnapshots("non-existent-snapshot")
                .get());
        // with ignore unavailable set to true, should not throw an exception
        GetSnapshotsResponse getSnapshotsResponse = client.admin()
                .cluster()
                .prepareGetSnapshots(repositoryName)
                .setIgnoreUnavailable(true)
                .addSnapshots("non-existent-snapshot")
                .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(0));

        logger.info("--> creating an index and indexing documents");
        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate(indexName, 1, Settings.builder().put("number_of_replicas", 0)));
        ensureGreen();
        indexRandomDocs(indexName, 10);

        // make sure we return only the in-progress snapshot when taking the first snapshot on a clean repository
        // take initial snapshot with a block, making sure we only get 1 in-progress snapshot returned
        // block a node so the create snapshot operation can remain in progress
        final String initialBlockedNode = blockNodeWithIndex(repositoryName, indexName);
        client.admin().cluster().prepareCreateSnapshot(repositoryName, "snap-on-empty-repo")
                .setWaitForCompletion(false)
                .setIndices(indexName)
                .get();
        waitForBlock(initialBlockedNode, repositoryName, TimeValue.timeValueSeconds(60)); // wait for block to kick in
        getSnapshotsResponse = client.admin().cluster()
                .prepareGetSnapshots("test-repo")
                .setSnapshots(randomFrom("_all", "_current", "snap-on-*", "*-on-empty-repo", "snap-on-empty-repo"))
                .get();
        assertEquals(1, getSnapshotsResponse.getSnapshots().size());
        assertEquals("snap-on-empty-repo", getSnapshotsResponse.getSnapshots().get(0).snapshotId().getName());
        unblockNode(repositoryName, initialBlockedNode); // unblock node
        admin().cluster().prepareDeleteSnapshot(repositoryName, "snap-on-empty-repo").get();

        final int numSnapshots = randomIntBetween(1, 3) + 1;
        logger.info("--> take {} snapshot(s)", numSnapshots - 1);
        final String[] snapshotNames = new String[numSnapshots];
        for (int i = 0; i < numSnapshots - 1; i++) {
            final String snapshotName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            CreateSnapshotResponse createSnapshotResponse = client.admin()
                    .cluster()
                    .prepareCreateSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(true)
                    .setIndices(indexName)
                    .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            snapshotNames[i] = snapshotName;
        }
        logger.info("--> take another snapshot to be in-progress");
        // add documents so there are data files to block on
        for (int i = 10; i < 20; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final String inProgressSnapshot = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        snapshotNames[numSnapshots - 1] = inProgressSnapshot;
        // block a node so the create snapshot operation can remain in progress
        final String blockedNode = blockNodeWithIndex(repositoryName, indexName);
        client.admin().cluster().prepareCreateSnapshot(repositoryName, inProgressSnapshot)
                .setWaitForCompletion(false)
                .setIndices(indexName)
                .get();
        waitForBlock(blockedNode, repositoryName, TimeValue.timeValueSeconds(60)); // wait for block to kick in

        logger.info("--> get all snapshots with a current in-progress");
        // with ignore unavailable set to true, should not throw an exception
        final List<String> snapshotsToGet = new ArrayList<>();
        if (randomBoolean()) {
            // use _current plus the individual names of the finished snapshots
            snapshotsToGet.add("_current");
            for (int i = 0; i < numSnapshots - 1; i++) {
                snapshotsToGet.add(snapshotNames[i]);
            }
        } else {
            snapshotsToGet.add("_all");
        }
        getSnapshotsResponse = client.admin().cluster()
                .prepareGetSnapshots(repositoryName)
                .setSnapshots(snapshotsToGet.toArray(Strings.EMPTY_ARRAY))
                .get();
        List<String> sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream()
                .map(s -> s.snapshotId().getName())
                .sorted()
                .collect(Collectors.toList()), equalTo(sortedNames));

        getSnapshotsResponse = client.admin().cluster()
                .prepareGetSnapshots(repositoryName)
                .addSnapshots(snapshotNames)
                .get();
        sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream()
                .map(s -> s.snapshotId().getName())
                .sorted()
                .collect(Collectors.toList()), equalTo(sortedNames));

        logger.info("--> make sure duplicates are not returned in the response");
        String regexName = snapshotNames[randomIntBetween(0, numSnapshots - 1)];
        final int splitPos = regexName.length() / 2;
        final String firstRegex = regexName.substring(0, splitPos) + "*";
        final String secondRegex = "*" + regexName.substring(splitPos);
        getSnapshotsResponse = client.admin().cluster()
                .prepareGetSnapshots(repositoryName)
                .addSnapshots(snapshotNames)
                .addSnapshots(firstRegex, secondRegex)
                .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream()
                .map(s -> s.snapshotId().getName())
                .sorted()
                .collect(Collectors.toList()), equalTo(sortedNames));

        unblockNode(repositoryName, blockedNode); // unblock node
        waitForCompletion(repositoryName, inProgressSnapshot, TimeValue.timeValueSeconds(60));
    }

    private static SnapshotIndexShardStatus stateFirstShard(SnapshotStatus snapshotStatus, String indexName) {
        return snapshotStatus.getIndices().get(indexName).getShards().get(0);
    }

    private static SnapshotStatus getSnapshotStatus(String repoName, String snapshotName) {
        try {
            return client().admin().cluster().prepareSnapshotStatus(repoName).setSnapshots(snapshotName)
                .get().getSnapshots().get(0);
        } catch (SnapshotMissingException e) {
            throw new AssertionError(e);
        }
    }

    private static Settings singleShardOneNode(String node) {
        return indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", node).build();
    }
}
