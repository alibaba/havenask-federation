/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.repositories;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.havenask.ArpcThreadLeakFilterIT;
import org.havenask.HttpThreadLeakFilterIT;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.blobstore.HavenaskBlobStoreRepositoryIntegTestCase;
import org.havenask.repositories.fs.FsRepository;
import org.havenask.snapshots.SnapshotMissingException;
import org.havenask.snapshots.SnapshotRestoreException;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.havenask.engine.HavenaskInternalClusterTestCase.havenaskNodeSettings;
import static org.havenask.test.HavenaskIntegTestCase.Scope.SUITE;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;

@ThreadLeakFilters(filters = { HttpThreadLeakFilterIT.class, ArpcThreadLeakFilterIT.class })
@HavenaskIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 3, numClientNodes = 0, scope = SUITE)
public class HavenaskFsRepositoryIT extends HavenaskBlobStoreRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return FsRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.repositorySettings());
        settings.put("location", randomRepoPath());
        if (randomBoolean()) {
            long size = 1 << randomInt(10);
            settings.put("chunk_size", new ByteSizeValue(size, ByteSizeUnit.KB));
        }
        return settings.build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HavenaskEnginePlugin.HAVENASK_SET_DEFAULT_ENGINE_SETTING.getKey(), true)
            .build();
        return havenaskNodeSettings(settings, nodeOrdinal);
    }

    public void testSnapshotAndRestore() throws Exception {
        final String repoName = createRepository(randomName());
        int indexCount = randomIntBetween(1, 5);
        int[] docCounts = new int[indexCount];
        String[] indexNames = generateRandomNames(indexCount);
        for (int i = 0; i < indexCount; i++) {
            createIndex(
                indexNames[i],
                Settings.builder()
                    .put(EngineSettings.HAVENASK_BUILD_CONFIG_MAX_DOC_COUNT.getKey(), 1)
                    .build()
            );
        }

        ensureGreen();

        for (int i = 0; i < indexCount; i++) {
            docCounts[i] = iterations(10, 1000);
            logger.info("-->  create random index {} with {} records", indexNames[i], docCounts[i]);
            addRandomDocuments(indexNames[i], docCounts[i]);
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(docCounts[i]).get(), docCounts[i]);
        }

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        assertSuccessfulSnapshot(
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).setIndices(indexNames)
        );

        List<String> deleteIndices = randomSubsetOf(randomIntBetween(0, indexCount), indexNames);
        if (deleteIndices.size() > 0) {
            logger.info("-->  delete indices {}", deleteIndices);
            assertAcked(client().admin().indices().prepareDelete(deleteIndices.toArray(new String[deleteIndices.size()])));
        }

        Set<String> closeIndices = new HashSet<>(Arrays.asList(indexNames));
        closeIndices.removeAll(deleteIndices);

        if (closeIndices.size() > 0) {
            for (String index : closeIndices) {
                if (randomBoolean()) {
                    logger.info("--> add random documents to {}", index);
                    addRandomDocuments(index, randomIntBetween(10, 1000));
                } else {
                    int docCount = (int) client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value;
                    int deleteCount = randomIntBetween(1, docCount);
                    logger.info("--> delete {} random documents from {}", deleteCount, index);
                    for (int i = 0; i < deleteCount; i++) {
                        int doc = randomIntBetween(0, docCount - 1);
                        client().prepareDelete(index, index, Integer.toString(doc)).get();
                    }
                    client().admin().indices().prepareRefresh(index).get();
                }
            }

            // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
            ensureGreen();
            logger.info("-->  close indices {}", closeIndices);
            assertAcked(client().admin().indices().prepareClose(closeIndices.toArray(new String[closeIndices.size()])));
        }

        logger.info("--> restore all indices from the snapshot");
        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(true));

        // higher timeout since we can have quite a few shards and a little more data here
        ensureGreen(TimeValue.timeValueSeconds(120));

        for (int i = 0; i < indexCount; i++) {
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(docCounts[i]).get(), docCounts[i]);
        }

        logger.info("-->  delete snapshot {}:{}", repoName, snapshotName);
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());

        expectThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().prepareGetSnapshots(repoName).setSnapshots(snapshotName).get()
        );

        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());

        expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(randomBoolean()).get()
        );
    }

    @Override
    public void testReadNonExistingPath() {

    }

    @Override
    public void testMultipleSnapshotAndRollback() {

    }

    @Override
    public void testList() {

    }

    @Override
    public void testContainerCreationAndDeletion() {

    }

    @Override
    public void testIndicesDeletedFromRepository() {

    }

    @Override
    public void testDeleteBlobs() {

    }
}
