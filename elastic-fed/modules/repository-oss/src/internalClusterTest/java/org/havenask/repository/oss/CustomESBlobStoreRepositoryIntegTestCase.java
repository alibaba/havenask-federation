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

package org.havenask.repository.oss;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.common.settings.Settings;
import org.havenask.repositories.blobstore.HavenaskBlobStoreRepositoryIntegTestCase;
import org.havenask.repositories.fs.FsRepository;
import org.havenask.snapshots.SnapshotMissingException;
import org.havenask.snapshots.SnapshotRestoreException;
import org.havenask.test.hamcrest.HavenaskAssertions;

public class CustomESBlobStoreRepositoryIntegTestCase extends HavenaskBlobStoreRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return FsRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder().put(super.repositorySettings()).put("location", randomRepoPath()).build();
    }

    public void testRestoreAfterDeleteSnapshot() throws Exception {
        String repoName = this.createRepository(randomName());
        int indexCount = randomIntBetween(1, 5);
        int[] docCounts = new int[indexCount];
        String[] indexNames = this.generateRandomNames(indexCount);

        for (int i = 0; i < indexCount; ++i) {
            docCounts[i] = iterations(10, 1000);
            this.logger.info("-->  create random index {} with {} records", indexNames[i], docCounts[i]);
            this.addRandomDocuments(indexNames[i], docCounts[i]);
            HavenaskAssertions.assertHitCount(
                (SearchResponse) client().prepareSearch(new String[] { indexNames[i] }).setSize(0).get(),
                (long) docCounts[i]
            );
        }
        // create two snapshot
        String snapshotName = randomName();
        this.logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        assertSuccessfulSnapshot(
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).setIndices(indexNames)
        );

        String newSnapshotName = randomName();
        this.logger.info("-->  create snapshot {}:{}", repoName, newSnapshotName);
        assertSuccessfulSnapshot(
            client().admin().cluster().prepareCreateSnapshot(repoName, newSnapshotName).setWaitForCompletion(true).setIndices(indexNames)
        );

        List<String> deleteIndices = randomSubsetOf(randomIntBetween(0, indexCount), indexNames);
        if (deleteIndices.size() > 0) {
            this.logger.info("-->  delete indices {}", deleteIndices);
            HavenaskAssertions.assertAcked(
                client().admin().indices().prepareDelete((String[]) deleteIndices.toArray(new String[deleteIndices.size()]))
            );
        }

        Set<String> closeIndices = new HashSet(Arrays.asList(indexNames));
        closeIndices.removeAll(deleteIndices);
        if (closeIndices.size() > 0) {
            Iterator iterator = closeIndices.iterator();

            while (true) {
                while (iterator.hasNext()) {
                    String index = (String) iterator.next();
                    if (randomBoolean()) {
                        this.logger.info("--> add random documents to {}", index);
                        this.addRandomDocuments(index, randomIntBetween(10, 1000));
                    } else {
                        int docCount = (int) ((SearchResponse) client().prepareSearch(new String[] { index }).setSize(0).get()).getHits()
                            .getTotalHits().value;
                        int deleteCount = randomIntBetween(1, docCount);
                        this.logger.info("--> delete {} random documents from {}", deleteCount, index);

                        for (int i = 0; i < deleteCount; ++i) {
                            int doc = randomIntBetween(0, docCount - 1);
                            client().prepareDelete(index, index, Integer.toString(doc)).get();
                        }

                        client().admin().indices().prepareRefresh(new String[] { index }).get();
                    }
                }

                this.ensureGreen(new String[0]);
                this.logger.info("-->  close indices {}", closeIndices);
                HavenaskAssertions.assertAcked(
                    client().admin().indices().prepareClose((String[]) closeIndices.toArray(new String[closeIndices.size()]))
                );
                break;
            }
        }

        this.logger.info("-->  delete snapshot {}:{}", repoName, snapshotName);
        HavenaskAssertions.assertAcked(
            (AcknowledgedResponse) client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get()
        );
        expectThrows(
            SnapshotMissingException.class,
            () -> { client().admin().cluster().prepareGetSnapshots(repoName).setSnapshots(new String[] { snapshotName }).get(); }
        );
        expectThrows(
            SnapshotMissingException.class,
            () -> { client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get(); }
        );
        expectThrows(
            SnapshotRestoreException.class,
            () -> { client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(randomBoolean()).get(); }
        );

        this.logger.info("--> restore all indices from the snapshot");
        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repoName, newSnapshotName).setWaitForCompletion(true));
        this.ensureGreen(new String[0]);

        for (int i = 0; i < indexCount; ++i) {
            HavenaskAssertions.assertHitCount(
                (SearchResponse) client().prepareSearch(new String[] { indexNames[i] }).setSize(0).get(),
                (long) docCounts[i]
            );
        }

    }

    private String[] generateRandomNames(int num) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < num; i++) {
            String name;
            do {
                name = randomName();
            } while (names.contains(name));
            names.add(name);
        }
        return names.toArray(new String[num]);
    }

    public static void assertSuccessfulRestore(RestoreSnapshotRequestBuilder requestBuilder) {
        RestoreSnapshotResponse response = requestBuilder.get();
        assertSuccessfulRestore(response);
    }

    private static void assertSuccessfulRestore(RestoreSnapshotResponse response) {
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(response.getRestoreInfo().totalShards()));
    }
}
