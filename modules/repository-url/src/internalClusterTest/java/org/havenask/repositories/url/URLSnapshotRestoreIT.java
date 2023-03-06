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

package org.havenask.repositories.url;

import org.havenask.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.havenask.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.client.Client;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.plugin.repository.url.URLRepositoryPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.fs.FsRepository;
import org.havenask.snapshots.SnapshotState;
import org.havenask.test.HavenaskIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST)
public class URLSnapshotRestoreIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(URLRepositoryPlugin.class);
    }

    public void testUrlRepository() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
            .setType(FsRepository.TYPE).setSettings(Settings.builder()
                .put(FsRepository.LOCATION_SETTING.getKey(), repositoryLocation)
                .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client
            .admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        int actualTotalShards = createSnapshotResponse.getSnapshotInfo().totalShards();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(actualTotalShards));

        SnapshotState state = client
            .admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap")
            .get()
            .getSnapshots()
            .get(0)
            .state();
        assertThat(state, equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> create read-only URL repository");
        assertAcked(client.admin().cluster().preparePutRepository("url-repo")
            .setType(URLRepository.TYPE).setSettings(Settings.builder()
                .put(URLRepository.URL_SETTING.getKey(), repositoryLocation.toUri().toURL().toString())
                .put("list_directories", randomBoolean())));
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client
            .admin()
            .cluster()
            .prepareRestoreSnapshot("url-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> list available shapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));

        logger.info("--> delete snapshot");
        AcknowledgedResponse deleteSnapshotResponse = client.admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").get();
        assertAcked(deleteSnapshotResponse);

        logger.info("--> list available shapshot again, no snapshots should be returned");
        getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(0));
    }
}
