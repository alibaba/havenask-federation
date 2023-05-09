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

import org.apache.lucene.index.IndexCommit;

import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.Environment;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.snapshots.IndexShardSnapshotStatus;
import org.havenask.index.store.Store;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.RepositoryPlugin;
import org.havenask.repositories.IndexId;
import org.havenask.repositories.Repository;
import org.havenask.repositories.RepositoryData;
import org.havenask.repositories.ShardGenerations;
import org.havenask.repositories.fs.FsRepository;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class RepositoryFilterUserMetadataIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MetadataFilteringPlugin.class);
    }

    public void testFilteredRepoMetadataIsUsed() {
        final String masterName = internalCluster().getMasterName();
        final String repoName = "test-repo";
        assertAcked(client().admin().cluster().preparePutRepository(repoName).setType(MetadataFilteringPlugin.TYPE).setSettings(
            Settings.builder().put("location", randomRepoPath())
                .put(MetadataFilteringPlugin.MASTER_SETTING_VALUE, masterName)));
        createIndex("test-idx");
        final SnapshotInfo snapshotInfo = client().admin().cluster().prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(true).get().getSnapshotInfo();
        assertThat(snapshotInfo.userMetadata(), is(Collections.singletonMap(MetadataFilteringPlugin.MOCK_FILTERED_META, masterName)));
    }

    // Mock plugin that stores the name of the master node that started a snapshot in each snapshot's metadata
    public static final class MetadataFilteringPlugin extends org.havenask.plugins.Plugin implements RepositoryPlugin {

        private static final String MOCK_FILTERED_META = "mock_filtered_meta";

        private static final String MASTER_SETTING_VALUE = "initial_master";

        private static final String TYPE = "mock_meta_filtering";

        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                               ClusterService clusterService, RecoverySettings recoverySettings) {
            return Collections.singletonMap("mock_meta_filtering", metadata ->
                new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {

                    // Storing the initially expected metadata value here to verify that #filterUserMetadata is only called once on the
                    // initial master node starting the snapshot
                    private final String initialMetaValue = metadata.settings().get(MASTER_SETTING_VALUE);

                    @Override
                    public void finalizeSnapshot(ShardGenerations shardGenerations, long repositoryStateId,
                                                 Metadata clusterMetadata, SnapshotInfo snapshotInfo, Version repositoryMetaVersion,
                                                 Function<ClusterState, ClusterState> stateTransformer,
                                                 ActionListener<RepositoryData> listener) {
                        super.finalizeSnapshot(shardGenerations, repositoryStateId, clusterMetadata, snapshotInfo,
                            repositoryMetaVersion, stateTransformer, listener);
                    }

                    @Override
                    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                                              IndexCommit snapshotIndexCommit, String shardStateIdentifier,
                                              IndexShardSnapshotStatus snapshotStatus, Version repositoryMetaVersion,
                                              Map<String, Object> userMetadata, ActionListener<String> listener) {
                        assertThat(userMetadata, is(Collections.singletonMap(MOCK_FILTERED_META, initialMetaValue)));
                        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, shardStateIdentifier,
                            snapshotStatus, repositoryMetaVersion, userMetadata, listener);
                    }

                    @Override
                    public Map<String, Object> adaptUserMetadata(Map<String, Object> userMetadata) {
                        return Collections.singletonMap(MOCK_FILTERED_META, clusterService.getNodeName());
                    }
                });
        }
    }
}
