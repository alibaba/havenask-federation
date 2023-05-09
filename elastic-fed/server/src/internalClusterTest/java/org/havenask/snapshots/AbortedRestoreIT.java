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

package org.havenask.snapshots;

import org.havenask.action.ActionFuture;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.havenask.action.admin.indices.recovery.RecoveryResponse;
import org.havenask.action.support.IndicesOptions;
import org.havenask.cluster.routing.RecoverySource;
import org.havenask.cluster.service.ClusterService;
import org.havenask.indices.recovery.RecoveryState;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.threadpool.ThreadPool;
import org.havenask.threadpool.ThreadPoolStats;

import org.hamcrest.Matcher;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AbortedRestoreIT extends AbstractSnapshotIntegTestCase {

    public void testAbortedRestoreAlsoAbortFileRestores() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String indexName = "test-abort-restore";
        createIndex(indexName, indexSettingsNoReplicas(1).build());
        indexRandomDocs(indexName, scaledRandomIntBetween(10, 1_000));
        ensureGreen();
        forceMerge();

        final String repositoryName = "repository";
        createRepository(repositoryName, "mock");

        final String snapshotName = "snapshot";
        createFullSnapshot(repositoryName, snapshotName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> blocking all data nodes for repository [{}]", repositoryName);
        blockAllDataNodes(repositoryName);
        failReadsAllDataNodes(repositoryName);

        logger.info("--> starting restore");
        final ActionFuture<RestoreSnapshotResponse> future = client().admin().cluster().prepareRestoreSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .execute();

        assertBusy(() -> {
            final RecoveryResponse recoveries = client().admin().indices().prepareRecoveries(indexName)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN).setActiveOnly(true).get();
            assertThat(recoveries.hasRecoveries(), is(true));
            final List<RecoveryState> shardRecoveries = recoveries.shardRecoveryStates().get(indexName);
            assertThat(shardRecoveries, hasSize(1));
            assertThat(future.isDone(), is(false));

            for (RecoveryState shardRecovery : shardRecoveries) {
                assertThat(shardRecovery.getRecoverySource().getType(), equalTo(RecoverySource.Type.SNAPSHOT));
                assertThat(shardRecovery.getStage(), equalTo(RecoveryState.Stage.INDEX));
            }
        });

        final ThreadPool.Info snapshotThreadPoolInfo = threadPool(dataNode).info(ThreadPool.Names.SNAPSHOT);
        assertThat(snapshotThreadPoolInfo.getMax(), greaterThan(0));

        logger.info("--> waiting for snapshot thread [max={}] pool to be full", snapshotThreadPoolInfo.getMax());
        waitForMaxActiveSnapshotThreads(dataNode, equalTo(snapshotThreadPoolInfo.getMax()));

        logger.info("--> aborting restore by deleting the index");
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> unblocking repository [{}]", repositoryName);
        unblockAllDataNodes(repositoryName);

        logger.info("--> restore should have failed");
        final RestoreSnapshotResponse restoreSnapshotResponse = future.get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(1));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));

        logger.info("--> waiting for snapshot thread pool to be empty");
        waitForMaxActiveSnapshotThreads(dataNode, equalTo(0));
    }

    private static void waitForMaxActiveSnapshotThreads(final String node, final Matcher<Integer> matcher) throws Exception {
        assertBusy(() -> assertThat(threadPoolStats(node, ThreadPool.Names.SNAPSHOT).getActive(), matcher), 30L, TimeUnit.SECONDS);
    }

    private static ThreadPool threadPool(final String node) {
        return internalCluster().getInstance(ClusterService.class, node).getClusterApplierService().threadPool();
    }

    private static ThreadPoolStats.Stats threadPoolStats(final String node, final String threadPoolName) {
        return StreamSupport.stream(threadPool(node).stats().spliterator(), false)
            .filter(threadPool -> threadPool.getName().equals(threadPoolName))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Failed to find thread pool " + threadPoolName));
    }
}
