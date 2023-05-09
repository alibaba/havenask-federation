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

package org.havenask.index.replication;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.replication.ReplicationResponse;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Randomness;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexSettings;
import org.havenask.index.seqno.RetentionLease;
import org.havenask.index.seqno.RetentionLeaseSyncAction;
import org.havenask.index.seqno.RetentionLeaseUtils;
import org.havenask.index.seqno.RetentionLeases;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.test.VersionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RetentionLeasesReplicationTests extends HavenaskIndexLevelReplicationTestCase {

    public void testSimpleSyncRetentionLeases() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        try (ReplicationGroup group = createGroup(between(0, 2), settings)) {
            group.startAll();
            List<RetentionLease> leases = new ArrayList<>();
            int iterations = between(1, 100);
            CountDownLatch latch = new CountDownLatch(iterations);
            for (int i = 0; i < iterations; i++) {
                if (leases.isEmpty() == false && rarely()) {
                    RetentionLease leaseToRemove = randomFrom(leases);
                    leases.remove(leaseToRemove);
                    group.removeRetentionLease(leaseToRemove.id(), ActionListener.wrap(latch::countDown));
                } else {
                    RetentionLease newLease = group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i,
                        ActionListener.wrap(latch::countDown));
                    leases.add(newLease);
                }
            }
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.version(), equalTo(iterations + group.getReplicas().size() + 1L));
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(leasesOnPrimary).values(),
                containsInAnyOrder(leases.toArray(new RetentionLease[0])));
            latch.await();
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testOutOfOrderRetentionLeasesRequests() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        int numberOfReplicas = between(1, 2);
        IndexMetadata indexMetadata = buildIndexMetadata(numberOfReplicas, settings, indexMapping);
        try (ReplicationGroup group = new ReplicationGroup(indexMetadata) {
            @Override
            protected void syncRetentionLeases(ShardId shardId, RetentionLeases leases, ActionListener<ReplicationResponse> listener) {
                listener.onResponse(new SyncRetentionLeasesResponse(new RetentionLeaseSyncAction.Request(shardId, leases)));
            }
        }) {
            group.startAll();
            int numLeases = between(1, 10);
            List<RetentionLeaseSyncAction.Request> requests = new ArrayList<>();
            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<ReplicationResponse> future = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, future);
                requests.add(((SyncRetentionLeasesResponse) future.actionGet()).syncRequest);
            }
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            for (IndexShard replica : group.getReplicas()) {
                Randomness.shuffle(requests);
                requests.forEach(request -> group.executeRetentionLeasesSyncRequestOnReplica(request, replica));
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testSyncRetentionLeasesWithPrimaryPromotion() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        int numberOfReplicas = between(2, 4);
        IndexMetadata indexMetadata = buildIndexMetadata(numberOfReplicas, settings, indexMapping);
        try (ReplicationGroup group = new ReplicationGroup(indexMetadata) {
            @Override
            protected void syncRetentionLeases(ShardId shardId, RetentionLeases leases, ActionListener<ReplicationResponse> listener) {
                listener.onResponse(new SyncRetentionLeasesResponse(new RetentionLeaseSyncAction.Request(shardId, leases)));
            }
        }) {
            group.startAll();
            for (IndexShard replica : group.getReplicas()) {
                replica.updateRetentionLeasesOnReplica(group.getPrimary().getRetentionLeases());
            }
            int numLeases = between(1, 100);
            IndexShard newPrimary = randomFrom(group.getReplicas());
            RetentionLeases latestRetentionLeasesOnNewPrimary = newPrimary.getRetentionLeases();
            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<ReplicationResponse> addLeaseFuture = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, addLeaseFuture);
                RetentionLeaseSyncAction.Request request = ((SyncRetentionLeasesResponse) addLeaseFuture.actionGet()).syncRequest;
                for (IndexShard replica : randomSubsetOf(group.getReplicas())) {
                    group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
                    if (newPrimary == replica) {
                        latestRetentionLeasesOnNewPrimary = request.getRetentionLeases();
                    }
                }
            }
            group.promoteReplicaToPrimary(newPrimary).get();
            // we need to make changes to retention leases to sync it to replicas
            // since we don't sync retention leases when promoting a new primary.
            PlainActionFuture<ReplicationResponse> newLeaseFuture = new PlainActionFuture<>();
            group.addRetentionLease("new-lease-after-promotion", randomNonNegativeLong(), "test", newLeaseFuture);
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(leasesOnPrimary.version(), equalTo(latestRetentionLeasesOnNewPrimary.version() + 1));
            assertThat(leasesOnPrimary.leases(), hasSize(latestRetentionLeasesOnNewPrimary.leases().size() + 1));
            RetentionLeaseSyncAction.Request request = ((SyncRetentionLeasesResponse) newLeaseFuture.actionGet()).syncRequest;
            for (IndexShard replica : group.getReplicas()) {
                group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
            }
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testTurnOffTranslogRetentionAfterAllShardStarted() throws Exception {
        final Settings.Builder settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
        if (randomBoolean()) {
            settings.put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_6_5_0, Version.CURRENT));
        }
        try (ReplicationGroup group = createGroup(between(1, 2), settings.build())) {
            group.startAll();
            group.indexDocs(randomIntBetween(1, 10));
            for (IndexShard shard : group) {
                shard.updateShardState(shard.routingEntry(), shard.getOperationPrimaryTerm(), null, 1L,
                    group.getPrimary().getReplicationGroup().getInSyncAllocationIds(),
                    group.getPrimary().getReplicationGroup().getRoutingTable());
            }
            group.syncGlobalCheckpoint();
            group.flush();
            assertBusy(() -> {
                // we turn off the translog retention policy using the generic threadPool
                for (IndexShard shard : group) {
                    assertThat(shard.translogStats().estimatedNumberOfOperations(), equalTo(0));
                }
            });
        }
    }

    static final class SyncRetentionLeasesResponse extends ReplicationResponse {
        final RetentionLeaseSyncAction.Request syncRequest;
        SyncRetentionLeasesResponse(RetentionLeaseSyncAction.Request syncRequest) {
            this.syncRequest = syncRequest;
        }
    }
}
