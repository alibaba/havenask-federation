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

package org.havenask.action.admin.indices.close;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.LegacyESVersion;
import org.havenask.action.ActionListener;
import org.havenask.action.admin.indices.flush.FlushRequest;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.replication.ReplicationOperation;
import org.havenask.action.support.replication.ReplicationRequest;
import org.havenask.action.support.replication.ReplicationResponse;
import org.havenask.action.support.replication.TransportReplicationAction;
import org.havenask.cluster.action.shard.ShardStateAction;
import org.havenask.cluster.block.ClusterBlock;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.lease.Releasable;
import org.havenask.common.settings.Settings;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.IndicesService;
import org.havenask.tasks.TaskId;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

public class TransportVerifyShardBeforeCloseAction extends TransportReplicationAction<
    TransportVerifyShardBeforeCloseAction.ShardRequest, TransportVerifyShardBeforeCloseAction.ShardRequest, ReplicationResponse> {

    public static final String NAME = CloseIndexAction.NAME + "[s]";
    protected Logger logger = LogManager.getLogger(getClass());

    @Inject
    public TransportVerifyShardBeforeCloseAction(final Settings settings, final TransportService transportService,
                                                 final ClusterService clusterService, final IndicesService indicesService,
                                                 final ThreadPool threadPool, final ShardStateAction stateAction,
                                                 final ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, stateAction, actionFilters,
            ShardRequest::new, ShardRequest::new, ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void acquirePrimaryOperationPermit(final IndexShard primary,
                                                 final ShardRequest request,
                                                 final ActionListener<Releasable> onAcquired) {
        primary.acquireAllPrimaryOperationsPermits(onAcquired, request.timeout());
    }

    @Override
    protected void acquireReplicaOperationPermit(final IndexShard replica,
                                                 final ShardRequest request,
                                                 final ActionListener<Releasable> onAcquired,
                                                 final long primaryTerm,
                                                 final long globalCheckpoint,
                                                 final long maxSeqNoOfUpdateOrDeletes) {
        replica.acquireAllReplicaOperationsPermits(primaryTerm, globalCheckpoint, maxSeqNoOfUpdateOrDeletes, onAcquired, request.timeout());
    }

    @Override
    protected void shardOperationOnPrimary(final ShardRequest shardRequest, final IndexShard primary,
            ActionListener<PrimaryResult<ShardRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            executeShardOperation(shardRequest, primary);
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            executeShardOperation(shardRequest, replica);
            return new ReplicaResult();
        });
    }

    private void executeShardOperation(final ShardRequest request, final IndexShard indexShard) throws IOException {
        final ShardId shardId = indexShard.shardId();
        if (indexShard.getActiveOperationsCount() != IndexShard.OPERATIONS_BLOCKED) {
            throw new IllegalStateException("Index shard " + shardId + " is not blocking all operations during closing");
        }

        final ClusterBlocks clusterBlocks = clusterService.state().blocks();
        if (clusterBlocks.hasIndexBlock(shardId.getIndexName(), request.clusterBlock()) == false) {
            throw new IllegalStateException("Index shard " + shardId + " must be blocked by " + request.clusterBlock() + " before closing");
        }
        if (request.isPhase1()) {
            // in order to advance the global checkpoint to the maximum sequence number, the (persisted) local checkpoint needs to be
            // advanced first, which, when using async translog syncing, does not automatically hold at the time where we have acquired
            // all operation permits. Instead, this requires and explicit sync, which communicates the updated (persisted) local checkpoint
            // to the primary (we call this phase1), and phase2 can then use the fact that the global checkpoint has moved to the maximum
            // sequence number to pass the verifyShardBeforeIndexClosing check and create a safe commit where the maximum sequence number
            // is equal to the global checkpoint.
            indexShard.sync();
        } else {
            indexShard.verifyShardBeforeIndexClosing();
            indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            logger.trace("{} shard is ready for closing", shardId);
        }
    }

    @Override
    protected ReplicationOperation.Replicas<ShardRequest> newReplicasProxy() {
        return new VerifyShardBeforeCloseActionReplicasProxy();
    }

    /**
     * A {@link ReplicasProxy} that marks as stale the shards that are unavailable during the verification
     * and the flush of the shard. This is done to ensure that such shards won't be later promoted as primary
     * or reopened in an unverified state with potential non flushed translog operations.
     */
    class VerifyShardBeforeCloseActionReplicasProxy extends ReplicasProxy {
        @Override
        public void markShardCopyAsStaleIfNeeded(final ShardId shardId, final String allocationId, final long primaryTerm,
                                                 final ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }

    public static class ShardRequest extends ReplicationRequest<ShardRequest> {

        private final ClusterBlock clusterBlock;

        private final boolean phase1;

        ShardRequest(StreamInput in) throws IOException {
            super(in);
            clusterBlock = new ClusterBlock(in);
            if (in.getVersion().onOrAfter(LegacyESVersion.V_7_3_0)) {
                phase1 = in.readBoolean();
            } else {
                phase1 = false;
            }
        }

        public ShardRequest(final ShardId shardId, final ClusterBlock clusterBlock, final boolean phase1, final TaskId parentTaskId) {
            super(shardId);
            this.clusterBlock = Objects.requireNonNull(clusterBlock);
            this.phase1 = phase1;
            setParentTask(parentTaskId);
        }

        @Override
        public String toString() {
            return "verify shard " + shardId + " before close with block " + clusterBlock;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterBlock.writeTo(out);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_3_0)) {
                out.writeBoolean(phase1);
            }
        }

        public ClusterBlock clusterBlock() {
            return clusterBlock;
        }

        public boolean isPhase1() {
            return phase1;
        }
    }
}
