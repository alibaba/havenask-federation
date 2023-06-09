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

package org.havenask.action.support.replication;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.havenask.ExceptionsHelper;
import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.DefaultShardOperationFailedException;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.action.support.TransportActions;
import org.havenask.action.support.broadcast.BroadcastRequest;
import org.havenask.action.support.broadcast.BroadcastResponse;
import org.havenask.action.support.broadcast.BroadcastShardOperationFailedException;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.util.concurrent.CountDown;
import org.havenask.index.shard.ShardId;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Base class for requests that should be executed on all shards of an index or several indices.
 * This action sends shard requests to all primary shards of the indices and they are then replicated like write requests
 */
public abstract class TransportBroadcastReplicationAction<Request extends BroadcastRequest<Request>, Response extends BroadcastResponse,
        ShardRequest extends ReplicationRequest<ShardRequest>, ShardResponse extends ReplicationResponse>
        extends HandledTransportAction<Request, Response> {

    private final TransportReplicationAction replicatedBroadcastShardAction;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public TransportBroadcastReplicationAction(String name, Writeable.Reader<Request> requestReader, ClusterService clusterService,
                                               TransportService transportService,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                               TransportReplicationAction replicatedBroadcastShardAction) {
        super(name, transportService, actionFilters, requestReader);
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        List<ShardId> shards = shards(request, clusterState);
        final CopyOnWriteArrayList<ShardResponse> shardsResponses = new CopyOnWriteArrayList<>();
        if (shards.size() == 0) {
            finishAndNotifyListener(listener, shardsResponses);
        }
        final CountDown responsesCountDown = new CountDown(shards.size());
        for (final ShardId shardId : shards) {
            ActionListener<ShardResponse> shardActionListener = new ActionListener<ShardResponse>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    shardsResponses.add(shardResponse);
                    logger.trace("{}: got response from {}", actionName, shardId);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.trace("{}: got failure from {}", actionName, shardId);
                    int totalNumCopies = clusterState.getMetadata().getIndexSafe(shardId.getIndex()).getNumberOfReplicas() + 1;
                    ShardResponse shardResponse = newShardResponse();
                    ReplicationResponse.ShardInfo.Failure[] failures;
                    if (TransportActions.isShardNotAvailableException(e)) {
                        failures = new ReplicationResponse.ShardInfo.Failure[0];
                    } else {
                        ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(shardId, null, e,
                            ExceptionsHelper.status(e), true);
                        failures = new ReplicationResponse.ShardInfo.Failure[totalNumCopies];
                        Arrays.fill(failures, failure);
                    }
                    shardResponse.setShardInfo(new ReplicationResponse.ShardInfo(totalNumCopies, 0, failures));
                    shardsResponses.add(shardResponse);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }
            };
            shardExecute(task, request, shardId, shardActionListener);
        }
    }

    protected void shardExecute(Task task, Request request, ShardId shardId, ActionListener<ShardResponse> shardActionListener) {
        ShardRequest shardRequest = newShardRequest(request, shardId);
        shardRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        replicatedBroadcastShardAction.execute(shardRequest, shardActionListener);
    }

    /**
     * @return all shard ids the request should run on
     */
    protected List<ShardId> shards(Request request, ClusterState clusterState) {
        List<ShardId> shardIds = new ArrayList<>();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().getIndices().get(index);
            if (indexMetadata != null) {
                for (IntObjectCursor<IndexShardRoutingTable> shardRouting
                        : clusterState.getRoutingTable().indicesRouting().get(index).getShards()) {
                    shardIds.add(shardRouting.value.shardId());
                }
            }
        }
        return shardIds;
    }

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardRequest newShardRequest(Request request, ShardId shardId);

    private void finishAndNotifyListener(ActionListener listener, CopyOnWriteArrayList<ShardResponse> shardsResponses) {
        logger.trace("{}: got all shard responses", actionName);
        int successfulShards = 0;
        int failedShards = 0;
        int totalNumCopies = 0;
        List<DefaultShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.size(); i++) {
            ReplicationResponse shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // non active shard, ignore
            } else {
                failedShards += shardResponse.getShardInfo().getFailed();
                successfulShards += shardResponse.getShardInfo().getSuccessful();
                totalNumCopies += shardResponse.getShardInfo().getTotal();
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                for (ReplicationResponse.ShardInfo.Failure failure : shardResponse.getShardInfo().getFailures()) {
                    shardFailures.add(new DefaultShardOperationFailedException(
                        new BroadcastShardOperationFailedException(failure.fullShardId(), failure.getCause())));
                }
            }
        }
        listener.onResponse(newResponse(successfulShards, failedShards, totalNumCopies, shardFailures));
    }

    protected abstract BroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
                                                     List<DefaultShardOperationFailedException> shardFailures);
}
