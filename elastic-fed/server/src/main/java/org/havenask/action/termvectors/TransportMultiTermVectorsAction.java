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

package org.havenask.action.termvectors;

import org.havenask.action.ActionListener;
import org.havenask.action.RoutingMissingException;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.util.concurrent.AtomicArray;
import org.havenask.index.IndexNotFoundException;
import org.havenask.index.shard.ShardId;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportMultiTermVectorsAction extends HandledTransportAction<MultiTermVectorsRequest, MultiTermVectorsResponse> {

    private final ClusterService clusterService;
    private final TransportShardMultiTermsVectorAction shardAction;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportMultiTermVectorsAction(TransportService transportService, ClusterService clusterService,
                                           TransportShardMultiTermsVectorAction shardAction, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(MultiTermVectorsAction.NAME, transportService, actionFilters, MultiTermVectorsRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        ClusterState clusterState = clusterService.state();

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiTermVectorsItemResponse> responses = new AtomicArray<>(request.requests.size());

        Map<ShardId, MultiTermVectorsShardRequest> shardRequests = new HashMap<>();
        for (int i = 0; i < request.requests.size(); i++) {
            TermVectorsRequest termVectorsRequest = request.requests.get(i);
            termVectorsRequest.routing(clusterState.metadata().resolveIndexRouting(termVectorsRequest.routing(),
                termVectorsRequest.index()));
            if (!clusterState.metadata().hasConcreteIndex(termVectorsRequest.index())) {
                responses.set(i, new MultiTermVectorsItemResponse(null,
                    new MultiTermVectorsResponse.Failure(termVectorsRequest.index(), termVectorsRequest.type(), termVectorsRequest.id(),
                        new IndexNotFoundException(termVectorsRequest.index()))));
                continue;
            }
            String concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, termVectorsRequest).getName();
            if (termVectorsRequest.routing() == null &&
                clusterState.getMetadata().routingRequired(concreteSingleIndex)) {
                responses.set(i, new MultiTermVectorsItemResponse(null,
                    new MultiTermVectorsResponse.Failure(concreteSingleIndex, termVectorsRequest.type(), termVectorsRequest.id(),
                        new RoutingMissingException(concreteSingleIndex, termVectorsRequest.type(), termVectorsRequest.id()))));
                continue;
            }
            ShardId shardId = clusterService.operationRouting().shardId(clusterState, concreteSingleIndex,
                    termVectorsRequest.id(), termVectorsRequest.routing());
            MultiTermVectorsShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiTermVectorsShardRequest(shardId.getIndexName(), shardId.id());
                shardRequest.preference(request.preference);
                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, termVectorsRequest);
        }

        if (shardRequests.size() == 0) {
            // only failures..
            listener.onResponse(new MultiTermVectorsResponse(responses.toArray(new MultiTermVectorsItemResponse[responses.length()])));
        }

        executeShardAction(listener, responses, shardRequests);
    }

    protected void executeShardAction(ActionListener<MultiTermVectorsResponse> listener,
                                      AtomicArray<MultiTermVectorsItemResponse> responses,
                                      Map<ShardId, MultiTermVectorsShardRequest> shardRequests) {
        final AtomicInteger counter = new AtomicInteger(shardRequests.size());

        for (final MultiTermVectorsShardRequest shardRequest : shardRequests.values()) {
            shardAction.execute(shardRequest, new ActionListener<MultiTermVectorsShardResponse>() {
                @Override
                public void onResponse(MultiTermVectorsShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        responses.set(response.locations.get(i), new MultiTermVectorsItemResponse(response.responses.get(i),
                                response.failures.get(i)));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // create failures for all relevant requests
                    for (int i = 0; i < shardRequest.locations.size(); i++) {
                        TermVectorsRequest termVectorsRequest = shardRequest.requests.get(i);
                        responses.set(shardRequest.locations.get(i), new MultiTermVectorsItemResponse(null,
                                new MultiTermVectorsResponse.Failure(shardRequest.index(), termVectorsRequest.type(),
                                        termVectorsRequest.id(), e)));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new MultiTermVectorsResponse(
                            responses.toArray(new MultiTermVectorsItemResponse[responses.length()])));
                }
            });
        }
    }
}
