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
import org.havenask.action.support.single.shard.TransportSingleShardAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.routing.GroupShardsIterator;
import org.havenask.cluster.routing.ShardIterator;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.Writeable;
import org.havenask.index.IndexService;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.index.termvectors.TermVectorsService;
import org.havenask.indices.IndicesService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;

/**
 * Performs the get operation.
 */
public class TransportTermVectorsAction extends TransportSingleShardAction<TermVectorsRequest, TermVectorsResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportTermVectorsAction(ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(TermVectorsAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                TermVectorsRequest::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;

    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        if (request.request().doc() != null && request.request().routing() == null) {
            // artificial document without routing specified, ignore its "id" and use either random shard or according to preference
            GroupShardsIterator<ShardIterator> groupShardsIter = clusterService.operationRouting().searchShards(state,
                    new String[] { request.concreteIndex() }, null, request.request().preference());
            return groupShardsIter.iterator().next();
        }

        return clusterService.operationRouting().getShards(state, request.concreteIndex(), request.request().id(),
                request.request().routing(), request.request().preference());
    }

    @Override
    protected boolean resolveIndex(TermVectorsRequest request) {
        return true;
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        // update the routing (request#index here is possibly an alias or a parent)
        request.request().routing(state.metadata().resolveIndexRouting(request.request().routing(), request.request().index()));
        // Fail fast on the node that received the request.
        if (request.request().routing() == null && state.getMetadata().routingRequired(request.concreteIndex())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().type(), request.request().id());
        }
    }

    @Override
    protected void asyncShardOperation(TermVectorsRequest request, ShardId shardId,
                                       ActionListener<TermVectorsResponse> listener) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        if (request.realtime()) { // it's a realtime request which is not subject to refresh cycles
            super.asyncShardOperation(request, shardId, listener);
        } else {
            indexShard.awaitShardSearchActive(b -> {
                try {
                    super.asyncShardOperation(request, shardId, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        }
    }

    @Override
    protected TermVectorsResponse shardOperation(TermVectorsRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        return TermVectorsService.getTermVectors(indexShard, request);
    }

    @Override
    protected Writeable.Reader<TermVectorsResponse> getResponseReader() {
        return TermVectorsResponse::new;
    }

    @Override
    protected String getExecutor(TermVectorsRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().isSearchThrottled() ? ThreadPool.Names.SEARCH_THROTTLED : super.getExecutor(request,
            shardId);
    }
}
