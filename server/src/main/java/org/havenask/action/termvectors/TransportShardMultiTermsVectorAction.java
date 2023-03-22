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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.TransportActions;
import org.havenask.action.support.single.shard.TransportSingleShardAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
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

public class TransportShardMultiTermsVectorAction extends
        TransportSingleShardAction<MultiTermVectorsShardRequest, MultiTermVectorsShardResponse> {

    private final IndicesService indicesService;

    private static final String ACTION_NAME = MultiTermVectorsAction.NAME + "[shard]";

    @Inject
    public TransportShardMultiTermsVectorAction(ClusterService clusterService, TransportService transportService,
                                                IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                MultiTermVectorsShardRequest::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected Writeable.Reader<MultiTermVectorsShardResponse> getResponseReader() {
        return MultiTermVectorsShardResponse::new;
    }

    @Override
    protected boolean resolveIndex(MultiTermVectorsShardRequest request) {
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
                .getShards(state, request.concreteIndex(), request.request().shardId(), request.request().preference());
    }

    @Override
    protected MultiTermVectorsShardResponse shardOperation(MultiTermVectorsShardRequest request, ShardId shardId) {
        final MultiTermVectorsShardResponse response = new MultiTermVectorsShardResponse();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.id());
        for (int i = 0; i < request.locations.size(); i++) {
            TermVectorsRequest termVectorsRequest = request.requests.get(i);
            try {
                TermVectorsResponse termVectorsResponse = TermVectorsService.getTermVectors(indexShard, termVectorsRequest);
                response.add(request.locations.get(i), termVectorsResponse);
            } catch (RuntimeException e) {
                if (TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                } else {
                    logger.debug(() -> new ParameterizedMessage("{} failed to execute multi term vectors for [{}]/[{}]",
                        shardId, termVectorsRequest.type(), termVectorsRequest.id()), e);
                    response.add(request.locations.get(i),
                            new MultiTermVectorsResponse.Failure(request.index(), termVectorsRequest.type(), termVectorsRequest.id(), e));
                }
            }
        }

        return response;
    }

    @Override
    protected String getExecutor(MultiTermVectorsShardRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().isSearchThrottled() ? ThreadPool.Names.SEARCH_THROTTLED : super.getExecutor(request,
            shardId);
    }
}
