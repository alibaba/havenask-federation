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

package org.havenask.action.admin.indices.flush;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.replication.ReplicationResponse;
import org.havenask.action.support.replication.TransportReplicationAction;
import org.havenask.cluster.action.shard.ShardStateAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Settings;
import org.havenask.index.shard.IndexShard;
import org.havenask.indices.IndicesService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;

public class TransportShardFlushAction
        extends TransportReplicationAction<ShardFlushRequest, ShardFlushRequest, ReplicationResponse> {

    public static final String NAME = FlushAction.NAME + "[s]";

    @Inject
    public TransportShardFlushAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                     ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
            actionFilters, ShardFlushRequest::new, ShardFlushRequest::new, ThreadPool.Names.FLUSH);
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(ShardFlushRequest shardRequest, IndexShard primary,
            ActionListener<PrimaryResult<ShardFlushRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            primary.flush(shardRequest.getRequest());
            logger.trace("{} flush request executed on primary", primary.shardId());
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardFlushRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            replica.flush(request.getRequest());
            logger.trace("{} flush request executed on replica", replica.shardId());
            return new ReplicaResult();
        });
    }
}
