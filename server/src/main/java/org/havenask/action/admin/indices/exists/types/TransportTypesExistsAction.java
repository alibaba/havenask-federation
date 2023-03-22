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

package org.havenask.action.admin.indices.exists.types;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.TransportMasterNodeReadAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;

/**
 * Types exists transport action.
 */
public class TransportTypesExistsAction extends TransportMasterNodeReadAction<TypesExistsRequest, TypesExistsResponse> {

    @Inject
    public TransportTypesExistsAction(TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(TypesExistsAction.NAME, transportService, clusterService, threadPool, actionFilters, TypesExistsRequest::new,
            indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        // lightweight check
        return ThreadPool.Names.SAME;
    }

    @Override
    protected TypesExistsResponse read(StreamInput in) throws IOException {
        return new TypesExistsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(TypesExistsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(final TypesExistsRequest request, final ClusterState state,
                                   final ActionListener<TypesExistsResponse> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request.indicesOptions(), request.indices());
        if (concreteIndices.length == 0) {
            listener.onResponse(new TypesExistsResponse(false));
            return;
        }

        for (String concreteIndex : concreteIndices) {
            if (!state.metadata().hasConcreteIndex(concreteIndex)) {
                listener.onResponse(new TypesExistsResponse(false));
                return;
            }

            MappingMetadata mapping = state.metadata().getIndices().get(concreteIndex).mapping();
            if (mapping == null) {
                listener.onResponse(new TypesExistsResponse(false));
                return;
            }

            for (String type : request.types()) {
                if (mapping.type().equals(type) == false) {
                    listener.onResponse(new TypesExistsResponse(false));
                    return;
                }
            }
        }

        listener.onResponse(new TypesExistsResponse(true));
    }
}
