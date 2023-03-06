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

package org.havenask.action.support.master.info;

import org.havenask.action.ActionListener;
import org.havenask.action.ActionResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.TransportMasterNodeReadAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.stream.Writeable;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

public abstract class TransportClusterInfoAction<Request extends ClusterInfoRequest<Request>, Response extends ActionResponse>
        extends TransportMasterNodeReadAction<Request, Response> {

    public TransportClusterInfoAction(String actionName, TransportService transportService,
                                      ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                      Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        // read operation, lightweight...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected final void masterOperation(final Request request, final ClusterState state, final ActionListener<Response> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        doMasterOperation(request, concreteIndices, state, listener);
    }

    protected abstract void doMasterOperation(Request request, String[] concreteIndices, ClusterState state,
                                              ActionListener<Response> listener);
}
