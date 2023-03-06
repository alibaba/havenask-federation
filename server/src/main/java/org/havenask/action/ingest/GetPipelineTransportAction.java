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

package org.havenask.action.ingest;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.TransportMasterNodeReadAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.ingest.IngestService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;

public class GetPipelineTransportAction extends TransportMasterNodeReadAction<GetPipelineRequest, GetPipelineResponse> {

    @Inject
    public GetPipelineTransportAction(ThreadPool threadPool, ClusterService clusterService,
                                      TransportService transportService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetPipelineAction.NAME, transportService, clusterService, threadPool, actionFilters, GetPipelineRequest::new,
                indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetPipelineResponse read(StreamInput in) throws IOException {
        return new GetPipelineResponse(in);
    }

    @Override
    protected void masterOperation(GetPipelineRequest request, ClusterState state, ActionListener<GetPipelineResponse> listener)
            throws Exception {
        listener.onResponse(new GetPipelineResponse(IngestService.getPipelines(state, request.getIds())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
