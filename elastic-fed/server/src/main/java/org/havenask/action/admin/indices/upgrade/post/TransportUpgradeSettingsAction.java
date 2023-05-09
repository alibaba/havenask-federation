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

package org.havenask.action.admin.indices.upgrade.post;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.action.support.master.TransportMasterNodeAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ack.ClusterStateUpdateResponse;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.MetadataUpdateSettingsService;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;

public class TransportUpgradeSettingsAction extends TransportMasterNodeAction<UpgradeSettingsRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportUpgradeSettingsAction.class);

    private final MetadataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpgradeSettingsAction(TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, MetadataUpdateSettingsService updateSettingsService,
                                          IndexNameExpressionResolver indexNameExpressionResolver, ActionFilters actionFilters) {
        super(UpgradeSettingsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            UpgradeSettingsRequest::new, indexNameExpressionResolver);
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected String executor() {
        // we go async right away....
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(UpgradeSettingsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(final UpgradeSettingsRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        UpgradeSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpgradeSettingsClusterStateUpdateRequest()
                .ackTimeout(request.timeout())
                .versions(request.versions())
                .masterNodeTimeout(request.masterNodeTimeout());

        updateSettingsService.upgradeIndexSettings(clusterStateUpdateRequest, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to upgrade minimum compatibility version settings on indices [{}]",
                    request.versions().keySet()), t);
                listener.onFailure(t);
            }
        });
    }
}
