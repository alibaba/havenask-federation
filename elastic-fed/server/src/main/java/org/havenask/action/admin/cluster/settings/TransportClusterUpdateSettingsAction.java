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

package org.havenask.action.admin.cluster.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.TransportMasterNodeAction;
import org.havenask.cluster.AckedClusterStateUpdateTask;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.Priority;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;

public class TransportClusterUpdateSettingsAction extends
    TransportMasterNodeAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterUpdateSettingsAction.class);

    private final AllocationService allocationService;

    private final ClusterSettings clusterSettings;

    @Inject
    public TransportClusterUpdateSettingsAction(TransportService transportService, ClusterService clusterService,
                                                ThreadPool threadPool, AllocationService allocationService, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver, ClusterSettings clusterSettings) {
        super(ClusterUpdateSettingsAction.NAME, false, transportService, clusterService, threadPool, actionFilters,
            ClusterUpdateSettingsRequest::new, indexNameExpressionResolver);
        this.allocationService = allocationService;
        this.clusterSettings = clusterSettings;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterUpdateSettingsRequest request, ClusterState state) {
        // allow for dedicated changes to the metadata blocks, so we don't block those to allow to "re-enable" it
        if (request.transientSettings().size() + request.persistentSettings().size() == 1) {
            // only one setting
            if (Metadata.SETTING_READ_ONLY_SETTING.exists(request.persistentSettings())
                || Metadata.SETTING_READ_ONLY_SETTING.exists(request.transientSettings())
                || Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.exists(request.transientSettings())
                || Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.exists(request.persistentSettings())) {
                // one of the settings above as the only setting in the request means - resetting the block!
                return null;
            }
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterUpdateSettingsResponse read(StreamInput in) throws IOException {
        return new ClusterUpdateSettingsResponse(in);
    }

    @Override
    protected void masterOperation(final ClusterUpdateSettingsRequest request, final ClusterState state,
                                   final ActionListener<ClusterUpdateSettingsResponse> listener) {
        final SettingsUpdater updater = new SettingsUpdater(clusterSettings);
        clusterService.submitStateUpdateTask("cluster_update_settings",
                new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(Priority.IMMEDIATE, request, listener) {

            private volatile boolean changed = false;

            @Override
            protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                return new ClusterUpdateSettingsResponse(acknowledged, updater.getTransientUpdates(), updater.getPersistentUpdate());
            }

            @Override
            public void onAllNodesAcked(@Nullable Exception e) {
                if (changed) {
                    reroute(true);
                } else {
                    super.onAllNodesAcked(e);
                }
            }

            @Override
            public void onAckTimeout() {
                if (changed) {
                    reroute(false);
                } else {
                    super.onAckTimeout();
                }
            }

            private void reroute(final boolean updateSettingsAcked) {
                // We're about to send a second update task, so we need to check if we're still the elected master
                // For example the minimum_master_node could have been breached and we're no longer elected master,
                // so we should *not* execute the reroute.
                if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
                    logger.debug("Skipping reroute after cluster update settings, because node is no longer master");
                    listener.onResponse(new ClusterUpdateSettingsResponse(updateSettingsAcked, updater.getTransientUpdates(),
                        updater.getPersistentUpdate()));
                    return;
                }

                // The reason the reroute needs to be send as separate update task, is that all the *cluster* settings are encapsulate
                // in the components (e.g. FilterAllocationDecider), so the changes made by the first call aren't visible
                // to the components until the ClusterStateListener instances have been invoked, but are visible after
                // the first update task has been completed.
                clusterService.submitStateUpdateTask("reroute_after_cluster_update_settings",
                        new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(Priority.URGENT, request, listener) {

                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        //we wait for the reroute ack only if the update settings was acknowledged
                        return updateSettingsAcked;
                    }

                    @Override
                    // we return when the cluster reroute is acked or it times out but the acknowledged flag depends on whether the
                    // update settings was acknowledged
                    protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                        return new ClusterUpdateSettingsResponse(updateSettingsAcked && acknowledged, updater.getTransientUpdates(),
                            updater.getPersistentUpdate());
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.debug("failed to preform reroute after cluster settings were updated - current node is no longer a master");
                        listener.onResponse(new ClusterUpdateSettingsResponse(updateSettingsAcked, updater.getTransientUpdates(),
                            updater.getPersistentUpdate()));
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        //if the reroute fails we only log
                        logger.debug(() -> new ParameterizedMessage("failed to perform [{}]", source), e);
                        listener.onFailure(new HavenaskException("reroute after update settings failed", e));
                    }

                    @Override
                    public ClusterState execute(final ClusterState currentState) {
                        // now, reroute in case things that require it changed (e.g. number of replicas)
                        return allocationService.reroute(currentState, "reroute after cluster update settings");
                    }
                });
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.debug(() -> new ParameterizedMessage("failed to perform [{}]", source), e);
                super.onFailure(source, e);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                final ClusterState clusterState =
                        updater.updateSettings(
                                currentState,
                                clusterSettings.upgradeSettings(request.transientSettings()),
                                clusterSettings.upgradeSettings(request.persistentSettings()),
                                logger);
                changed = clusterState != currentState;
                return clusterState;
            }
        });
    }

}
