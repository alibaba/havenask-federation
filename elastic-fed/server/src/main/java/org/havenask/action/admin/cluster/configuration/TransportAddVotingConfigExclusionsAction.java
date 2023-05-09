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

package org.havenask.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.HavenaskException;
import org.havenask.HavenaskTimeoutException;
import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.TransportMasterNodeAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateObserver;
import org.havenask.cluster.ClusterStateObserver.Listener;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.coordination.CoordinationMetadata;
import org.havenask.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Priority;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.threadpool.ThreadPool;
import org.havenask.threadpool.ThreadPool.Names;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportAddVotingConfigExclusionsAction extends TransportMasterNodeAction<AddVotingConfigExclusionsRequest,
    AddVotingConfigExclusionsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAddVotingConfigExclusionsAction.class);

    public static final Setting<Integer> MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING
        = Setting.intSetting("cluster.max_voting_config_exclusions", 10, 1, Property.Dynamic, Property.NodeScope);

    private volatile int maxVotingConfigExclusions;

    @Inject
    public TransportAddVotingConfigExclusionsAction(Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                                                    ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AddVotingConfigExclusionsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            AddVotingConfigExclusionsRequest::new, indexNameExpressionResolver);

        maxVotingConfigExclusions = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING, this::setMaxVotingConfigExclusions);
    }

    private void setMaxVotingConfigExclusions(int maxVotingConfigExclusions) {
        this.maxVotingConfigExclusions = maxVotingConfigExclusions;
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
        return new AddVotingConfigExclusionsResponse(in);
    }

    @Override
    protected void masterOperation(AddVotingConfigExclusionsRequest request, ClusterState state,
                                   ActionListener<AddVotingConfigExclusionsResponse> listener) throws Exception {

        resolveVotingConfigExclusionsAndCheckMaximum(request, state, maxVotingConfigExclusions);
        // throws IAE if no nodes matched or maximum exceeded

        clusterService.submitStateUpdateTask("add-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {

            private Set<VotingConfigExclusion> resolvedExclusions;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert resolvedExclusions == null : resolvedExclusions;
                final int finalMaxVotingConfigExclusions = TransportAddVotingConfigExclusionsAction.this.maxVotingConfigExclusions;
                resolvedExclusions = resolveVotingConfigExclusionsAndCheckMaximum(request, currentState, finalMaxVotingConfigExclusions);

                final CoordinationMetadata.Builder builder = CoordinationMetadata.builder(currentState.coordinationMetadata());
                resolvedExclusions.forEach(builder::addVotingConfigExclusion);
                final Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(builder.build()).build();
                final ClusterState newState = ClusterState.builder(currentState).metadata(newMetadata).build();
                assert newState.getVotingConfigExclusions().size() <= finalMaxVotingConfigExclusions;
                return newState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer
                    = new ClusterStateObserver(clusterService, request.getTimeout(), logger, threadPool.getThreadContext());

                final Set<String> excludedNodeIds = resolvedExclusions.stream().map(VotingConfigExclusion::getNodeId)
                    .collect(Collectors.toSet());

                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return excludedNodeIds.stream().noneMatch(votingConfigNodeIds::contains);
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(new AddVotingConfigExclusionsResponse());
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new HavenaskException("cluster service closed while waiting for voting config exclusions " +
                            resolvedExclusions + " to take effect"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new HavenaskTimeoutException("timed out waiting for voting config exclusions "
                            + resolvedExclusions + " to take effect"));
                    }
                };

                if (allNodesRemoved.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    observer.waitForNextChange(clusterStateListener, allNodesRemoved);
                }
            }
        });
    }

    private static Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(AddVotingConfigExclusionsRequest request,
                                                                                           ClusterState state,
                                                                                           int maxVotingConfigExclusions) {
        return request.resolveVotingConfigExclusionsAndCheckMaximum(state, maxVotingConfigExclusions,
            MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey());
    }

    @Override
    protected ClusterBlockException checkBlock(AddVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
