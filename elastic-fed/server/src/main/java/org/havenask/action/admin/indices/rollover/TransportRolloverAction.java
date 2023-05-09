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

package org.havenask.action.admin.indices.rollover;

import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.admin.indices.stats.IndicesStatsAction;
import org.havenask.action.admin.indices.stats.IndicesStatsRequest;
import org.havenask.action.admin.indices.stats.IndicesStatsResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.ActiveShardsObserver;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.master.TransportMasterNodeAction;
import org.havenask.client.Client;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.index.shard.DocsStats;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Main class to swap the index pointed to by an alias, given some conditions
 */
public class TransportRolloverAction extends TransportMasterNodeAction<RolloverRequest, RolloverResponse> {

    private final MetadataRolloverService rolloverService;
    private final ActiveShardsObserver activeShardsObserver;
    private final Client client;

    @Inject
    public TransportRolloverAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   MetadataRolloverService rolloverService, Client client) {
        super(RolloverAction.NAME, transportService, clusterService, threadPool, actionFilters, RolloverRequest::new,
            indexNameExpressionResolver);
        this.rolloverService = rolloverService;
        this.client = client;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RolloverResponse read(StreamInput in) throws IOException {
        return new RolloverResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(RolloverRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true,
            request.indicesOptions().expandWildcardsOpen(), request.indicesOptions().expandWildcardsClosed());

        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request));
    }

    @Override
    protected void masterOperation(RolloverRequest request, ClusterState state,
                                   ActionListener<RolloverResponse> listener) throws Exception {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void masterOperation(Task task, final RolloverRequest rolloverRequest, final ClusterState state,
                                   final ActionListener<RolloverResponse> listener) throws Exception {

        MetadataRolloverService.RolloverResult preResult =
            rolloverService.rolloverClusterState(state,
                rolloverRequest.getRolloverTarget(), rolloverRequest.getNewIndexName(), rolloverRequest.getCreateIndexRequest(),
                Collections.emptyList(), true, true);
        Metadata metadata = state.metadata();
        String sourceIndexName = preResult.sourceIndexName;
        String rolloverIndexName = preResult.rolloverIndexName;
        IndicesStatsRequest statsRequest = new IndicesStatsRequest().indices(rolloverRequest.getRolloverTarget())
            .clear()
            .indicesOptions(IndicesOptions.fromOptions(true, false, true, true))
            .docs(true);
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(IndicesStatsAction.INSTANCE, statsRequest,
            new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse statsResponse) {
                    final Map<String, Boolean> conditionResults = evaluateConditions(rolloverRequest.getConditions().values(),
                        metadata.index(sourceIndexName), statsResponse);

                    if (rolloverRequest.isDryRun()) {
                        listener.onResponse(
                            new RolloverResponse(sourceIndexName, rolloverIndexName, conditionResults, true, false, false, false));
                        return;
                    }
                    List<Condition<?>> metConditions = rolloverRequest.getConditions().values().stream()
                        .filter(condition -> conditionResults.get(condition.toString())).collect(Collectors.toList());
                    if (conditionResults.size() == 0 || metConditions.size() > 0) {
                        clusterService.submitStateUpdateTask("rollover_index source [" + sourceIndexName + "] to target ["
                            + rolloverIndexName + "]", new ClusterStateUpdateTask() {
                            @Override
                            public ClusterState execute(ClusterState currentState) throws Exception {
                                MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(currentState,
                                    rolloverRequest.getRolloverTarget(), rolloverRequest.getNewIndexName(),
                                    rolloverRequest.getCreateIndexRequest(), metConditions, false, false);
                                if (rolloverResult.sourceIndexName.equals(sourceIndexName) == false) {
                                    throw new HavenaskException("Concurrent modification of alias [{}] during rollover",
                                        rolloverRequest.getRolloverTarget());
                                }
                                return rolloverResult.clusterState;
                            }

                            @Override
                            public void onFailure(String source, Exception e) {
                                listener.onFailure(e);
                            }

                            @Override
                            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                if (newState.equals(oldState) == false) {
                                    activeShardsObserver.waitForActiveShards(new String[]{rolloverIndexName},
                                        rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                                        rolloverRequest.masterNodeTimeout(),
                                        isShardsAcknowledged -> listener.onResponse(new RolloverResponse(
                                            sourceIndexName, rolloverIndexName, conditionResults, false, true, true,
                                            isShardsAcknowledged)),
                                        listener::onFailure);
                                }
                            }
                        });
                    } else {
                        // conditions not met
                        listener.onResponse(
                            new RolloverResponse(sourceIndexName, rolloverIndexName, conditionResults, false, false, false, false)
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    static Map<String, Boolean> evaluateConditions(final Collection<Condition<?>> conditions,
                                                   @Nullable final DocsStats docsStats,
                                                   @Nullable final IndexMetadata metadata) {
        if (metadata == null) {
            return conditions.stream().collect(Collectors.toMap(Condition::toString, cond -> false));
        }
        final long numDocs = docsStats == null ? 0 : docsStats.getCount();
        final long indexSize = docsStats == null ? 0 : docsStats.getTotalSizeInBytes();
        final Condition.Stats stats = new Condition.Stats(numDocs, metadata.getCreationDate(), new ByteSizeValue(indexSize));
        return conditions.stream()
            .map(condition -> condition.evaluate(stats))
            .collect(Collectors.toMap(result -> result.condition.toString(), result -> result.matched));
    }

    static Map<String, Boolean> evaluateConditions(final Collection<Condition<?>> conditions,
                                                   @Nullable final IndexMetadata metadata,
                                                   @Nullable final IndicesStatsResponse statsResponse) {
        if (metadata == null) {
            return conditions.stream().collect(Collectors.toMap(Condition::toString, cond -> false));
        } else {
            final DocsStats docsStats = Optional.ofNullable(statsResponse)
                .map(stats -> stats.getIndex(metadata.getIndex().getName()))
                .map(indexStats -> indexStats.getPrimaries().getDocs())
                .orElse(null);
            return evaluateConditions(conditions, docsStats, metadata);
        }
    }
}
