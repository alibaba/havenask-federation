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

package org.havenask.action.admin.indices.dangling.delete;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.FailedNodeException;
import org.havenask.action.admin.indices.dangling.DanglingIndexInfo;
import org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.havenask.action.admin.indices.dangling.list.NodeListDanglingIndicesResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.action.support.master.TransportMasterNodeAction;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.AckedClusterStateUpdateTask;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.metadata.IndexGraveyard;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the deletion of a dangling index. When handling a {@link DeleteDanglingIndexAction},
 * this class first checks that such a dangling index exists. It then submits a cluster state update
 * to add the index to the index graveyard.
 */
public class TransportDeleteDanglingIndexAction extends TransportMasterNodeAction<DeleteDanglingIndexRequest, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportDeleteDanglingIndexAction.class);

    private final Settings settings;
    private final NodeClient nodeClient;

    @Inject
    public TransportDeleteDanglingIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        NodeClient nodeClient
    ) {
        super(
            DeleteDanglingIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDanglingIndexRequest::new,
            indexNameExpressionResolver
        );
        this.settings = settings;
        this.nodeClient = nodeClient;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(
        DeleteDanglingIndexRequest deleteRequest,
        ClusterState state,
        ActionListener<AcknowledgedResponse> deleteListener
    ) throws Exception {
        findDanglingIndex(deleteRequest.getIndexUUID(), new ActionListener<Index>() {
            @Override
            public void onResponse(Index indexToDelete) {
                // This flag is checked at this point so that we always check that the supplied index ID
                // does correspond to a dangling index.
                if (deleteRequest.isAcceptDataLoss() == false) {
                    deleteListener.onFailure(new IllegalArgumentException("accept_data_loss must be set to true"));
                    return;
                }

                String indexName = indexToDelete.getName();
                String indexUUID = indexToDelete.getUUID();

                final ActionListener<AcknowledgedResponse> clusterStateUpdatedListener = new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        deleteListener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("Failed to delete dangling index [" + indexName + "] [" + indexUUID + "]", e);
                        deleteListener.onFailure(e);
                    }
                };

                final String taskSource = "delete-dangling-index [" + indexName + "] [" + indexUUID + "]";

                clusterService.submitStateUpdateTask(
                    taskSource,
                    new AckedClusterStateUpdateTask<AcknowledgedResponse>(deleteRequest, clusterStateUpdatedListener) {

                        @Override
                        protected AcknowledgedResponse newResponse(boolean acknowledged) {
                            return new AcknowledgedResponse(acknowledged);
                        }

                        @Override
                        public ClusterState execute(final ClusterState currentState) {
                            return deleteDanglingIndex(currentState, indexToDelete);
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Failed to find dangling index [" + deleteRequest.getIndexUUID() + "]", e);
                deleteListener.onFailure(e);
            }
        });
    }

    private ClusterState deleteDanglingIndex(ClusterState currentState, Index indexToDelete) {
        final Metadata metaData = currentState.getMetadata();

        for (ObjectObjectCursor<String, IndexMetadata> each : metaData.indices()) {
            if (indexToDelete.getUUID().equals(each.value.getIndexUUID())) {
                throw new IllegalArgumentException(
                    "Refusing to delete dangling index "
                        + indexToDelete
                        + " as an index with UUID ["
                        + indexToDelete.getUUID()
                        + "] already exists in the cluster state"
                );
            }
        }

        // By definition, a dangling index is an index not present in the cluster state and with no tombstone,
        // so we shouldn't reach this point if these conditions aren't met. For super-safety, however, check
        // that a tombstone doesn't already exist for this index.
        if (metaData.indexGraveyard().containsIndex(indexToDelete)) {
            return currentState;
        }

        Metadata.Builder metaDataBuilder = Metadata.builder(metaData);

        final IndexGraveyard newGraveyard = IndexGraveyard.builder(metaDataBuilder.indexGraveyard())
            .addTombstone(indexToDelete)
            .build(settings);
        metaDataBuilder.indexGraveyard(newGraveyard);

        return ClusterState.builder(currentState).metadata(metaDataBuilder.build()).build();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDanglingIndexRequest request, ClusterState state) {
        return null;
    }

    private void findDanglingIndex(String indexUUID, ActionListener<Index> listener) {
        this.nodeClient.execute(
            ListDanglingIndicesAction.INSTANCE,
            new ListDanglingIndicesRequest(indexUUID),
            new ActionListener<ListDanglingIndicesResponse>() {
                @Override
                public void onResponse(ListDanglingIndicesResponse response) {
                    if (response.hasFailures()) {
                        final String nodeIds = response.failures()
                            .stream()
                            .map(FailedNodeException::nodeId)
                            .collect(Collectors.joining(","));
                        HavenaskException e = new HavenaskException("Failed to query nodes [" + nodeIds + "]");

                        for (FailedNodeException failure : response.failures()) {
                            logger.error("Failed to query node [" + failure.nodeId() + "]", failure);
                            e.addSuppressed(failure);
                        }

                        listener.onFailure(e);
                        return;
                    }

                    final List<NodeListDanglingIndicesResponse> nodes = response.getNodes();

                    for (NodeListDanglingIndicesResponse nodeResponse : nodes) {
                        for (DanglingIndexInfo each : nodeResponse.getDanglingIndices()) {
                            if (each.getIndexUUID().equals(indexUUID)) {
                                listener.onResponse(new Index(each.getIndexName(), each.getIndexUUID()));
                                return;
                            }
                        }
                    }

                    listener.onFailure(new IllegalArgumentException("No dangling index found for UUID [" + indexUUID + "]"));
                }

                @Override
                public void onFailure(Exception exp) {
                    listener.onFailure(exp);
                }
            }
        );
    }
}
