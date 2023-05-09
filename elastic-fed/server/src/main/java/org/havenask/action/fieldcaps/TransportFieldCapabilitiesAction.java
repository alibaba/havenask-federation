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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.action.fieldcaps;

import org.havenask.action.ActionListener;
import org.havenask.action.OriginalIndices;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.client.Client;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.inject.Inject;
import org.havenask.common.util.concurrent.CountDown;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.RemoteClusterAware;
import org.havenask.transport.RemoteClusterService;
import org.havenask.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportFieldCapabilitiesIndexAction shardAction;
    private final RemoteClusterService remoteClusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportFieldCapabilitiesAction(TransportService transportService,
                                            ClusterService clusterService, ThreadPool threadPool,
                                            TransportFieldCapabilitiesIndexAction shardAction,
                                            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(FieldCapabilitiesAction.NAME, transportService, actionFilters, FieldCapabilitiesRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.shardAction = shardAction;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        // retrieve the initial timestamp in case the action is a cross cluster search
        long nowInMillis = request.nowInMillis() == null ? System.currentTimeMillis() : request.nowInMillis();
        final ClusterState clusterState = clusterService.state();
        final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(request.indicesOptions(),
            request.indices(), idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterState));
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final String[] concreteIndices;
        if (localIndices == null) {
            // in the case we have one or more remote indices but no local we don't expand to all local indices and just do remote indices
            concreteIndices = Strings.EMPTY_ARRAY;
        } else {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, localIndices);
        }
        final int totalNumRequest = concreteIndices.length + remoteClusterIndices.size();
        final CountDown completionCounter = new CountDown(totalNumRequest);
        final List<FieldCapabilitiesIndexResponse> indexResponses = Collections.synchronizedList(new ArrayList<>());
        final Runnable onResponse = () -> {
            if (completionCounter.countDown()) {
                if (request.isMergeResults()) {
                    listener.onResponse(merge(indexResponses, request.includeUnmapped()));
                } else {
                    listener.onResponse(new FieldCapabilitiesResponse(indexResponses));
                }
            }
        };
        if (totalNumRequest == 0) {
            listener.onResponse(new FieldCapabilitiesResponse(new String[0], Collections.emptyMap()));
        } else {
            ActionListener<FieldCapabilitiesIndexResponse> innerListener = new ActionListener<FieldCapabilitiesIndexResponse>() {
                @Override
                public void onResponse(FieldCapabilitiesIndexResponse result) {
                    if (result.canMatch()) {
                        indexResponses.add(result);
                    }
                    onResponse.run();
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO we should somehow inform the user that we failed
                    onResponse.run();
                }
            };
            for (String index : concreteIndices) {
                shardAction.execute(new FieldCapabilitiesIndexRequest(request.fields(), index, localIndices,
                    request.indexFilter(), nowInMillis), innerListener);
            }

            // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
            // send us back all individual index results.
            for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                String clusterAlias = remoteIndices.getKey();
                OriginalIndices originalIndices = remoteIndices.getValue();
                Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
                FieldCapabilitiesRequest remoteRequest = new FieldCapabilitiesRequest();
                remoteRequest.setMergeResults(false); // we need to merge on this node
                remoteRequest.indicesOptions(originalIndices.indicesOptions());
                remoteRequest.indices(originalIndices.indices());
                remoteRequest.fields(request.fields());
                remoteRequest.indexFilter(request.indexFilter());
                remoteRequest.nowInMillis(nowInMillis);
                remoteClusterClient.fieldCaps(remoteRequest,  ActionListener.wrap(response -> {
                    for (FieldCapabilitiesIndexResponse res : response.getIndexResponses()) {
                        indexResponses.add(new FieldCapabilitiesIndexResponse(RemoteClusterAware.
                            buildRemoteIndexName(clusterAlias, res.getIndexName()), res.get(), res.canMatch()));
                    }
                    onResponse.run();
                }, failure -> onResponse.run()));
            }
        }
    }

    private FieldCapabilitiesResponse merge(List<FieldCapabilitiesIndexResponse> indexResponses, boolean includeUnmapped) {
        String[] indices = indexResponses.stream()
            .map(FieldCapabilitiesIndexResponse::getIndexName)
            .sorted()
            .toArray(String[]::new);
        final Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder = new HashMap<> ();
        for (FieldCapabilitiesIndexResponse response : indexResponses) {
            innerMerge(responseMapBuilder, response.getIndexName(), response.get());
        }
        final Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> entry : responseMapBuilder.entrySet()) {
            final Map<String, FieldCapabilities.Builder> typeMapBuilder = entry.getValue();
            if (includeUnmapped) {
                addUnmappedFields(indices, entry.getKey(), typeMapBuilder);
            }
            boolean multiTypes = typeMapBuilder.size() > 1;
            final Map<String, FieldCapabilities> typeMap = new HashMap<>();
            for (Map.Entry<String, FieldCapabilities.Builder> fieldEntry : typeMapBuilder.entrySet()) {
                typeMap.put(fieldEntry.getKey(), fieldEntry.getValue().build(multiTypes));
            }
            responseMap.put(entry.getKey(), Collections.unmodifiableMap(typeMap));
        }

        return new FieldCapabilitiesResponse(indices, Collections.unmodifiableMap(responseMap));
    }

    private void addUnmappedFields(String[] indices, String field, Map<String, FieldCapabilities.Builder> typeMap) {
        Set<String> unmappedIndices = new HashSet<>();
        Arrays.stream(indices).forEach(unmappedIndices::add);
        typeMap.values().stream().forEach((b) -> b.getIndices().stream().forEach(unmappedIndices::remove));
        if (unmappedIndices.isEmpty() == false) {
            FieldCapabilities.Builder unmapped = new FieldCapabilities.Builder(field, "unmapped");
            typeMap.put("unmapped", unmapped);
            for (String index : unmappedIndices) {
                unmapped.add(index, false, false, Collections.emptyMap());
            }
        }
    }

    private void innerMerge(Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder,
                                String indexName, Map<String, IndexFieldCapabilities> map) {
        for (Map.Entry<String, IndexFieldCapabilities> entry : map.entrySet()) {
            final String field = entry.getKey();
            final IndexFieldCapabilities fieldCap = entry.getValue();
            Map<String, FieldCapabilities.Builder> typeMap = responseMapBuilder.computeIfAbsent(field, f -> new HashMap<>());
            FieldCapabilities.Builder builder = typeMap.computeIfAbsent(fieldCap.getType(),
                key -> new FieldCapabilities.Builder(field, key));
            builder.add(indexName, fieldCap.isSearchable(), fieldCap.isAggregatable(), fieldCap.meta());
        }
    }
}
