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

package org.havenask.action.admin.indices.mapping.get;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class TransportGetFieldMappingsAction extends HandledTransportAction<GetFieldMappingsRequest, GetFieldMappingsResponse> {

    private final ClusterService clusterService;
    private final TransportGetFieldMappingsIndexAction shardAction;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportGetFieldMappingsAction(TransportService transportService, ClusterService clusterService,
                                           TransportGetFieldMappingsIndexAction shardAction,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetFieldMappingsAction.NAME, transportService, actionFilters, GetFieldMappingsRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, GetFieldMappingsRequest request, final ActionListener<GetFieldMappingsResponse> listener) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(concreteIndices.length);
        final AtomicReferenceArray<Object> indexResponses = new AtomicReferenceArray<>(concreteIndices.length);

        if (concreteIndices.length == 0) {
            listener.onResponse(new GetFieldMappingsResponse(emptyMap()));
        } else {
            boolean probablySingleFieldRequest = concreteIndices.length == 1 && request.types().length == 1 && request.fields().length == 1;
            for (final String index : concreteIndices) {
                GetFieldMappingsIndexRequest shardRequest = new GetFieldMappingsIndexRequest(request, index, probablySingleFieldRequest);
                shardAction.execute(shardRequest, new ActionListener<GetFieldMappingsResponse>() {
                    @Override
                    public void onResponse(GetFieldMappingsResponse result) {
                        indexResponses.set(indexCounter.getAndIncrement(), result);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(indexResponses));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        int index = indexCounter.getAndIncrement();
                        indexResponses.set(index, e);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(indexResponses));
                        }
                    }
                });
            }
        }
    }

    private GetFieldMappingsResponse merge(AtomicReferenceArray<Object> indexResponses) {
        Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>> mergedResponses = new HashMap<>();
        for (int i = 0; i < indexResponses.length(); i++) {
            Object element = indexResponses.get(i);
            if (element instanceof GetFieldMappingsResponse) {
                GetFieldMappingsResponse response = (GetFieldMappingsResponse) element;
                mergedResponses.putAll(response.mappings());
            }
        }
        return new GetFieldMappingsResponse(unmodifiableMap(mergedResponses));
    }
}
