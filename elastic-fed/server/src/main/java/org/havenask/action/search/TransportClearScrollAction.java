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

package org.havenask.action.search;

import org.apache.lucene.store.ByteArrayDataInput;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionListenerResponseHandler;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class TransportClearScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportClearScrollAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                      SearchTransportService searchTransportService, NamedWriteableRegistry namedWriteableRegistry) {
        super(ClearScrollAction.NAME, transportService, actionFilters, ClearScrollRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        try {
            List<String> elasticScrollIds = new ArrayList<>();
            List<String> havenaskScrollIds = new ArrayList<>();
            seperateElasticScrollIdAndHavenaskScrollId(request.getScrollIds(), elasticScrollIds, havenaskScrollIds);
            if (!havenaskScrollIds.isEmpty()) {
                if (!elasticScrollIds.isEmpty()) {
                    ActionListener<ClearScrollResponse> havenaskListener= new ActionListener<ClearScrollResponse>() {
                        @Override
                        public void onResponse(ClearScrollResponse clearScrollResponse) {

                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new RuntimeException("clear havenask scrollId failed", e);
                        }
                    };

                    transportService.sendRequest(
                            clusterService.state().nodes().getLocalNode(),
                            CLEAR_HAVENASK_SCROLL_ACTION,
                            request,
                            new ActionListenerResponseHandler<>(havenaskListener, ClearScrollAction.INSTANCE.getResponseReader())
                    );

                    Runnable runnable = new ClearScrollController(
                            request, listener, clusterService.state().nodes(), logger, searchTransportService);
                    runnable.run();
                } else {
                    transportService.sendRequest(
                            clusterService.state().nodes().getLocalNode(),
                            CLEAR_HAVENASK_SCROLL_ACTION,
                            request,
                            new ActionListenerResponseHandler<>(listener, ClearScrollAction.INSTANCE.getResponseReader())
                    );
                }
            } else {
                Runnable runnable = new ClearScrollController(
                        request, listener, clusterService.state().nodes(), logger, searchTransportService);
                runnable.run();
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private static final String CLEAR_HAVENASK_SCROLL_ACTION = "indices:data/read/havenask/scroll/clear";

    private void seperateElasticScrollIdAndHavenaskScrollId(List<String> scrollIds, List<String> elasticScrollIds, List<String> havenaskScrollIds) throws IOException {
        if (scrollIds.size() == 1 && "_all".equals(scrollIds.get(0))) {
            elasticScrollIds.add(scrollIds.get(0));
            havenaskScrollIds.add(scrollIds.get(0));
            return;
        }

        for (String scrollId : scrollIds) {
            byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            final String firstChunk = in.readString();
            if ("havenask_scroll_id".equals(firstChunk)) {
                havenaskScrollIds.add(scrollId);
            } else {
                elasticScrollIds.add(scrollId);
            }
        }
    }

}
