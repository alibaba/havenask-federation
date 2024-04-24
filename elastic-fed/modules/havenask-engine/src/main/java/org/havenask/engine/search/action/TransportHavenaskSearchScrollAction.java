/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.search.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionListenerResponseHandler;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.search.SearchScrollRequest;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.engine.HavenaskScrollService;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.http.QrsHttpClient;
import org.havenask.engine.search.dsl.DSLSession;
import org.havenask.engine.search.internal.HavenaskScrollContext;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportResponseHandler;
import org.havenask.transport.TransportService;

import java.util.Objects;

public class TransportHavenaskSearchScrollAction extends HandledTransportAction<SearchScrollRequest, SearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportHavenaskSearchScrollAction.class);
    private ClusterService clusterService;
    private TransportService transportService;
    private HavenaskScrollService havenaskScrollService;
    private QrsClient qrsClient;

    @Inject
    public TransportHavenaskSearchScrollAction(
        ClusterService clusterService,
        TransportService transportService,
        HavenaskScrollService havenaskScrollService,
        NativeProcessControlService nativeProcessControlService,
        ActionFilters actionFilters
    ) {
        super(HavenaskSearchScrollAction.NAME, transportService, actionFilters, SearchScrollRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.havenaskScrollService = havenaskScrollService;
        this.qrsClient = new QrsHttpClient(nativeProcessControlService.getQrsHttpPort());
    }

    @Override
    protected void doExecute(Task task, SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        try {
            ParsedHavenaskScrollId havenaskScrollId = TransportHavenaskSearchHelper.parseHavenaskScrollId(request.scrollId());
            // 如果不是本节点的处理内容，则转发到对应节点进行处理
            if (!clusterService.localNode().getId().equals(havenaskScrollId.getNodeId())) {
                DiscoveryNode targetNode = clusterService.state().nodes().get(havenaskScrollId.getNodeId());
                TransportResponseHandler<SearchResponse> responseHandler = new ActionListenerResponseHandler<>(
                    listener,
                    HavenaskSearchScrollAction.INSTANCE.getResponseReader()
                );
                transportService.sendRequest(targetNode, HavenaskSearchScrollAction.INSTANCE.name(), request, responseHandler);
                return;
            }

            HavenaskScrollContext havenaskScrollContext = havenaskScrollService.getScrollContext(havenaskScrollId.getScrollSessionId());
            if (Objects.isNull(havenaskScrollContext)) {
                throw new HavenaskException("no havenask scroll context found, sessionId: " + havenaskScrollId.getScrollSessionId());
            }
            DSLSession session = havenaskScrollContext.getDSLSession();

            havenaskScrollContext.markAsUsed(request.scroll().keepAlive().getMillis());

            SearchResponse searchResponse = session.execute();
            listener.onResponse(searchResponse);
        } catch (Exception e) {
            logger.error("Failed to execute havenask scroll search, ", e);
            listener.onFailure(e);
        }

    }
}
