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

import org.havenask.action.ActionListener;
import org.havenask.action.ActionListenerResponseHandler;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.Nullable;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.engine.HavenaskScrollService;
import org.havenask.engine.search.internal.HavenaskScrollContext;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.Transport;
import org.havenask.transport.TransportActionProxy;
import org.havenask.transport.TransportRequest;
import org.havenask.transport.TransportRequestOptions;
import org.havenask.transport.TransportResponse;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class HavenaskSearchTransportService {
    public static final String FREE_HAVENASK_CONTEXT_SCROLL_ACTION_NAME = "indices:data/read/search[free_havenask_context/scroll]";
    public static final String CLEAR_HAVENASK_SCROLL_CONTEXTS_ACTION_NAME = "indices:data/read/search[clear_havenask_scroll_contexts]";
    private final TransportService transportService;

    @Inject
    public HavenaskSearchTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    public void sendFreeHavenaskScrollContext(
        Transport.Connection connection,
        List<String> scrollSessionId,
        final ActionListener<SearchHavenaskScrollFreeContextResponse> listener
    ) {
        transportService.sendRequest(
            connection,
            FREE_HAVENASK_CONTEXT_SCROLL_ACTION_NAME,
            new SearchHavenaskScrollFreeContextRequest(scrollSessionId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, SearchHavenaskScrollFreeContextResponse::new)
        );
    }

    public void sendClearAllHavenaskScrollContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(
            connection,
            CLEAR_HAVENASK_SCROLL_CONTEXTS_ACTION_NAME,
            TransportRequest.Empty.INSTANCE,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, (in) -> TransportResponse.Empty.INSTANCE)
        );
    }

    static class SearchHavenaskScrollFreeContextRequest extends TransportRequest {
        private List<String> scrollSessionIds;

        SearchHavenaskScrollFreeContextRequest(List<String> scrollSessionIds) {
            this.scrollSessionIds = scrollSessionIds;
        }

        SearchHavenaskScrollFreeContextRequest(StreamInput in) throws IOException {
            this.scrollSessionIds = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(scrollSessionIds);
        }

        public List<String> getScrollSessionIds() {
            return this.scrollSessionIds;
        }
    }

    public static class SearchHavenaskScrollFreeContextResponse extends TransportResponse {
        private boolean freed;

        SearchHavenaskScrollFreeContextResponse(boolean freed) {
            this.freed = freed;
        }

        SearchHavenaskScrollFreeContextResponse(StreamInput in) throws IOException {
            this.freed = in.readBoolean();
        }

        public boolean isFreed() {
            return freed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(freed);
        }
    }

    public static void registerRequestHandler(TransportService transportService, HavenaskScrollService havenaskScrollService) {
        transportService.registerRequestHandler(
            FREE_HAVENASK_CONTEXT_SCROLL_ACTION_NAME,
            ThreadPool.Names.SAME,
            SearchHavenaskScrollFreeContextRequest::new,
            ((request, channel, task) -> {
                List<String> scrollSessionIds = request.getScrollSessionIds();
                boolean succeeded = true;
                for (String scrollSessionId : scrollSessionIds) {
                    HavenaskScrollContext removed = havenaskScrollService.removeScrollContext(scrollSessionId);
                    if (Objects.isNull(removed)) {
                        succeeded = false;
                    }
                }
                channel.sendResponse(new SearchHavenaskScrollFreeContextResponse(succeeded));
            })
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            FREE_HAVENASK_CONTEXT_SCROLL_ACTION_NAME,
            SearchHavenaskScrollFreeContextResponse::new
        );

        transportService.registerRequestHandler(
            CLEAR_HAVENASK_SCROLL_CONTEXTS_ACTION_NAME,
            ThreadPool.Names.SAME,
            TransportRequest.Empty::new,
            ((request, channel, task) -> {
                havenaskScrollService.removeAllHavenaskScrollContext();
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            })
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            CLEAR_HAVENASK_SCROLL_CONTEXTS_ACTION_NAME,
            (in) -> TransportResponse.Empty.INSTANCE
        );
    }

    public Transport.Connection getConnection(@Nullable String clusterAlias, DiscoveryNode node) {
        if (clusterAlias == null) {
            return transportService.getConnection(node);
        } else {
            return transportService.getRemoteClusterService().getConnection(node, clusterAlias);
        }
    }
}
