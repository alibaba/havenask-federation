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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.action.ActionListener;
import org.havenask.action.search.ClearScrollRequest;
import org.havenask.action.search.ClearScrollResponse;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.util.concurrent.CountDown;
import org.havenask.transport.Transport;
import org.havenask.transport.TransportResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ClearHavenaskScrollController implements Runnable {
    private final DiscoveryNodes nodes;
    private final HavenaskSearchTransportService havenaskSearchTransportService;
    private final CountDown expectedOps;
    private final ActionListener<ClearScrollResponse> listener;
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicInteger freedHavenaskScrollContext = new AtomicInteger(0);
    private final Logger logger;
    private final Runnable runner;

    ClearHavenaskScrollController(
        ClearScrollRequest request,
        ActionListener<ClearScrollResponse> listener,
        DiscoveryNodes nodes,
        Logger logger,
        HavenaskSearchTransportService havenaskSearchTransportService
    ) {
        this.nodes = nodes;
        this.logger = logger;
        this.havenaskSearchTransportService = havenaskSearchTransportService;
        this.listener = listener;
        List<String> scrollIds = request.getScrollIds();
        final int expectedOps;
        if (scrollIds.size() == 1 && "_all".equals(scrollIds.get(0))) {
            expectedOps = nodes.getSize();
            runner = this::cleanAllHavenaskScrolls;
        } else {
            Map<String, List<String>> nodeScrollSessionMap = new HashMap<>();
            for (String scrollId : request.getScrollIds()) {
                ParsedHavenaskScrollId parsedScrollId = TransportHavenaskSearchHelper.parseHavenaskScrollId(scrollId);
                String nodeId = parsedScrollId.getNodeId();
                if (!nodeScrollSessionMap.containsKey(nodeId)) {
                    List<String> ScrollSessionIds = new ArrayList<>();
                    ScrollSessionIds.add(parsedScrollId.getScrollSessionId());
                    nodeScrollSessionMap.put(nodeId, ScrollSessionIds);
                } else {
                    nodeScrollSessionMap.get(nodeId).add(parsedScrollId.getScrollSessionId());
                }
            }
            if (nodeScrollSessionMap.isEmpty()) {
                expectedOps = 0;
                runner = () -> { listener.onResponse(new ClearScrollResponse(true, 0)); };
            } else {
                expectedOps = nodeScrollSessionMap.size();
                runner = () -> cleanHavenaskScrollIds(nodeScrollSessionMap);
            }
        }
        this.expectedOps = new CountDown(expectedOps);
    }

    @Override
    public void run() {
        runner.run();
    }

    // TODO: add clusterAlias case
    private void cleanAllHavenaskScrolls() {
        for (final DiscoveryNode node : nodes) {
            try {
                Transport.Connection connection = havenaskSearchTransportService.getConnection(null, node);
                havenaskSearchTransportService.sendClearAllHavenaskScrollContexts(connection, new ActionListener<TransportResponse>() {
                    @Override
                    public void onResponse(TransportResponse transportResponse) {
                        onFreedHavenaskScrollContext(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailedFreedHavenaskScrollContext(e, node);
                    }
                });
            } catch (Exception e) {
                onFailedFreedHavenaskScrollContext(e, node);
            }
        }
    }

    private void cleanHavenaskScrollIds(Map<String, List<String>> nodeScrollSessionMap) {
        Set<String> nodeIdSet = nodeScrollSessionMap.keySet();
        for (String nodeId : nodeIdSet) {
            DiscoveryNode node = nodes.get(nodeId);
            if (Objects.isNull(node)) {
                onFreedHavenaskScrollContext(false);
            } else {
                try {
                    Transport.Connection connection = havenaskSearchTransportService.getConnection(null, node);
                    havenaskSearchTransportService.sendFreeHavenaskScrollContext(
                        connection,
                        nodeScrollSessionMap.get(nodeId),
                        ActionListener.wrap(
                            freed -> onFreedHavenaskScrollContext(freed.isFreed()),
                            e -> onFailedFreedHavenaskScrollContext(e, node)
                        )
                    );

                } catch (Exception e) {
                    onFailedFreedHavenaskScrollContext(e, node);
                }
            }
        }
    }

    private void onFreedHavenaskScrollContext(boolean freed) {
        if (freed) {
            freedHavenaskScrollContext.incrementAndGet();
        }
        if (expectedOps.countDown()) {
            boolean succeeded = hasFailed.get() == false;
            listener.onResponse(new ClearScrollResponse(succeeded, freedHavenaskScrollContext.get()));
        }
    }

    private void onFailedFreedHavenaskScrollContext(Throwable e, DiscoveryNode node) {
        logger.warn(() -> new ParameterizedMessage("Clear SC failed on node[{}]", node), e);
        hasFailed.set(true);
        if (expectedOps.countDown()) {
            listener.onResponse(new ClearScrollResponse(false, freedHavenaskScrollContext.get()));
        }
    }
}
