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

package org.havenask.action.admin.cluster.node.stats;

import org.havenask.action.FailedNodeException;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.nodes.BaseNodeRequest;
import org.havenask.action.support.nodes.TransportNodesAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.node.NodeService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TransportNodesStatsAction extends TransportNodesAction<NodesStatsRequest,
                                                                    NodesStatsResponse,
                                                                    TransportNodesStatsAction.NodeStatsRequest,
                                                                    NodeStats> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesStatsAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                     NodeService nodeService, ActionFilters actionFilters) {
        super(NodesStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            NodesStatsRequest::new, NodeStatsRequest::new, ThreadPool.Names.MANAGEMENT, NodeStats.class);
        this.nodeService = nodeService;
    }

    @Override
    protected NodesStatsResponse newResponse(NodesStatsRequest request, List<NodeStats> responses, List<FailedNodeException> failures) {
        return new NodesStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStatsRequest newNodeRequest(NodesStatsRequest request) {
        return new NodeStatsRequest(request);
    }

    @Override
    protected NodeStats newNodeResponse(StreamInput in) throws IOException {
        return new NodeStats(in);
    }

    @Override
    protected NodeStats nodeOperation(NodeStatsRequest nodeStatsRequest) {
        NodesStatsRequest request = nodeStatsRequest.request;
        Set<String> metrics = request.requestedMetrics();
        return nodeService.stats(
            request.indices(),
            NodesStatsRequest.Metric.OS.containedIn(metrics),
            NodesStatsRequest.Metric.PROCESS.containedIn(metrics),
            NodesStatsRequest.Metric.JVM.containedIn(metrics),
            NodesStatsRequest.Metric.THREAD_POOL.containedIn(metrics),
            NodesStatsRequest.Metric.FS.containedIn(metrics),
            NodesStatsRequest.Metric.TRANSPORT.containedIn(metrics),
            NodesStatsRequest.Metric.HTTP.containedIn(metrics),
            NodesStatsRequest.Metric.BREAKER.containedIn(metrics),
            NodesStatsRequest.Metric.SCRIPT.containedIn(metrics),
            NodesStatsRequest.Metric.DISCOVERY.containedIn(metrics),
            NodesStatsRequest.Metric.INGEST.containedIn(metrics),
            NodesStatsRequest.Metric.ADAPTIVE_SELECTION.containedIn(metrics),
            NodesStatsRequest.Metric.SCRIPT_CACHE.containedIn(metrics),
            NodesStatsRequest.Metric.INDEXING_PRESSURE.containedIn(metrics));
    }

    public static class NodeStatsRequest extends BaseNodeRequest {

        NodesStatsRequest request;

        public NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesStatsRequest(in);
        }

        NodeStatsRequest(NodesStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
