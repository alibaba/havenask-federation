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

package org.havenask.rest.action.cat;

import org.havenask.action.admin.cluster.node.info.NodeInfo;
import org.havenask.action.admin.cluster.node.info.NodesInfoRequest;
import org.havenask.action.admin.cluster.node.info.NodesInfoResponse;
import org.havenask.action.admin.cluster.state.ClusterStateRequest;
import org.havenask.action.admin.cluster.state.ClusterStateResponse;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.Strings;
import org.havenask.common.Table;
import org.havenask.monitor.process.ProcessInfo;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.action.RestActionListener;
import org.havenask.rest.action.RestResponseListener;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.havenask.rest.RestRequest.Method.GET;

public class RestNodeAttrsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/nodeattrs"));
    }

    @Override
    public String getName() {
        return "cat_node_attrs_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/nodeattrs\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.timeout(request.param("timeout"));
                nodesInfoRequest.clear()
                    .addMetric(NodesInfoRequest.Metric.PROCESS.metricName());
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestResponseListener<NodesInfoResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(NodesInfoResponse nodesInfoResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(request, clusterStateResponse, nodesInfoResponse), channel);
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("node", "default:true;alias:name;desc:node name");
        table.addCell("id",   "default:false;alias:id,nodeId;desc:unique node id");
        table.addCell("pid",  "default:false;alias:p;desc:process id");
        table.addCell("host", "alias:h;desc:host name");
        table.addCell("ip",   "alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");
        table.addCell("attr", "default:true;alias:attr.name;desc:attribute description");
        table.addCell("value","default:true;alias:attr.value;desc:attribute value");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo) {
        boolean fullId = req.paramAsBoolean("full_id", false);

        DiscoveryNodes nodes = state.getState().nodes();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : nodes) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
            for (Map.Entry<String, String> attrEntry : node.getAttributes().entrySet()) {
                table.startRow();
                table.addCell(node.getName());
                table.addCell(fullId ? node.getId() : Strings.substring(node.getId(), 0, 4));
                table.addCell(info == null ? null : info.getInfo(ProcessInfo.class).getId());
                table.addCell(node.getHostName());
                table.addCell(node.getHostAddress());
                table.addCell(node.getAddress().address().getPort());
                table.addCell(attrEntry.getKey());
                table.addCell(attrEntry.getValue());
                table.endRow();
            }
        }
        return table;
    }
}
