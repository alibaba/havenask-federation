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
import org.havenask.action.support.nodes.BaseNodesResponse;
import org.havenask.cluster.ClusterName;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

public class NodesStatsResponse extends BaseNodesResponse<NodeStats> implements ToXContentFragment {

    public NodesStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesStatsResponse(ClusterName clusterName, List<NodeStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeStats::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (NodeStats nodeStats : getNodes()) {
            builder.startObject(nodeStats.getNode().getId());
            builder.field("timestamp", nodeStats.getTimestamp());
            nodeStats.toXContent(builder, params);

            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
