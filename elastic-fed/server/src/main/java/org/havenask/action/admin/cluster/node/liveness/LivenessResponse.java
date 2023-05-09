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

package org.havenask.action.admin.cluster.node.liveness;

import org.havenask.action.ActionResponse;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport level private response for the transport handler registered under
 * {@value TransportLivenessAction#NAME}
 */
public final class LivenessResponse extends ActionResponse {

    private DiscoveryNode node;
    private ClusterName clusterName;

    public LivenessResponse() {}

    public LivenessResponse(StreamInput in) throws IOException {
        super(in);
        clusterName = new ClusterName(in);
        node = in.readOptionalWriteable(DiscoveryNode::new);
    }

    public LivenessResponse(ClusterName clusterName, DiscoveryNode node) {
        this.node = node;
        this.clusterName = clusterName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeOptionalWriteable(node);
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public DiscoveryNode getDiscoveryNode() {
        return node;
    }
}
