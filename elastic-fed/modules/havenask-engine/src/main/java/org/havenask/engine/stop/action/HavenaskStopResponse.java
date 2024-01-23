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

package org.havenask.engine.stop.action;

import org.havenask.action.FailedNodeException;
import org.havenask.action.support.nodes.BaseNodesResponse;
import org.havenask.cluster.ClusterName;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HavenaskStopResponse extends BaseNodesResponse<HavenaskStopNodeResponse> {
    private List<String> results = new ArrayList<>();
    private List<Integer> resultCodes = new ArrayList<>();

    public HavenaskStopResponse(StreamInput in) throws IOException {
        super(in);
        for (HavenaskStopNodeResponse node : getNodes()) {
            results.add(node.getResult());
            resultCodes.add(node.getResultCode());
        }
    }

    public HavenaskStopResponse(ClusterName clusterName, List<HavenaskStopNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        for (HavenaskStopNodeResponse node : nodes) {
            results.add(node.getResult());
            resultCodes.add(node.getResultCode());
        }
    }

    @Override
    protected List<HavenaskStopNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(HavenaskStopNodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<HavenaskStopNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
