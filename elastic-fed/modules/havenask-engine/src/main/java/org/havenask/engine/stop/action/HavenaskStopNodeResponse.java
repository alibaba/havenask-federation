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

import org.havenask.action.support.nodes.BaseNodeResponse;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;

public class HavenaskStopNodeResponse extends BaseNodeResponse implements ToXContentObject {
    private final String nodeId;
    private final String result;
    private final int resultCode;

    public HavenaskStopNodeResponse(StreamInput in) throws IOException {
        super(in);
        result = in.readString();
        resultCode = in.readInt();
        nodeId = getNode().getId();
    }

    public HavenaskStopNodeResponse(DiscoveryNode node, String result, int resultCode) {
        super(node);
        this.result = result;
        this.resultCode = resultCode;
        this.nodeId = node.getId();
    }

    public String getResult() {
        return result;
    }

    public int getResultCode() {
        return resultCode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(result);
        out.writeInt(resultCode);
    }

    public static HavenaskStopNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new HavenaskStopNodeResponse(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // String to XContentBuilder
        builder.startObject();
        builder.field("result", result);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "    { \n"
            + "       nodeId: "
            + nodeId
            + ";\n"
            + "       result: "
            + result
            + "\n"
            + "       resultCode: "
            + resultCode
            + "\n"
            + "   }";
    }
}
