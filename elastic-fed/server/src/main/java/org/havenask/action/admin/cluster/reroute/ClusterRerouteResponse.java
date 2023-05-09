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

package org.havenask.action.admin.cluster.reroute;

import org.havenask.LegacyESVersion;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.routing.allocation.RoutingExplanations;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response returned after a cluster reroute request
 */
public class ClusterRerouteResponse extends AcknowledgedResponse implements ToXContentObject {

    private final ClusterState state;
    private final RoutingExplanations explanations;

    ClusterRerouteResponse(StreamInput in) throws IOException {
        super(in, in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0));
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            state = ClusterState.readFrom(in, null);
            explanations = RoutingExplanations.readFrom(in);
        } else {
            state = ClusterState.readFrom(in, null);
            acknowledged = in.readBoolean();
            explanations = RoutingExplanations.readFrom(in);
        }
    }

    ClusterRerouteResponse(boolean acknowledged, ClusterState state, RoutingExplanations explanations) {
        super(acknowledged);
        this.state = state;
        this.explanations = explanations;
    }

    /**
     * Returns the cluster state resulted from the cluster reroute request execution
     */
    public ClusterState getState() {
        return this.state;
    }

    public RoutingExplanations getExplanations() {
        return this.explanations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            super.writeTo(out);
            state.writeTo(out);
            RoutingExplanations.writeTo(explanations, out);
        } else {
            if (out.getVersion().onOrAfter(LegacyESVersion.V_6_3_0)) {
                state.writeTo(out);
            } else {
                ClusterModule.filterCustomsForPre63Clients(state).writeTo(out);
            }
            out.writeBoolean(acknowledged);
            RoutingExplanations.writeTo(explanations, out);
        }
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("state");
        state.toXContent(builder, params);
        builder.endObject();
        if (params.paramAsBoolean("explain", false)) {
            explanations.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
    }
}
