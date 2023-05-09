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

package org.havenask.action.admin.cluster.state;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionResponse;
import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * The response for getting the cluster state.
 */
public class ClusterStateResponse extends ActionResponse {

    private ClusterName clusterName;
    private ClusterState clusterState;
    private boolean waitForTimedOut = false;

    public ClusterStateResponse(StreamInput in) throws IOException {
        super(in);
        clusterName = new ClusterName(in);
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_6_0)) {
            clusterState = in.readOptionalWriteable(innerIn -> ClusterState.readFrom(innerIn, null));
        } else {
            clusterState = ClusterState.readFrom(in, null);
        }
        if (in.getVersion().before(LegacyESVersion.V_7_0_0)) {
            new ByteSizeValue(in);
        }
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_6_0)) {
            waitForTimedOut = in.readBoolean();
        }
    }

    public ClusterStateResponse(ClusterName clusterName, ClusterState clusterState, boolean waitForTimedOut) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.waitForTimedOut = waitForTimedOut;
    }

    /**
     * The requested cluster state.  Only the parts of the cluster state that were
     * requested are included in the returned {@link ClusterState} instance.
     */
    public ClusterState getState() {
        return this.clusterState;
    }

    /**
     * The name of the cluster.
     */
    public ClusterName getClusterName() {
        return this.clusterName;
    }

    /**
     * Returns whether the request timed out waiting for a cluster state with a metadata version equal or
     * higher than the specified metadata.
     */
    public boolean isWaitForTimedOut() {
        return waitForTimedOut;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_6_0)) {
            out.writeOptionalWriteable(clusterState);
        } else {
            if (out.getVersion().onOrAfter(LegacyESVersion.V_6_3_0)) {
                clusterState.writeTo(out);
            } else {
                ClusterModule.filterCustomsForPre63Clients(clusterState).writeTo(out);
            }
        }
        if (out.getVersion().before(LegacyESVersion.V_7_0_0)) {
            ByteSizeValue.ZERO.writeTo(out);
        }
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_6_0)) {
            out.writeBoolean(waitForTimedOut);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateResponse response = (ClusterStateResponse) o;
        return waitForTimedOut == response.waitForTimedOut &&
            Objects.equals(clusterName, response.clusterName) &&
            // Best effort. Only compare cluster state version and master node id,
            // because cluster state doesn't implement equals()
            Objects.equals(getVersion(clusterState), getVersion(response.clusterState)) &&
            Objects.equals(getMasterNodeId(clusterState), getMasterNodeId(response.clusterState));
    }

    @Override
    public int hashCode() {
        // Best effort. Only use cluster state version and master node id,
        // because cluster state doesn't implement  hashcode()
        return Objects.hash(
            clusterName,
            getVersion(clusterState),
            getMasterNodeId(clusterState),
            waitForTimedOut
        );
    }

    private static String getMasterNodeId(ClusterState clusterState) {
        if (clusterState == null) {
            return null;
        }
        DiscoveryNodes nodes = clusterState.getNodes();
        if (nodes != null) {
            return nodes.getMasterNodeId();
        } else {
            return null;
        }
    }

    private static Long getVersion(ClusterState clusterState) {
        if (clusterState != null) {
            return clusterState.getVersion();
        } else {
            return null;
        }
    }

}
