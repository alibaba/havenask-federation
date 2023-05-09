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

package org.havenask.cluster.health;

import org.havenask.Version;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.IndexRoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public final class ClusterStateHealth implements Iterable<ClusterIndexHealth>, Writeable {

    private final int numberOfNodes;
    private final int numberOfDataNodes;
    private final boolean hasDiscoveredMaster;
    private final int activeShards;
    private final int relocatingShards;
    private final int activePrimaryShards;
    private final int initializingShards;
    private final int unassignedShards;
    private final double activeShardsPercent;
    private final ClusterHealthStatus status;
    private final Map<String, ClusterIndexHealth> indices;

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and all indices in the cluster.
     *
     * @param clusterState The current cluster state. Must not be null.
     */
    public ClusterStateHealth(final ClusterState clusterState) {
        this(clusterState, clusterState.metadata().getConcreteAllIndices());
    }

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and the provided index names.
     *
     * @param clusterState    The current cluster state. Must not be null.
     * @param concreteIndices An array of index names to consider. Must not be null but may be empty.
     */
    public ClusterStateHealth(final ClusterState clusterState, final String[] concreteIndices) {
        numberOfNodes = clusterState.nodes().getSize();
        numberOfDataNodes = clusterState.nodes().getDataNodes().size();
        hasDiscoveredMaster = clusterState.nodes().getMasterNodeId() != null;
        indices = new HashMap<>();
        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexRoutingTable == null) {
                continue;
            }

            ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable);

            indices.put(indexHealth.getIndex(), indexHealth);
        }

        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;

        for (ClusterIndexHealth indexHealth : indices.values()) {
            computeActivePrimaryShards += indexHealth.getActivePrimaryShards();
            computeActiveShards += indexHealth.getActiveShards();
            computeRelocatingShards += indexHealth.getRelocatingShards();
            computeInitializingShards += indexHealth.getInitializingShards();
            computeUnassignedShards += indexHealth.getUnassignedShards();
            if (indexHealth.getStatus() == ClusterHealthStatus.RED) {
                computeStatus = ClusterHealthStatus.RED;
            } else if (indexHealth.getStatus() == ClusterHealthStatus.YELLOW && computeStatus != ClusterHealthStatus.RED) {
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        }

        if (clusterState.blocks().hasGlobalBlockWithStatus(RestStatus.SERVICE_UNAVAILABLE)) {
            computeStatus = ClusterHealthStatus.RED;
        }

        this.status = computeStatus;
        this.activePrimaryShards = computeActivePrimaryShards;
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;

        // shortcut on green
        if (computeStatus.equals(ClusterHealthStatus.GREEN)) {
            this.activeShardsPercent = 100;
        } else {
            List<ShardRouting> shardRoutings = clusterState.getRoutingTable().allShards();
            int activeShardCount = 0;
            int totalShardCount = 0;
            for (ShardRouting shardRouting : shardRoutings) {
                if (shardRouting.active()) activeShardCount++;
                totalShardCount++;
            }
            this.activeShardsPercent = (((double) activeShardCount) / totalShardCount) * 100;
        }
    }

    public ClusterStateHealth(final StreamInput in) throws IOException {
        activePrimaryShards = in.readVInt();
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        numberOfNodes = in.readVInt();
        numberOfDataNodes = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_1_0_0)) {
            hasDiscoveredMaster = in.readBoolean();
        } else {
            hasDiscoveredMaster = true;
        }
        status = ClusterHealthStatus.fromValue(in.readByte());
        int size = in.readVInt();
        indices = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            ClusterIndexHealth indexHealth = new ClusterIndexHealth(in);
            indices.put(indexHealth.getIndex(), indexHealth);
        }
        activeShardsPercent = in.readDouble();
    }

    /**
     * For ClusterHealthResponse's XContent Parser
     */
    public ClusterStateHealth(int activePrimaryShards, int activeShards, int relocatingShards, int initializingShards, int unassignedShards,
            int numberOfNodes, int numberOfDataNodes, boolean hasDiscoveredMaster, double activeShardsPercent, ClusterHealthStatus status,
        Map<String, ClusterIndexHealth> indices) {
        this.activePrimaryShards = activePrimaryShards;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.numberOfNodes = numberOfNodes;
        this.numberOfDataNodes = numberOfDataNodes;
        this.hasDiscoveredMaster = hasDiscoveredMaster;
        this.activeShardsPercent = activeShardsPercent;
        this.status = status;
        this.indices = indices;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }

    public int getNumberOfDataNodes() {
        return this.numberOfDataNodes;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return Collections.unmodifiableMap(indices);
    }

    public double getActiveShardsPercent() {
        return activeShardsPercent;
    }

    public boolean hasDiscoveredMaster() {
        return hasDiscoveredMaster;
    }

    @Override
    public Iterator<ClusterIndexHealth> iterator() {
        return indices.values().iterator();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(activePrimaryShards);
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeVInt(numberOfNodes);
        out.writeVInt(numberOfDataNodes);
        if (out.getVersion().onOrAfter(Version.V_1_0_0)) {
            out.writeBoolean(hasDiscoveredMaster);
        }
        out.writeByte(status.value());
        out.writeVInt(indices.size());
        for (ClusterIndexHealth indexHealth : this) {
            indexHealth.writeTo(out);
        }
        out.writeDouble(activeShardsPercent);
    }

    @Override
    public String toString() {
        return "ClusterStateHealth{" +
                "numberOfNodes=" + numberOfNodes +
                ", numberOfDataNodes=" + numberOfDataNodes +
                ", hasDiscoveredMaster=" + hasDiscoveredMaster +
                ", activeShards=" + activeShards +
                ", relocatingShards=" + relocatingShards +
                ", activePrimaryShards=" + activePrimaryShards +
                ", initializingShards=" + initializingShards +
                ", unassignedShards=" + unassignedShards +
                ", activeShardsPercent=" + activeShardsPercent +
                ", status=" + status +
                ", indices.size=" + (indices == null ? "null" : indices.size()) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateHealth that = (ClusterStateHealth) o;
        return numberOfNodes == that.numberOfNodes &&
                numberOfDataNodes == that.numberOfDataNodes &&
                hasDiscoveredMaster == that.hasDiscoveredMaster &&
                activeShards == that.activeShards &&
                relocatingShards == that.relocatingShards &&
                activePrimaryShards == that.activePrimaryShards &&
                initializingShards == that.initializingShards &&
                unassignedShards == that.unassignedShards &&
                Double.compare(that.activeShardsPercent, activeShardsPercent) == 0 &&
                status == that.status &&
                Objects.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfNodes, numberOfDataNodes, hasDiscoveredMaster, activeShards, relocatingShards,
                activePrimaryShards, initializingShards, unassignedShards, activeShardsPercent, status, indices);
    }
}
