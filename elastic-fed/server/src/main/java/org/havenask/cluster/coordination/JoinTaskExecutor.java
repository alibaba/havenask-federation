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

package org.havenask.cluster.coordination;

import org.apache.logging.log4j.Logger;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateTaskExecutor;
import org.havenask.cluster.NotMasterException;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RerouteService;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.common.Priority;
import org.havenask.common.settings.Settings;
import org.havenask.discovery.zen.ElectMasterService;
import org.havenask.persistent.PersistentTasksCustomMetadata;
import org.havenask.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.havenask.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class JoinTaskExecutor implements ClusterStateTaskExecutor<JoinTaskExecutor.Task> {

    private final AllocationService allocationService;

    private final Logger logger;
    private final RerouteService rerouteService;
    private final TransportService transportService;

    private final int minimumMasterNodesOnLocalNode;

    public static class Task {

        private final DiscoveryNode node;
        private final String reason;

        public Task(DiscoveryNode node, String reason) {
            this.node = node;
            this.reason = reason;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return node != null ? node + " " + reason : reason;
        }

        public boolean isBecomeMasterTask() {
            return reason.equals(BECOME_MASTER_TASK_REASON);
        }

        public boolean isFinishElectionTask() {
            return reason.equals(FINISH_ELECTION_TASK_REASON);
        }

        private static final String BECOME_MASTER_TASK_REASON = "_BECOME_MASTER_TASK_";
        private static final String FINISH_ELECTION_TASK_REASON = "_FINISH_ELECTION_";
    }

    public JoinTaskExecutor(Settings settings, AllocationService allocationService, Logger logger,
                            RerouteService rerouteService, TransportService transportService) {
        this.allocationService = allocationService;
        this.logger = logger;
        minimumMasterNodesOnLocalNode = ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(settings);
        this.rerouteService = rerouteService;
        this.transportService = transportService;
    }

    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> joiningNodes) throws Exception {
        final ClusterTasksResult.Builder<Task> results = ClusterTasksResult.builder();

        final DiscoveryNodes currentNodes = currentState.nodes();
        boolean nodesChanged = false;
        ClusterState.Builder newState;

        if (joiningNodes.size() == 1 && joiningNodes.get(0).isFinishElectionTask()) {
            return results.successes(joiningNodes).build(currentState);
        } else if (currentNodes.getMasterNode() == null && joiningNodes.stream().anyMatch(Task::isBecomeMasterTask)) {
            assert joiningNodes.stream().anyMatch(Task::isFinishElectionTask)
                : "becoming a master but election is not finished " + joiningNodes;
            // use these joins to try and become the master.
            // Note that we don't have to do any validation of the amount of joining nodes - the commit
            // during the cluster state publishing guarantees that we have enough
            newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
            nodesChanged = true;
        } else if (currentNodes.isLocalNodeElectedMaster() == false) {
            logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
            throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
        } else {
            newState = ClusterState.builder(currentState);
        }

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

        assert nodesBuilder.isLocalNodeElectedMaster();

        Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
        Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
        // we only enforce major version transitions on a fully formed clusters
        final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
        // processing any joins
        Map<String, String> joiniedNodeNameIds = new HashMap<>();
        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
            } else if (currentNodes.nodeExistsWithSameRoles(joinTask.node()) && !currentNodes.nodeExistsWithBWCVersion(joinTask.node())) {
                logger.debug("received a join request for an existing node [{}]", joinTask.node());
            } else {
                final DiscoveryNode node = joinTask.node();
                try {
                    if (enforceMajorVersion) {
                        ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                    }
                    ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                    // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                    // we have to reject nodes that don't support all indices we have in this cluster
                    ensureIndexCompatibility(node.getVersion(), currentState.getMetadata());
                    nodesBuilder.add(node);
                    nodesChanged = true;
                    minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                    maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                    if (node.isMasterNode()) {
                        joiniedNodeNameIds.put(node.getName(), node.getId());
                    }
                } catch (IllegalArgumentException | IllegalStateException e) {
                    results.failure(joinTask, e);
                    continue;
                }
            }
            results.success(joinTask);
        }

        if (nodesChanged) {
            rerouteService.reroute("post-join reroute", Priority.HIGH, ActionListener.wrap(
                r -> logger.trace("post-join reroute completed"),
                e -> logger.debug("post-join reroute failed", e)));

            if (joiniedNodeNameIds.isEmpty() == false) {
                Set<CoordinationMetadata.VotingConfigExclusion> currentVotingConfigExclusions = currentState.getVotingConfigExclusions();
                Set<CoordinationMetadata.VotingConfigExclusion> newVotingConfigExclusions = currentVotingConfigExclusions.stream()
                    .map(e -> {
                        // Update nodeId in VotingConfigExclusion when a new node with excluded node name joins
                        if (CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER.equals(e.getNodeId()) &&
                            joiniedNodeNameIds.containsKey(e.getNodeName())) {
                            return new CoordinationMetadata.VotingConfigExclusion(joiniedNodeNameIds.get(e.getNodeName()), e.getNodeName());
                        } else {
                            return e;
                        }
                    }).collect(Collectors.toSet());

                // if VotingConfigExclusions did get updated
                if (newVotingConfigExclusions.equals(currentVotingConfigExclusions) == false) {
                    CoordinationMetadata.Builder coordMetadataBuilder = CoordinationMetadata.builder(currentState.coordinationMetadata())
                        .clearVotingConfigExclusions();
                    newVotingConfigExclusions.forEach(coordMetadataBuilder::addVotingConfigExclusion);
                    Metadata newMetadata = Metadata.builder(currentState.metadata())
                                                    .coordinationMetadata(coordMetadataBuilder.build()).build();
                    return results.build(allocationService.adaptAutoExpandReplicas(newState.nodes(nodesBuilder)
                                                                                            .metadata(newMetadata).build()));
                }
            }

            return results.build(allocationService.adaptAutoExpandReplicas(newState.nodes(nodesBuilder).build()));
        } else {
            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize its join and set us as a master
            return results.build(newState.build());
        }
    }

    protected ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<Task> joiningNodes) {
        assert currentState.nodes().getMasterNodeId() == null : currentState;
        DiscoveryNodes currentNodes = currentState.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask()) {
                refreshDiscoveryNodeVersionAfterUpgrade(currentNodes, nodesBuilder);
            } else if (joinTask.isFinishElectionTask()) {
                //no-op
            } else {
                final DiscoveryNode joiningNode = joinTask.node();
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress,
                        joiningNode);
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                }
            }
        }


        // now trim any left over dead nodes - either left there when the previous master stepped down
        // or removed by us above
        ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(ClusterBlocks.builder()
            .blocks(currentState.blocks())
            .removeGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ID))
            .minimumMasterNodesOnPublishingMaster(minimumMasterNodesOnLocalNode)
            .build();
        logger.trace("becomeMasterAndTrimConflictingNodes: {}", tmpState.nodes());
        allocationService.cleanCaches();
        tmpState = PersistentTasksCustomMetadata.disassociateDeadNodes(tmpState);
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

    private void refreshDiscoveryNodeVersionAfterUpgrade(DiscoveryNodes currentNodes, DiscoveryNodes.Builder nodesBuilder) {
        // During the upgrade from Elasticsearch, Havenask node send their version as 7.10.2 to Elasticsearch master
        // in order to successfully join the cluster. But as soon as Havenask node becomes the master, cluster state
        // should show the Havenask nodes version as 1.x. As the cluster state was carry forwarded from ES master,
        // version in DiscoveryNode is stale 7.10.2. As soon as Havenask node becomes master, it can refresh the
        // DiscoveryNodes version and publish the updated state while finishing the election. This helps in atomically
        // updating the version of those node which have connection with the new master.
        // Note: This should get deprecated with BWC mode logic
        if(null == transportService) {
            // this logic is only applicable when Havenask node is master and is noop for zen discovery node
            return;
        }
        if(currentNodes.getMinNodeVersion().before(Version.V_1_0_0)) {
            Map<String, Version> channelVersions = transportService.getChannelVersion(currentNodes);
            for (DiscoveryNode node : currentNodes) {
                if(channelVersions.containsKey(node.getId())) {
                    if (channelVersions.get(node.getId()) != node.getVersion()) {
                        DiscoveryNode tmpNode = nodesBuilder.get(node.getId());
                        nodesBuilder.remove(node.getId());
                        nodesBuilder.add(new DiscoveryNode(tmpNode.getName(), tmpNode.getId(), tmpNode.getEphemeralId(),
                            tmpNode.getHostName(), tmpNode.getHostAddress(), tmpNode.getAddress(), tmpNode.getAttributes(),
                            tmpNode.getRoles(), channelVersions.get(tmpNode.getId())));
                        logger.info("Refreshed the DiscoveryNode version for node {}:{} from {} to {}",
                            node.getId(), node.getAddress(), node.getVersion(), channelVersions.get(tmpNode.getId()));
                    }
                } else {
                    // in case existing Havenask node is present in the cluster and but there is no connection to that node yet,
                    // either that node will send new JoinRequest to the master with version >=1.0, then no issue or
                    // there is an edge case if doesn't send JoinRequest and connection is established,
                    // then it can continue to report version as 7.10.2 instead of actual Havenask version. So,
                    // removing the node from cluster state to prevent stale version reporting and let it reconnect.
                    if (node.getVersion().equals(LegacyESVersion.V_7_10_2)) {
                        nodesBuilder.remove(node.getId());
                    }
                }
            }
        }
    }

    @Override
    public boolean runOnlyOnMaster() {
        // we validate that we are allowed to change the cluster state during cluster state processing
        return false;
    }

    public static Task newBecomeMasterTask() {
        return new Task(null, Task.BECOME_MASTER_TASK_REASON);
    }

    /**
     * a task that is used to signal the election is stopped and we should process pending joins.
     * it may be used in combination with {@link JoinTaskExecutor#newBecomeMasterTask()}
     */
    public static Task newFinishElectionTask() {
        return new Task(null, Task.FINISH_ELECTION_TASK_REASON);
    }

    /**
     * Ensures that all indices are compatible with the given node version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of havenask as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     * @see Version#minimumIndexCompatibilityVersion()
     * @throws IllegalStateException if any index is incompatible with the given version
     */
    public static void ensureIndexCompatibility(final Version nodeVersion, Metadata metadata) {
        Version supportedIndexVersion = nodeVersion.minimumIndexCompatibilityVersion();
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetadata idxMetadata : metadata) {
            if (idxMetadata.getCreationVersion().after(nodeVersion)) {
                throw new IllegalStateException("index " + idxMetadata.getIndex() + " version not supported: "
                    + idxMetadata.getCreationVersion() + " the node version is: " + nodeVersion);
            }
            if (idxMetadata.getCreationVersion().before(supportedIndexVersion)) {
                throw new IllegalStateException("index " + idxMetadata.getIndex() + " version not supported: "
                    + idxMetadata.getCreationVersion() + " minimum compatible index version is: " + supportedIndexVersion);
            }
        }
    }

    /** ensures that the joining node has a version that's compatible with all current nodes*/
    public static void ensureNodesCompatibility(final Version joiningNodeVersion, DiscoveryNodes currentNodes) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNodeVersion, minNodeVersion, maxNodeVersion);
    }

    /** ensures that the joining node has a version that's compatible with a given version range */
    public static void ensureNodesCompatibility(Version joiningNodeVersion, Version minClusterNodeVersion, Version maxClusterNodeVersion) {
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "The cluster contains nodes with version [" + maxClusterNodeVersion + "], which is incompatible.");
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported." +
                "The cluster contains nodes with version [" + minClusterNodeVersion + "], which is incompatible.");
        }
    }

    /**
     * ensures that the joining node's major version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the master is already fully operating under the new major version, it doesn't go back to mixed
     * version mode
     **/
    public static void ensureMajorVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        final byte jnMajor = joiningNodeVersion.major == 1 ? 7 : joiningNodeVersion.major;
        final byte clusterMajor = minClusterNodeVersion.major == 1? 7: minClusterNodeVersion.major;
        if (jnMajor < clusterMajor) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "All nodes in the cluster are of a higher major [" + clusterMajor + "].");
        }
    }

    public static Collection<BiConsumer<DiscoveryNode,ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            ensureNodesCompatibility(node.getVersion(), state.getNodes());
            ensureIndexCompatibility(node.getVersion(), state.getMetadata());
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }
}
