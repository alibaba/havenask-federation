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

package org.havenask.action.admin.cluster.configuration;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.support.master.MasterNodeRequest;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A request to add voting config exclusions for certain master-eligible nodes, and wait for these nodes to be removed from the voting
 * configuration.
 */
public class AddVotingConfigExclusionsRequest extends MasterNodeRequest<AddVotingConfigExclusionsRequest> {
    public static final String DEPRECATION_MESSAGE = "nodeDescription is deprecated and will be removed, use nodeIds or nodeNames instead";
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(AddVotingConfigExclusionsRequest.class);
    private final String[] nodeDescriptions;
    private final String[] nodeIds;
    private final String[] nodeNames;
    private final TimeValue timeout;

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given node names, and wait for a
     * default 30 seconds for these exclusions to take effect, removing the nodes from the voting configuration.
     * @param nodeNames Names of the nodes to add - see {@link AddVotingConfigExclusionsRequest#resolveVotingConfigExclusions(ClusterState)}
     */
    public AddVotingConfigExclusionsRequest(String... nodeNames) {
        this(Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY, nodeNames, TimeValue.timeValueSeconds(30));
    }

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given descriptions, and wait for these
     * nodes to be removed from the voting configuration.
     * @param nodeDescriptions Descriptions of the nodes whose exclusions to add - see {@link DiscoveryNodes#resolveNodes(String...)}.
     * @param nodeIds Ids of the nodes whose exclusions to add - see
     *                  {@link AddVotingConfigExclusionsRequest#resolveVotingConfigExclusions(ClusterState)}.
     * @param nodeNames Names of the nodes whose exclusions to add - see
     *                  {@link AddVotingConfigExclusionsRequest#resolveVotingConfigExclusions(ClusterState)}.
     * @param timeout How long to wait for the added exclusions to take effect and be removed from the voting configuration.
     */
    public AddVotingConfigExclusionsRequest(String[] nodeDescriptions, String[] nodeIds, String[] nodeNames, TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }

        if (noneOrMoreThanOneIsSet(nodeDescriptions, nodeIds, nodeNames)) {
            throw new IllegalArgumentException("Please set node identifiers correctly. " +
                "One and only one of [node_name], [node_names] and [node_ids] has to be set");
        }

        if (nodeDescriptions.length > 0) {
            deprecationLogger.deprecate("voting_config_exclusion", DEPRECATION_MESSAGE);
        }

        this.nodeDescriptions = nodeDescriptions;
        this.nodeIds = nodeIds;
        this.nodeNames = nodeNames;
        this.timeout = timeout;
    }

    public AddVotingConfigExclusionsRequest(StreamInput in) throws IOException {
        super(in);
        nodeDescriptions = in.readStringArray();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            nodeIds = in.readStringArray();
            nodeNames = in.readStringArray();
        } else {
            nodeIds = Strings.EMPTY_ARRAY;
            nodeNames = Strings.EMPTY_ARRAY;
        }
        timeout = in.readTimeValue();

        if (nodeDescriptions.length > 0) {
            deprecationLogger.deprecate("voting_config_exclusion",
                "nodeDescription is deprecated and will be removed, use nodeIds or nodeNames instead");
        }

    }

    Set<VotingConfigExclusion> resolveVotingConfigExclusions(ClusterState currentState) {
        final DiscoveryNodes allNodes = currentState.nodes();
        Set<VotingConfigExclusion> newVotingConfigExclusions = new HashSet<>();

        if (nodeDescriptions.length >= 1) {
            newVotingConfigExclusions = Arrays.stream(allNodes.resolveNodes(nodeDescriptions)).map(allNodes::get)
                .filter(DiscoveryNode::isMasterNode).map(VotingConfigExclusion::new).collect(Collectors.toSet());

            if (newVotingConfigExclusions.isEmpty()) {
                throw new IllegalArgumentException("add voting config exclusions request for " + Arrays.asList(nodeDescriptions)
                    + " matched no master-eligible nodes");
            }
        } else if (nodeIds.length >= 1) {
            for (String nodeId : nodeIds) {
                if (allNodes.nodeExists(nodeId)) {
                    DiscoveryNode discoveryNode = allNodes.get(nodeId);
                    if (discoveryNode.isMasterNode()) {
                        newVotingConfigExclusions.add(new VotingConfigExclusion(discoveryNode));
                    }
                } else {
                    newVotingConfigExclusions.add(new VotingConfigExclusion(nodeId, VotingConfigExclusion.MISSING_VALUE_MARKER));
                }
            }
        } else {
            assert nodeNames.length >= 1;
            Map<String, DiscoveryNode> existingNodes = StreamSupport.stream(allNodes.spliterator(), false)
                                                                .collect(Collectors.toMap(DiscoveryNode::getName, Function.identity()));

            for (String nodeName : nodeNames) {
                if (existingNodes.containsKey(nodeName)){
                    DiscoveryNode discoveryNode = existingNodes.get(nodeName);
                    if (discoveryNode.isMasterNode()) {
                        newVotingConfigExclusions.add(new VotingConfigExclusion(discoveryNode));
                    }
                } else {
                    newVotingConfigExclusions.add(new VotingConfigExclusion(VotingConfigExclusion.MISSING_VALUE_MARKER, nodeName));
                }
            }
        }

        newVotingConfigExclusions.removeIf(n -> currentState.getVotingConfigExclusions().contains(n));
        return newVotingConfigExclusions;
    }

    Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(ClusterState currentState, int maxExclusionsCount,
                                                                            String maximumSettingKey) {
        final Set<VotingConfigExclusion> resolvedExclusions = resolveVotingConfigExclusions(currentState);

        final int oldExclusionsCount = currentState.getVotingConfigExclusions().size();
        final int newExclusionsCount = resolvedExclusions.size();
        if (oldExclusionsCount + newExclusionsCount > maxExclusionsCount) {
            throw new IllegalArgumentException("add voting config exclusions request for " + Arrays.asList(nodeDescriptions)
                + " would add [" + newExclusionsCount + "] exclusions to the existing [" + oldExclusionsCount
                + "] which would exceed the maximum of [" + maxExclusionsCount + "] set by ["
                + maximumSettingKey + "]");
        }
        return resolvedExclusions;
    }

    private boolean noneOrMoreThanOneIsSet(String[] deprecatedNodeDescription, String[] nodeIds, String[] nodeNames) {
        if (deprecatedNodeDescription.length > 0) {
            return nodeIds.length > 0 || nodeNames.length > 0;
        } else if (nodeIds.length > 0) {
            return nodeNames.length > 0;
        } else {
            return nodeNames.length > 0 == false;
        }
    }

    /**
     * @return descriptions of the nodes for whom to add voting config exclusions.
     */
    public String[] getNodeDescriptions() {
        return nodeDescriptions;
    }

    /**
     * @return ids of the nodes for whom to add voting config exclusions.
     */
    public String[] getNodeIds() {
        return nodeIds;
    }

    /**
     * @return names of the nodes for whom to add voting config exclusions.
     */
    public String[] getNodeNames() {
        return nodeNames;
    }

    /**
     * @return how long to wait after adding the exclusions for the nodes to be removed from the voting configuration.
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(nodeDescriptions);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            out.writeStringArray(nodeIds);
            out.writeStringArray(nodeNames);
        }
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "AddVotingConfigExclusionsRequest{" +
            "nodeDescriptions=" + Arrays.asList(nodeDescriptions) + ", " +
            "nodeIds=" + Arrays.asList(nodeIds) + ", " +
            "nodeNames=" + Arrays.asList(nodeNames) + ", " +
            "timeout=" + timeout +
            '}';
    }
}
