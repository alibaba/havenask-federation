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

package org.havenask.action.admin.indices.dangling.list;

import org.havenask.action.admin.indices.dangling.DanglingIndexInfo;
import org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesResponse.AggregatedDanglingIndexInfo;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.test.HavenaskTestCase;
import org.havenask.action.admin.indices.dangling.list.NodeListDanglingIndicesResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.havenask.action.admin.indices.dangling.list.ListDanglingIndicesResponse.resultsByIndexUUID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListDanglingIndicesResponseTests extends HavenaskTestCase {

    public static final String UUID_1 = UUID.randomUUID().toString();
    public static final String UUID_2 = UUID.randomUUID().toString();

    /**
     * Checks that {@link ListDanglingIndicesResponse#resultsByIndexUUID(List)} handles the
     * basic base of empty input.
     */
    public void testResultsByIndexUUIDWithEmptyListReturnsEmptyMap() {
        assertThat(resultsByIndexUUID(emptyList()), empty());
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate a single dangling index
     * on a single node.
     */
    public void testResultsByIndexUUIDCanAggregateASingleResponse() {
        final DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("some-node-id");

        final List<DanglingIndexInfo> danglingIndexInfo = singletonList(
            new DanglingIndexInfo("some-node-id", "some-index", UUID_1, 123456L)
        );
        final List<NodeListDanglingIndicesResponse> nodes = singletonList(new NodeListDanglingIndicesResponse(node, danglingIndexInfo));

        final List<AggregatedDanglingIndexInfo> aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(1));

        final AggregatedDanglingIndexInfo expected = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        expected.getNodeIds().add("some-node-id");
        assertThat(aggregated.get(0), equalTo(expected));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate a single dangling index
     * across multiple nodes.
     */
    public void testResultsByIndexUUIDCanAggregateAcrossMultipleNodes() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        final DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");
        when(node2.getId()).thenReturn("node-id-2");

        final List<DanglingIndexInfo> danglingIndexInfo1 = singletonList(new DanglingIndexInfo("node-id-1", "some-index", UUID_1, 123456L));
        final List<DanglingIndexInfo> danglingIndexInfo2 = singletonList(new DanglingIndexInfo("node-id-2", "some-index", UUID_1, 123456L));
        final List<NodeListDanglingIndicesResponse> nodes = asList(
            new NodeListDanglingIndicesResponse(node1, danglingIndexInfo1),
            new NodeListDanglingIndicesResponse(node2, danglingIndexInfo2)
        );

        final List<AggregatedDanglingIndexInfo> aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(1));

        final AggregatedDanglingIndexInfo expected = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        expected.getNodeIds().add("node-id-1");
        expected.getNodeIds().add("node-id-2");
        assertThat(aggregated.get(0), equalTo(expected));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate multiple dangling indices
     * on a single node.
     */
    public void testResultsByIndexUUIDCanAggregateMultipleIndicesOnOneNode() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");

        final List<DanglingIndexInfo> danglingIndexInfo = asList(
            new DanglingIndexInfo("node-id-1", "some-index", UUID_1, 123456L),
            new DanglingIndexInfo("node-id-1", "some-other-index", UUID_2, 7891011L)
        );

        final List<NodeListDanglingIndicesResponse> nodes = singletonList(new NodeListDanglingIndicesResponse(node1, danglingIndexInfo));

        final List<AggregatedDanglingIndexInfo> aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(2));

        AggregatedDanglingIndexInfo info1 = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        AggregatedDanglingIndexInfo info2 = new AggregatedDanglingIndexInfo(UUID_2, "some-other-index", 7891011L);
        info1.getNodeIds().add("node-id-1");
        info2.getNodeIds().add("node-id-1");

        assertThat(aggregated, containsInAnyOrder(info1, info2));
    }

    /**
     * Checks that <code>resultsByIndexUUID(List)</code> can aggregate multiple dangling indices
     * across multiple nodes.
     */
    public void testResultsByIndexUUIDCanAggregateMultipleIndicesAcrossMultipleNodes() {
        final DiscoveryNode node1 = mock(DiscoveryNode.class);
        final DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node-id-1");
        when(node2.getId()).thenReturn("node-id-2");

        final List<DanglingIndexInfo> danglingIndexInfo1 = singletonList(new DanglingIndexInfo("node-id-1", "some-index", UUID_1, 123456L));
        final List<DanglingIndexInfo> danglingIndexInfo2 = singletonList(
            new DanglingIndexInfo("node-id-2", "some-other-index", UUID_2, 7891011L)
        );
        final List<NodeListDanglingIndicesResponse> nodes = asList(
            new NodeListDanglingIndicesResponse(node1, danglingIndexInfo1),
            new NodeListDanglingIndicesResponse(node2, danglingIndexInfo2)
        );

        final List<AggregatedDanglingIndexInfo> aggregated = new ArrayList<>(resultsByIndexUUID(nodes));
        assertThat(aggregated, hasSize(2));

        AggregatedDanglingIndexInfo info1 = new AggregatedDanglingIndexInfo(UUID_1, "some-index", 123456L);
        AggregatedDanglingIndexInfo info2 = new AggregatedDanglingIndexInfo(UUID_2, "some-other-index", 7891011L);
        info1.getNodeIds().add("node-id-1");
        info2.getNodeIds().add("node-id-2");

        assertThat(aggregated, containsInAnyOrder(info1, info2));
    }
}
