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

package org.havenask.action.search;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.util.concurrent.AtomicArray;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchPhaseResult;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;

import static org.hamcrest.Matchers.equalTo;

public class TransportSearchHelperTests extends HavenaskTestCase {

    public static AtomicArray<SearchPhaseResult> generateQueryResults() {
        AtomicArray<SearchPhaseResult> array = new AtomicArray<>(3);
        DiscoveryNode node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult1 =
            new SearchAsyncActionTests.TestSearchPhaseResult(new ShardSearchContextId("a", 1), node1);
        testSearchPhaseResult1.setSearchShardTarget(new SearchShardTarget("node_1", new ShardId("idx", "uuid1", 2), "cluster_x", null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult2 =
            new SearchAsyncActionTests.TestSearchPhaseResult(new ShardSearchContextId("b", 12), node2);
        testSearchPhaseResult2.setSearchShardTarget(new SearchShardTarget("node_2", new ShardId("idy", "uuid2", 42), "cluster_y", null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult3 =
            new SearchAsyncActionTests.TestSearchPhaseResult(new ShardSearchContextId("c", 42), node3);
        testSearchPhaseResult3.setSearchShardTarget(new SearchShardTarget("node_3", new ShardId("idy", "uuid2", 43), null, null));
        array.setOnce(0, testSearchPhaseResult1);
        array.setOnce(1, testSearchPhaseResult2);
        array.setOnce(2, testSearchPhaseResult3);
        return array;
    }

    public void testParseScrollId()  {
        final Version version = VersionUtils.randomVersion(random());
        boolean includeUUID = version.onOrAfter(LegacyESVersion.V_7_7_0);
        final AtomicArray<SearchPhaseResult> queryResults = generateQueryResults();
        String scrollId = TransportSearchHelper.buildScrollId(queryResults, version);
        ParsedScrollId parseScrollId = TransportSearchHelper.parseScrollId(scrollId);
        assertEquals(3, parseScrollId.getContext().length);
        assertEquals("node_1", parseScrollId.getContext()[0].getNode());
        assertEquals("cluster_x", parseScrollId.getContext()[0].getClusterAlias());
        assertEquals(1, parseScrollId.getContext()[0].getSearchContextId().getId());
        if (includeUUID) {
            assertThat(parseScrollId.getContext()[0].getSearchContextId().getSessionId(), equalTo("a"));
        } else {
            assertThat(parseScrollId.getContext()[0].getSearchContextId().getSessionId(), equalTo(""));
        }

        assertEquals("node_2", parseScrollId.getContext()[1].getNode());
        assertEquals("cluster_y", parseScrollId.getContext()[1].getClusterAlias());
        assertEquals(12, parseScrollId.getContext()[1].getSearchContextId().getId());
        if (includeUUID) {
            assertThat(parseScrollId.getContext()[1].getSearchContextId().getSessionId(), equalTo("b"));
        } else {
            assertThat(parseScrollId.getContext()[1].getSearchContextId().getSessionId(), equalTo(""));
        }

        assertEquals("node_3", parseScrollId.getContext()[2].getNode());
        assertNull(parseScrollId.getContext()[2].getClusterAlias());
        assertEquals(42, parseScrollId.getContext()[2].getSearchContextId().getId());
        if (includeUUID) {
            assertThat(parseScrollId.getContext()[2].getSearchContextId().getSessionId(), equalTo("c"));
        } else {
            assertThat(parseScrollId.getContext()[2].getSearchContextId().getSessionId(), equalTo(""));
        }
    }
}
