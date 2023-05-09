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

package org.havenask.discovery;

import org.havenask.action.admin.cluster.node.info.NodesInfoRequest;
import org.havenask.action.admin.cluster.node.info.NodesInfoResponse;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.transport.TransportInfo;

import static org.havenask.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.havenask.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SettingsBasedSeedHostsProviderIT extends HavenaskIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));

        // super.nodeSettings enables file-based discovery, but here we disable it again so we can test the static list:
        if (randomBoolean()) {
            builder.putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey());
        } else {
            builder.remove(DISCOVERY_SEED_PROVIDERS_SETTING.getKey());
        }

        // super.nodeSettings sets this to an empty list, which disables any search for other nodes, but here we want this to happen:
        builder.remove(DISCOVERY_SEED_HOSTS_SETTING.getKey());

        return builder.build();
    }

    public void testClusterFormsWithSingleSeedHostInSettings() {
        final String seedNodeName = internalCluster().startNode();
        final NodesInfoResponse nodesInfoResponse
            = client(seedNodeName).admin().cluster().nodesInfo(new NodesInfoRequest("_local")).actionGet();
        final String seedNodeAddress =
            nodesInfoResponse.getNodes().get(0).getInfo(TransportInfo.class).getAddress().publishAddress().toString();
        logger.info("--> using seed node address {}", seedNodeAddress);

        int extraNodes = randomIntBetween(1, 5);
        internalCluster().startNodes(extraNodes,
            Settings.builder().putList(DISCOVERY_SEED_HOSTS_SETTING.getKey(), seedNodeAddress).build());

        ensureStableCluster(extraNodes + 1);
    }
}
