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

package org.havenask.client.transport;

import org.havenask.Version;
import org.havenask.client.Client;
import org.havenask.cluster.coordination.ClusterBootstrapService;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.network.NetworkModule;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.env.Environment;
import org.havenask.node.MockNode;
import org.havenask.node.Node;
import org.havenask.node.NodeValidationException;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import org.havenask.test.HavenaskIntegTestCase.Scope;
import org.havenask.test.MockHttpTransport;
import org.havenask.transport.MockTransportClient;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

import static org.havenask.test.NodeRoles.nonDataNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 1.0)
public class TransportClientIT extends HavenaskIntegTestCase {

    public void testPickingUpChangesInDiscoveryNode() {
        String nodeName = internalCluster().startNode(nonDataNode());

        TransportClient client = (TransportClient) internalCluster().client(nodeName);
        assertThat(client.connectedNodes().get(0).isDataNode(), equalTo(false));

    }

    public void testNodeVersionIsUpdated() throws IOException, NodeValidationException {
        TransportClient client = (TransportClient)  internalCluster().client();
        try (Node node = new MockNode(Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put("node.name", "testNodeVersionIsUpdated")
                .put("transport.type", getTestTransportType())
                .put(nonDataNode())
                .put("cluster.name", "foobar")
                .putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(), "testNodeVersionIsUpdated")
                .build(), Arrays.asList(getTestTransportPlugin(), MockHttpTransport.TestPlugin.class)).start()) {
            TransportAddress transportAddress = node.injector().getInstance(TransportService.class).boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);
            // since we force transport clients there has to be one node started that we connect to.
            assertThat(client.connectedNodes().size(), greaterThanOrEqualTo(1));
            // connected nodes have updated version
            for (DiscoveryNode discoveryNode : client.connectedNodes()) {
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT));
            }

            for (DiscoveryNode discoveryNode : client.listedNodes()) {
                assertThat(discoveryNode.getId(), startsWith("#transport#-"));
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }

            assertThat(client.filteredNodes().size(), equalTo(1));
            for (DiscoveryNode discoveryNode : client.filteredNodes()) {
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }
        }
    }

    public void testThatTransportClientSettingIsSet() {
        TransportClient client = (TransportClient)  internalCluster().client();
        Settings settings = client.injector.getInstance(Settings.class);
        assertThat(Client.CLIENT_TYPE_SETTING_S.get(settings), is("transport"));
    }

    public void testThatTransportClientSettingCannotBeChanged() {
        String transport = getTestTransportType();
        Settings baseSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), transport)
            .build();
        try (TransportClient client = new MockTransportClient(baseSettings)) {
            Settings settings = client.injector.getInstance(Settings.class);
            assertThat(Client.CLIENT_TYPE_SETTING_S.get(settings), is("transport"));
        }
    }
}
