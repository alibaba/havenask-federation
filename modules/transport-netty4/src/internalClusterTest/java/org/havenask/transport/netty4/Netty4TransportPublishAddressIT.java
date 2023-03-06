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

package org.havenask.transport.netty4;

import org.havenask.HavenaskNetty4IntegTestCase;
import org.havenask.action.admin.cluster.node.info.NodeInfo;
import org.havenask.action.admin.cluster.node.info.NodesInfoResponse;
import org.havenask.common.network.NetworkModule;
import org.havenask.common.network.NetworkUtils;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.BoundTransportAddress;
import org.havenask.common.transport.TransportAddress;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.transport.Netty4Plugin;
import org.havenask.transport.TransportInfo;

import java.net.Inet4Address;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Checks that Havenask produces a sane publish_address when it binds to
 * different ports on ipv4 and ipv6.
 */
@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0)
public class Netty4TransportPublishAddressIT extends HavenaskNetty4IntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
            .build();
    }

    public void testDifferentPorts() throws Exception {
        if (!NetworkUtils.SUPPORTS_V6) {
            return;
        }
        logger.info("--> starting a node on ipv4 only");
        Settings ipv4Settings = Settings.builder().put("network.host", "127.0.0.1").build();
        String ipv4OnlyNode = internalCluster().startNode(ipv4Settings); // should bind 127.0.0.1:XYZ

        logger.info("--> starting a node on ipv4 and ipv6");
        Settings bothSettings = Settings.builder().put("network.host", "_local_").build();
        internalCluster().startNode(bothSettings); // should bind [::1]:XYZ and 127.0.0.1:XYZ+1

        logger.info("--> waiting for the cluster to declare itself stable");
        ensureStableCluster(2); // fails if port of publish address does not match corresponding bound address

        logger.info("--> checking if boundAddress matching publishAddress has same port");
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        for (NodeInfo nodeInfo : nodesInfoResponse.getNodes()) {
            BoundTransportAddress boundTransportAddress = nodeInfo.getInfo(TransportInfo.class).getAddress();
            if (nodeInfo.getNode().getName().equals(ipv4OnlyNode)) {
                assertThat(boundTransportAddress.boundAddresses().length, equalTo(1));
                assertThat(boundTransportAddress.boundAddresses()[0].getPort(), equalTo(boundTransportAddress.publishAddress().getPort()));
            } else {
                assertThat(boundTransportAddress.boundAddresses().length, greaterThan(1));
                for (TransportAddress boundAddress : boundTransportAddress.boundAddresses()) {
                    assertThat(boundAddress, instanceOf(TransportAddress.class));
                    TransportAddress inetBoundAddress = boundAddress;
                    if (inetBoundAddress.address().getAddress() instanceof Inet4Address) {
                        // IPv4 address is preferred publish address for _local_
                        assertThat(inetBoundAddress.getPort(), equalTo(boundTransportAddress.publishAddress().getPort()));
                    }
                }
            }
        }
    }

}
