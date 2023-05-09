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

package org.havenask.cluster.node;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.test.HavenaskTestCase;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.havenask.test.NodeRoles.nonRemoteClusterClientNode;
import static org.havenask.test.NodeRoles.remoteClusterClientNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class DiscoveryNodeTests extends HavenaskTestCase {

    public void testRolesAreSorted() {
        final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        final DiscoveryNode node = new DiscoveryNode(
            "name",
            "id",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            emptyMap(),
            roles,
            Version.CURRENT
        );
        DiscoveryNodeRole previous = null;
        for (final DiscoveryNodeRole current : node.getRoles()) {
            if (previous != null) {
                assertThat(current, greaterThanOrEqualTo(previous));
            }
            previous = current;
        }

    }

    public void testDiscoveryNodeIsCreatedWithHostFromInetAddress() throws Exception {
        InetAddress inetAddress = randomBoolean() ? InetAddress.getByName("192.0.2.1") :
            InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);
        assertEquals(transportAddress.address().getHostString(), node.getHostName());
        assertEquals(transportAddress.getAddress(), node.getHostAddress());
    }

    public void testDiscoveryNodeSerializationKeepsHost() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.CURRENT);
        node.writeTo(streamOutput);

        StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
        DiscoveryNode serialized = new DiscoveryNode(in);
        assertEquals(transportAddress.address().getHostString(), serialized.getHostName());
        assertEquals(transportAddress.address().getHostString(), serialized.getAddress().address().getHostString());
        assertEquals(transportAddress.getAddress(), serialized.getHostAddress());
        assertEquals(transportAddress.getAddress(), serialized.getAddress().getAddress());
        assertEquals(transportAddress.getPort(), serialized.getAddress().getPort());
    }

    public void testDiscoveryNodeRoleWithOldVersion() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));

        DiscoveryNodeRole customRole = new DiscoveryNodeRole("custom_role", "z", true) {
            @Override
            public Setting<Boolean> legacySetting() {
                return null;
            }

            @Override
            public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
                if (nodeVersion.equals(Version.CURRENT)) {
                    return this;
                } else {
                    return DiscoveryNodeRole.DATA_ROLE;
                }
            }
        };

        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(),
            Collections.singleton(customRole), Version.CURRENT);

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setVersion(Version.CURRENT);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setVersion(Version.CURRENT);
            DiscoveryNode serialized = new DiscoveryNode(in);
            assertThat(serialized.getRoles().stream().map(DiscoveryNodeRole::roleName).collect(Collectors.joining()),
                equalTo("custom_role"));
        }

        {
            BytesStreamOutput streamOutput = new BytesStreamOutput();
            streamOutput.setVersion(LegacyESVersion.V_7_9_0);
            node.writeTo(streamOutput);

            StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
            in.setVersion(LegacyESVersion.V_7_9_0);
            DiscoveryNode serialized = new DiscoveryNode(in);
            assertThat(serialized.getRoles().stream().map(DiscoveryNodeRole::roleName).collect(Collectors.joining()),
                equalTo("data"));
        }

    }

    public void testDiscoveryNodeIsRemoteClusterClientDefault() {
        runTestDiscoveryNodeIsRemoteClusterClient(Settings.EMPTY, true);
    }

    public void testDiscoveryNodeIsRemoteClusterClientSet() {
        runTestDiscoveryNodeIsRemoteClusterClient(remoteClusterClientNode(), true);
    }

    public void testDiscoveryNodeIsRemoteClusterClientUnset() {
        runTestDiscoveryNodeIsRemoteClusterClient(nonRemoteClusterClientNode(), false);
    }

    private void runTestDiscoveryNodeIsRemoteClusterClient(final Settings settings, final boolean expected) {
        final DiscoveryNode node = DiscoveryNode.createLocal(settings, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node");
        assertThat(node.isRemoteClusterClient(), equalTo(expected));
        if (expected) {
            assertThat(node.getRoles(), hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
        } else {
            assertThat(node.getRoles(), not(hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)));
        }
    }

}
