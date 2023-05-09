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

import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;

import java.util.Collections;
import java.util.function.Predicate;

import static org.havenask.test.NodeRoles.onlyRole;
import static org.havenask.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class DiscoveryNodeRoleSettingTests extends HavenaskTestCase {

    public void testIsDataNode() {
        runRoleTest(DiscoveryNode::isDataNode, DiscoveryNodeRole.DATA_ROLE);
    }

    public void testIsIngestNode() {
        runRoleTest(DiscoveryNode::isIngestNode, DiscoveryNodeRole.INGEST_ROLE);
    }

    public void testIsMasterNode() {
        runRoleTest(DiscoveryNode::isMasterNode, DiscoveryNodeRole.MASTER_ROLE);
    }

    public void testIsRemoteClusterClient() {
        runRoleTest(DiscoveryNode::isRemoteClusterClient, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
    }

    private void runRoleTest(final Predicate<Settings> predicate, final DiscoveryNodeRole role) {
        final Settings legacyTrue = Settings.builder().put(role.legacySetting().getKey(), true).build();

        assertTrue(predicate.test(legacyTrue));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        assertThat(DiscoveryNode.getRolesFromSettings(legacyTrue), hasItem(role));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        final Settings legacyFalse = Settings.builder().put(role.legacySetting().getKey(), false).build();

        assertFalse(predicate.test(legacyFalse));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        assertThat(DiscoveryNode.getRolesFromSettings(legacyFalse), not(hasItem(role)));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});

        assertTrue(predicate.test(onlyRole(role)));
        assertThat(DiscoveryNode.getRolesFromSettings(onlyRole(role)), hasItem(role));

        assertFalse(predicate.test(removeRoles(Collections.singleton(role))));
        assertThat(DiscoveryNode.getRolesFromSettings(removeRoles(Collections.singleton(role))), not(hasItem(role)));

        final Settings settings = Settings.builder().put(onlyRole(role)).put(role.legacySetting().getKey(), randomBoolean()).build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DiscoveryNode.getRolesFromSettings(settings));
        assertThat(e.getMessage(), startsWith("can not explicitly configure node roles and use legacy role setting"));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{role.legacySetting()});
    }

}
