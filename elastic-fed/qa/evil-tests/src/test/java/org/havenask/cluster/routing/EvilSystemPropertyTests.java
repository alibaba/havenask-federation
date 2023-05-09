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

package org.havenask.cluster.routing;

import org.havenask.common.SuppressForbidden;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;

import static org.hamcrest.Matchers.equalTo;

public class EvilSystemPropertyTests extends HavenaskTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testDisableSearchAllocationAwareness() {
        Settings indexSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "test")
            .build();
        OperationRouting routing = new OperationRouting(indexSettings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertWarnings(OperationRouting.IGNORE_AWARENESS_ATTRIBUTES_DEPRECATION_MESSAGE);
        assertThat(routing.getAwarenessAttributes().size(), equalTo(1));
        assertThat(routing.getAwarenessAttributes().get(0), equalTo("test"));
        System.setProperty("havenask.search.ignore_awareness_attributes", "true");
        try {
            routing = new OperationRouting(indexSettings,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            assertTrue(routing.getAwarenessAttributes().isEmpty());
        } finally {
            System.clearProperty("havenask.search.ignore_awareness_attributes");
        }

    }
}
