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

package org.havenask.action.admin.indices.exists;

import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.discovery.MasterNotDiscoveredException;
import org.havenask.gateway.GatewayService;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import org.havenask.test.InternalTestCluster;

import java.io.IOException;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertRequestBuilderThrows;

@ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0,
    autoManageMasterNodes = false)
public class IndicesExistsIT extends HavenaskIntegTestCase {

    public void testIndexExistsWithBlocksInPlace() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        Settings settings = Settings.builder()
            .put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 99).build();
        String node = internalCluster().startNode(settings);

        assertRequestBuilderThrows(
            client(node).admin().indices().prepareExists("test").setMasterNodeTimeout(TimeValue.timeValueSeconds(0)),
            MasterNotDiscoveredException.class);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node)); // shut down node so that test properly cleans up
    }
}
