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

package org.havenask.action.admin.indices.forcemerge;

import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;

import java.util.Arrays;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertBlocked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST)
public class ForceMergeBlocksIT extends HavenaskIntegTestCase {

    public void testForceMergeWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        NumShards numShards = getNumShards("test");

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test", "type", "" + i).setSource("test", "init").execute().actionGet();
        }

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);
                ForceMergeResponse response = client().admin().indices().prepareForceMerge("test").execute().actionGet();
                assertNoFailures(response);
                assertThat(response.getSuccessfulShards(), equalTo(numShards.totalNumShards));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Request is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA, SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareForceMerge("test"));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Merging all indices is blocked when the cluster is read-only
        try {
            ForceMergeResponse response = client().admin().indices().prepareForceMerge().execute().actionGet();
            assertNoFailures(response);
            assertThat(response.getSuccessfulShards(), equalTo(numShards.totalNumShards));

            setClusterReadOnly(true);
            assertBlocked(client().admin().indices().prepareForceMerge());
        } finally {
            setClusterReadOnly(false);
        }
    }
}
