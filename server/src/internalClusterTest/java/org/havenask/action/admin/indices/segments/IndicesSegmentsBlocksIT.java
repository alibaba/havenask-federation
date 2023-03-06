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

package org.havenask.action.admin.indices.segments;

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

@ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST)
public class IndicesSegmentsBlocksIT extends HavenaskIntegTestCase {
    public void testIndicesSegmentsWithBlocks() {
        createIndex("test-blocks");
        ensureGreen("test-blocks");

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test-blocks", "type", "" + i).setSource("test", "init").execute().actionGet();
        }
        client().admin().indices().prepareFlush("test-blocks").get();

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("test-blocks", blockSetting);
                IndicesSegmentResponse response = client().admin().indices().prepareSegments("test-blocks").execute().actionGet();
                assertNoFailures(response);
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareSegments("test-blocks"));
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
        }
    }
}
