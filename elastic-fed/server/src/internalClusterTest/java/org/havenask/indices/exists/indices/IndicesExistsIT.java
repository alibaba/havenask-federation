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

package org.havenask.indices.exists.indices;

import org.havenask.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.havenask.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Arrays;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class IndicesExistsIT extends HavenaskIntegTestCase {
    // Indices exists never throws IndexMissingException, the indices options control its behaviour (return true or false)
    public void testIndicesExists() throws Exception {
        assertFalse(client().admin().indices().prepareExists("foo").get().isExists());
        assertFalse(client().admin().indices().prepareExists("foo*").get().isExists());
        assertFalse(client().admin().indices().prepareExists("_all").get().isExists());

        createIndex("foo", "foobar", "bar", "barbaz");

        IndicesExistsRequestBuilder indicesExistsRequestBuilder = client().admin().indices().prepareExists("foo*")
                .setExpandWildcardsOpen(false);
        IndicesExistsRequest request = indicesExistsRequestBuilder.request();
        //check that ignore unavailable and allow no indices are set to false. That is their only valid value as it can't be overridden
        assertFalse(request.indicesOptions().ignoreUnavailable());
        assertFalse(request.indicesOptions().allowNoIndices());
        assertThat(indicesExistsRequestBuilder.get().isExists(), equalTo(false));

        assertAcked(client().admin().indices().prepareClose("foobar").get());

        assertThat(client().admin().indices().prepareExists("foo*").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("foo*").setExpandWildcardsOpen(false)
                .setExpandWildcardsClosed(false).get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("foobar").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("foob*").setExpandWildcardsClosed(false).get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("bar*").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("bar").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("_all").get().isExists(), equalTo(true));
    }

    public void testIndicesExistsWithBlocks() {
        createIndex("ro");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("ro", blockSetting);
                assertThat(client().admin().indices().prepareExists("ro").execute().actionGet().isExists(), equalTo(true));
            } finally {
                disableIndexBlock("ro", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
            assertThat(client().admin().indices().prepareExists("ro").execute().actionGet().isExists(), equalTo(true));
            fail("Exists should fail when " + IndexMetadata.SETTING_BLOCKS_METADATA + " is true");
        } catch (ClusterBlockException e) {
            // Ok, a ClusterBlockException is expected
        } finally {
            disableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
        }
    }
}
