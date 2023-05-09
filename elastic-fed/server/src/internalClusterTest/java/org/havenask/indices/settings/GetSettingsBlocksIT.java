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

package org.havenask.indices.settings;

import org.havenask.action.admin.indices.settings.get.GetSettingsResponse;
import org.havenask.common.settings.Settings;
import org.havenask.index.mapper.FieldMapper;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Arrays;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class GetSettingsBlocksIT extends HavenaskIntegTestCase {
    public void testGetSettingsWithBlocks() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put("index.refresh_interval", -1)
                        .put("index.merge.policy.expunge_deletes_allowed", "30")
                        .put(FieldMapper.IGNORE_MALFORMED_SETTING.getKey(), false)));

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("test", block);
                GetSettingsResponse response = client().admin().indices().prepareGetSettings("test").get();
                assertThat(response.getIndexToSettings().size(), greaterThanOrEqualTo(1));
                assertThat(response.getSetting("test", "index.refresh_interval"), equalTo("-1"));
                assertThat(response.getSetting("test", "index.merge.policy.expunge_deletes_allowed"), equalTo("30"));
                assertThat(response.getSetting("test", FieldMapper.IGNORE_MALFORMED_SETTING.getKey()), equalTo("false"));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetSettings("test"));
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
