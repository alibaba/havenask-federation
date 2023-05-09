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

package org.havenask.cluster.metadata;

import org.havenask.common.SuppressForbidden;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;

import static org.havenask.cluster.metadata.IndexMetadata.DEFAULT_NUMBER_OF_SHARDS;
import static org.havenask.cluster.metadata.IndexMetadata.MAX_NUMBER_OF_SHARDS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class EvilSystemPropertyTests extends HavenaskTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testNumShards() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            IndexMetadata.buildNumberOfShardsSetting()
                .get(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1025).build()));
        assertEquals("Failed to parse value [1025] for setting [" + SETTING_NUMBER_OF_SHARDS + "] must be <= 1024", exception.getMessage());

        Integer numShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 100).build());
        assertEquals(100, numShards.intValue());
        int limit = randomIntBetween(1, 10);
        System.setProperty(MAX_NUMBER_OF_SHARDS, Integer.toString(limit));
        try {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
                IndexMetadata.buildNumberOfShardsSetting()
                    .get(Settings.builder().put("index.number_of_shards", 11).build()));
            assertEquals("Failed to parse value [11] for setting [index.number_of_shards] must be <= " + limit, e.getMessage());
            System.clearProperty(MAX_NUMBER_OF_SHARDS);

            Integer defaultFromSetting = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getDefault(Settings.EMPTY);
            assertEquals(1, defaultFromSetting.intValue());

            int randomDefault = randomIntBetween(1, 10);
            System.setProperty(DEFAULT_NUMBER_OF_SHARDS, Integer.toString(randomDefault));
            defaultFromSetting = IndexMetadata.buildNumberOfShardsSetting().getDefault(Settings.EMPTY);
            assertEquals(randomDefault, defaultFromSetting.intValue());

            randomDefault = randomIntBetween(1, 10);
            System.setProperty(MAX_NUMBER_OF_SHARDS, Integer.toString(randomDefault));
            System.setProperty(DEFAULT_NUMBER_OF_SHARDS, Integer.toString(randomDefault + 1));
            e = expectThrows(IllegalArgumentException.class, IndexMetadata::buildNumberOfShardsSetting);
            assertEquals(DEFAULT_NUMBER_OF_SHARDS + " value [" + (randomDefault + 1) + "] must between " +
                "1 and " + MAX_NUMBER_OF_SHARDS + " [" + randomDefault + "]", e.getMessage());
        } finally {
            System.clearProperty(MAX_NUMBER_OF_SHARDS);
            System.clearProperty(DEFAULT_NUMBER_OF_SHARDS);
        }
    }
}
