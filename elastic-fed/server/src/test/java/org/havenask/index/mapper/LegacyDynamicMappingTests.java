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

package org.havenask.index.mapper;

import org.havenask.LegacyESVersion;
import org.havenask.action.admin.indices.mapping.get.GetMappingsResponse;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.test.HavenaskSingleNodeTestCase;

import java.io.IOException;

import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;

public class LegacyDynamicMappingTests extends HavenaskSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testTypeNotCreatedOnIndexFailure() throws IOException {
        final Settings settings =
            Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), LegacyESVersion.V_6_3_0).build();
        try (XContentBuilder mapping = jsonBuilder()) {
            mapping.startObject();
            {
                mapping.startObject("_default_");
                {
                    mapping.field("dynamic", "strict");
                }
                mapping.endObject();
            }
            mapping.endObject();
            createIndex("test", settings, "_default_", mapping);
        }
        try (XContentBuilder sourceBuilder = jsonBuilder().startObject().field("test", "test").endObject()) {
            expectThrows(StrictDynamicMappingException.class, () -> client()
                    .prepareIndex()
                    .setIndex("test")
                    .setType("type")
                    .setSource(sourceBuilder)
                    .get());

            GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
            assertNull(getMappingsResponse.getMappings().get("test").get("type"));
        }
    }

}
