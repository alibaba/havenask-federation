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
import org.havenask.action.admin.indices.mapping.put.PutMappingRequest;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Strings;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.index.IndexService;
import org.havenask.test.HavenaskSingleNodeTestCase;

import java.io.IOException;

public class LegacyMapperServiceTests extends HavenaskSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testIndexMetadataUpdateDoesNotLoseDefaultMapper() throws IOException {
        final IndexService indexService =
                createIndex("test", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, LegacyESVersion.V_6_3_0).build());
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject(MapperService.DEFAULT_MAPPING);
                {
                    builder.field("date_detection", false);
                }
                builder.endObject();
            }
            builder.endObject();
            final PutMappingRequest putMappingRequest = new PutMappingRequest();
            putMappingRequest.indices("test");
            putMappingRequest.type(MapperService.DEFAULT_MAPPING);
            putMappingRequest.source(builder);
            client().admin().indices().preparePutMapping("test").setType(MapperService.DEFAULT_MAPPING).setSource(builder).get();
        }
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
        final Settings zeroReplicasSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(zeroReplicasSettings).get();
        /*
         * This assertion is a guard against a previous bug that would lose the default mapper when applying a metadata update that did not
         * update the default mapping.
         */
        assertNotNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
    }

    public void testDefaultMappingIsDeprecatedOn6() throws IOException {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, LegacyESVersion.V_6_3_0).build();
        final String mapping;
        try (XContentBuilder defaultMapping = XContentFactory.jsonBuilder()) {
            defaultMapping.startObject();
            {
                defaultMapping.startObject("_default_");
                {

                }
                defaultMapping.endObject();
            }
            defaultMapping.endObject();
            mapping = Strings.toString(defaultMapping);
        }
        final MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.merge("_default_", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertWarnings(MapperService.DEFAULT_MAPPING_ERROR_MESSAGE);
    }

}
