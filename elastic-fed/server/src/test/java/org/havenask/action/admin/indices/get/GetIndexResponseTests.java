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

package org.havenask.action.admin.indices.get;

import org.apache.lucene.util.CollectionUtil;
import org.havenask.action.admin.indices.alias.get.GetAliasesResponseTests;
import org.havenask.action.admin.indices.mapping.get.GetMappingsResponseTests;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.RandomCreateIndexGenerator;
import org.havenask.rest.BaseRestHandler;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.action.admin.indices.get.GetIndexResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

public class GetIndexResponseTests extends AbstractSerializingTestCase<GetIndexResponse> {

    @Override
    protected GetIndexResponse doParseInstance(XContentParser parser) throws IOException {
        return GetIndexResponse.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<GetIndexResponse> instanceReader() {
        return GetIndexResponse::new;
    }

    @Override
    protected GetIndexResponse createTestInstance() {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, String> dataStreams = ImmutableOpenMap.builder();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index: indices) {
            // rarely have no types
            int typeCount = rarely() ? 0 : 1;
            mappings.put(index, GetMappingsResponseTests.createMappingsForIndex(typeCount, true));

            List<AliasMetadata> aliasMetadataList = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int i=0; i<aliasesNum; i++) {
                aliasMetadataList.add(GetAliasesResponseTests.createAliasMetadata());
            }
            CollectionUtil.timSort(aliasMetadataList, Comparator.comparing(AliasMetadata::alias));
            aliases.put(index, Collections.unmodifiableList(aliasMetadataList));

            Settings.Builder builder = Settings.builder();
            builder.put(RandomCreateIndexGenerator.randomIndexSettings());
            settings.put(index, builder.build());

            if (includeDefaults) {
                defaultSettings.put(index, indexScopedSettings.diff(settings.get(index), Settings.EMPTY));
            }

            if (randomBoolean()) {
                dataStreams.put(index, randomAlphaOfLength(5).toLowerCase(Locale.ROOT));
            }
        }
        return new GetIndexResponse(
            indices, mappings.build(), aliases.build(), settings.build(), defaultSettings.build(), dataStreams.build()
        );
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        //we do not want to add new fields at the root (index-level), or inside the blocks
        return
            f -> f.equals("") || f.contains(".settings") || f.contains(".defaults") || f.contains(".mappings") ||
            f.contains(".aliases");
    }

    /**
     * For xContent roundtrip testing we force the xContent output to still contain types because the parser still expects them.
     * The new typeless parsing is implemented in the client side GetIndexResponse.
     */
    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "true"));
    }
}
