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

package org.havenask.action.admin.indices.template.get;

import org.havenask.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.IndexTemplateMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.test.AbstractXContentTestCase;
import org.havenask.action.admin.indices.template.get.GetIndexTemplatesResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.havenask.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.hamcrest.Matchers.equalTo;

public class GetIndexTemplatesResponseTests extends AbstractXContentTestCase<GetIndexTemplatesResponse> {
    @Override
    protected GetIndexTemplatesResponse doParseInstance(XContentParser parser) throws IOException {
        return GetIndexTemplatesResponse.fromXContent(parser);
    }

    @Override
    protected GetIndexTemplatesResponse createTestInstance() {
        List<IndexTemplateMetadata> templates = new ArrayList<>();
        int numTemplates = between(0, 10);
        for (int t = 0; t < numTemplates; t++) {
            IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder("template-" + t);
            templateBuilder.patterns(IntStream.range(0, between(1, 5)).mapToObj(i -> "pattern-" + i).collect(Collectors.toList()));
            int numAlias = between(0, 5);
            for (int i = 0; i < numAlias; i++) {
                templateBuilder.putAlias(AliasMetadata.builder(randomAlphaOfLengthBetween(1, 10)));
            }
            if (randomBoolean()) {
                templateBuilder.settings(Settings.builder().put("index.setting-1", randomLong()));
            }
            if (randomBoolean()) {
                templateBuilder.order(randomInt());
            }
            if (randomBoolean()) {
                templateBuilder.version(between(0, 100));
            }
            if (randomBoolean()) {
                try {
                    templateBuilder.putMapping("doc", "{\"doc\":{\"properties\":{\"type\":\"text\"}}}");
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
            templates.add(templateBuilder.build());
        }
        return new GetIndexTemplatesResponse(templates);
    }

    @Override
    protected boolean supportsUnknownFields() {
        // We can not inject anything at the top level because a GetIndexTemplatesResponse is serialized as a map
        // from template name to template content. IndexTemplateMetadataTests already covers situations where we
        // inject arbitrary things inside the IndexTemplateMetadata.
        return false;
    }

    /**
     * For now, we only unit test the legacy typed responses. This will soon no longer be the case,
     * as we introduce support for typeless xContent parsing in {@link GetFieldMappingsResponse}.
     */
    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(INCLUDE_TYPE_NAME_PARAMETER, "true"));
    }

    @Override
    protected void assertEqualInstances(GetIndexTemplatesResponse expectedInstance, GetIndexTemplatesResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertThat(new HashSet<>(newInstance.getIndexTemplates()), equalTo(new HashSet<>(expectedInstance.getIndexTemplates())));
    }
}
