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

package org.havenask.cluster.metadata;

import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.test.HavenaskTestCase;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.IndexTemplateMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;

public class IndexTemplateMetadataTests extends HavenaskTestCase {

    public void testIndexTemplateMetadataXContentRoundTrip() throws Exception {
        ToXContent.Params params = new ToXContent.MapParams(singletonMap("reduce_mappings", "true"));

        String template = "{\"index_patterns\" : [ \".test-*\" ],\"order\" : 1000," +
            "\"settings\" : {\"number_of_shards\" : 1,\"number_of_replicas\" : 0}," +
            "\"mappings\" : {\"doc\" :" +
            "{\"properties\":{\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"text\"},\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"keyword\"}}" +
            "}}}";

        BytesReference templateBytes = new BytesArray(template);
        final IndexTemplateMetadata indexTemplateMetadata;
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, templateBytes, XContentType.JSON)) {
            indexTemplateMetadata = IndexTemplateMetadata.Builder.fromXContent(parser, "test");
        }

        final BytesReference templateBytesRoundTrip;
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.startObject();
            IndexTemplateMetadata.Builder.toXContentWithTypes(indexTemplateMetadata, builder, params);
            builder.endObject();
            templateBytesRoundTrip = BytesReference.bytes(builder);
        }

        final IndexTemplateMetadata indexTemplateMetadataRoundTrip;
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, templateBytesRoundTrip, XContentType.JSON)) {
            indexTemplateMetadataRoundTrip = IndexTemplateMetadata.Builder.fromXContent(parser, "test");
        }
        assertThat(indexTemplateMetadata, equalTo(indexTemplateMetadataRoundTrip));
    }

    public void testValidateInvalidIndexPatterns() throws Exception {
        final IllegalArgumentException emptyPatternError = expectThrows(IllegalArgumentException.class, () -> {
            new IndexTemplateMetadata(randomRealisticUnicodeOfLengthBetween(5, 10), randomInt(), randomInt(),
                Collections.emptyList(), Settings.EMPTY, ImmutableOpenMap.of(), ImmutableOpenMap.of());
        });
        assertThat(emptyPatternError.getMessage(), equalTo("Index patterns must not be null or empty; got []"));

        final IllegalArgumentException nullPatternError = expectThrows(IllegalArgumentException.class, () -> {
            new IndexTemplateMetadata(randomRealisticUnicodeOfLengthBetween(5, 10), randomInt(), randomInt(),
                null, Settings.EMPTY, ImmutableOpenMap.of(), ImmutableOpenMap.of());
        });
        assertThat(nullPatternError.getMessage(), equalTo("Index patterns must not be null or empty; got null"));

        final String templateWithEmptyPattern = "{\"index_patterns\" : [],\"order\" : 1000," +
            "\"settings\" : {\"number_of_shards\" : 10,\"number_of_replicas\" : 1}," +
            "\"mappings\" : {\"doc\" :" +
            "{\"properties\":{\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"text\"},\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"keyword\"}}" +
            "}}}";
        try (XContentParser parser =
                 XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                     DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new BytesArray(templateWithEmptyPattern), XContentType.JSON)) {
            final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> IndexTemplateMetadata.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100)));
            assertThat(ex.getMessage(), equalTo("Index patterns must not be null or empty; got []"));
        }

        final String templateWithoutPattern = "{\"order\" : 1000," +
            "\"settings\" : {\"number_of_shards\" : 10,\"number_of_replicas\" : 1}," +
            "\"mappings\" : {\"doc\" :" +
            "{\"properties\":{\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"text\"},\"" +
            randomAlphaOfLength(10) + "\":{\"type\":\"keyword\"}}" +
            "}}}";
        try (XContentParser parser =
                 XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                     DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new BytesArray(templateWithoutPattern), XContentType.JSON)) {
            final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> IndexTemplateMetadata.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100)));
            assertThat(ex.getMessage(), equalTo("Index patterns must not be null or empty; got null"));
        }
    }

    public void testParseTemplateWithAliases() throws Exception {
        String templateInJSON = "{\"aliases\": {\"log\":{}}, \"index_patterns\": [\"pattern-1\"]}";
        try (XContentParser parser =
                 XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                     DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new BytesArray(templateInJSON), XContentType.JSON)) {
            IndexTemplateMetadata template = IndexTemplateMetadata.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100));
            assertThat(template.aliases().containsKey("log"), equalTo(true));
            assertThat(template.patterns(), contains("pattern-1"));
        }
    }

    public void testFromToXContent() throws Exception {
        String templateName = randomUnicodeOfCodepointLengthBetween(1, 10);
        IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(templateName);
        templateBuilder.patterns(Arrays.asList("pattern-1"));
        int numAlias = between(0, 5);
        for (int i = 0; i < numAlias; i++) {
            AliasMetadata.Builder alias = AliasMetadata.builder(randomRealisticUnicodeOfLengthBetween(1, 100));
            if (randomBoolean()) {
                alias.indexRouting(randomRealisticUnicodeOfLengthBetween(1, 100));
            }
            if (randomBoolean()) {
                alias.searchRouting(randomRealisticUnicodeOfLengthBetween(1, 100));
            }
            templateBuilder.putAlias(alias);
        }
        if (randomBoolean()) {
            templateBuilder.settings(Settings.builder().put("index.setting-1", randomLong()));
            templateBuilder.settings(Settings.builder().put("index.setting-2", randomTimeValue()));
        }
        if (randomBoolean()) {
            templateBuilder.order(randomInt());
        }
        if (randomBoolean()) {
            templateBuilder.version(between(0, 100));
        }
        if (randomBoolean()) {
            templateBuilder.putMapping("doc", "{\"doc\":{\"properties\":{\"type\":\"text\"}}}");
        }
        IndexTemplateMetadata template = templateBuilder.build();
        XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.JSON.xContent()));
        builder.startObject();
        IndexTemplateMetadata.Builder.toXContentWithTypes(template, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(shuffleXContent(builder))) {
            IndexTemplateMetadata parsed = IndexTemplateMetadata.Builder.fromXContent(parser, templateName);
            assertThat(parsed, equalTo(template));
        }
    }

    public void testDeprecationWarningsOnMultipleMappings() throws IOException {
        IndexTemplateMetadata.Builder builder = IndexTemplateMetadata.builder("my-template");
        builder.patterns(Arrays.asList("a", "b"));
        builder.putMapping("type1", "{\"type1\":{}}");
        builder.putMapping("type2", "{\"type2\":{}}");
        builder.build();

        assertWarnings("Index template my-template contains multiple typed mappings; " +
            "templates in 8x will only support a single mapping");
    }
}
