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
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Strings;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.index.mapper.MapperService.MergeReason;
import org.havenask.test.HavenaskSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.havenask.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.containsString;

public class RootObjectMapperTests extends HavenaskSingleNodeTestCase {

    public void testNumericDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .field("numeric_detection", false)
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("numeric_detection", true)
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .field("date_detection", true)
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("date_detection", false)
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateFormatters() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .field("dynamic_date_formats", Arrays.asList("yyyy-MM-dd"))
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if formatters are not set explicitly
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("dynamic_date_formats", Arrays.asList())
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicTemplates() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .startArray("dynamic_templates")
                            .startObject()
                                .startObject("my_template")
                                    .field("match_mapping_type", "string")
                                    .startObject("mapping")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endArray()
                    .endObject()
                .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .field("dynamic_templates", Arrays.asList())
                .endObject()
            .endObject());
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicTemplatesForIndexTemplate() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startArray("dynamic_templates")
                .startObject()
                    .startObject("first_template")
                        .field("path_match", "first")
                        .startObject("mapping")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject()
                    .startObject("second_template")
                        .field("path_match", "second")
                        .startObject("mapping")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endArray()
        .endObject());
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        // There should be no update if templates are not set.
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field")
                    .field("type", "integer")
                .endObject()
            .endObject()
        .endObject());
        DocumentMapper mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        DynamicTemplate[] templates = mapper.root().dynamicTemplates();
        assertEquals(2, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second", templates[1].pathMatch());

        // Dynamic templates should be appended and deduplicated.
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startArray("dynamic_templates")
                .startObject()
                    .startObject("third_template")
                        .field("path_match", "third")
                        .startObject("mapping")
                            .field("type", "integer")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject()
                    .startObject("second_template")
                        .field("path_match", "second_updated")
                        .startObject("mapping")
                            .field("type", "double")
                        .endObject()
                    .endObject()
                .endObject()
            .endArray()
        .endObject());
        mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        templates = mapper.root().dynamicTemplates();
        assertEquals(3, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second_updated", templates[1].pathMatch());
        assertEquals("third_template", templates[2].name());
        assertEquals("third", templates[2].pathMatch());
    }

    public void testIllegalFormatField() throws Exception {
        String dynamicMapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startArray("dynamic_date_formats")
                        .startArray().value("test_format").endArray()
                    .endArray()
                .endObject()
            .endObject());
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startArray("date_formats")
                        .startArray().value("test_format").endArray()
                    .endArray()
                .endObject()
            .endObject());

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        for (String m : Arrays.asList(mapping, dynamicMapping)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> parser.parse("type", new CompressedXContent(m)));
            assertEquals("Invalid format: [[test_format]]: expected string value", e.getMessage());
        }
    }

    public void testIllegalDynamicTemplates() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                    .startObject("dynamic_templates")
                    .endObject()
                .endObject()
            .endObject());

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Dynamic template syntax error. An array of named objects is expected.", e.getMessage());
    }

    public void testIllegalDynamicTemplateUnknownFieldType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"type\":\"string\""));
        assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":" +
            "\"string\"}}], caused by [No mapper found for type [string]]");
    }

    public void testIllegalDynamicTemplateUnknownAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "keyword");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"foo\":\"bar\""));
        assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{" +
            "\"foo\":\"bar\",\"type\":\"keyword\"}}], " +
            "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [keyword]]");
    }

    public void testIllegalDynamicTemplateInvalidAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "text");
                mapping.field("analyzer", "foobar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"analyzer\":\"foobar\""));
        assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{" +
            "\"analyzer\":\"foobar\",\"type\":\"text\"}}], caused by [analyzer [foobar] has not been configured in mappings]");
    }

    public void testIllegalDynamicTemplateNoMappingType() throws Exception {
        MapperService mapperService;

        {
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject("type");
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template");
                    if (randomBoolean()) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("index_phrases", true);
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();
            mapperService = createIndex("test").mapperService();
            DocumentMapper mapper =
                mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappingSource().toString(), containsString("\"index_phrases\":true"));
        }
        {
            boolean useMatchMappingType = randomBoolean();
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject("type");
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template");
                    if (useMatchMappingType) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("foo", "bar");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();

            DocumentMapper mapper =
                mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappingSource().toString(), containsString("\"foo\":\"bar\""));
            if (useMatchMappingType) {
                assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"*\",\"mapping\":{" +
                    "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], " +
                    "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [binary]]");
            } else {
                assertWarnings("dynamic template [my_template] has invalid content [{\"match\":\"string_*\",\"mapping\":{" +
                    "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], " +
                    "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [binary]]");
            }
        }
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testIllegalDynamicTemplatePre7Dot7Index() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        Version createdVersion = randomVersionBetween(random(), LegacyESVersion.V_7_0_0, LegacyESVersion.V_7_6_0);
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion)
            .build();
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(Strings.toString(mapping)), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"type\":\"string\""));
    }
}
