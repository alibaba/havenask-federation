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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class FieldNamesFieldMapperTests extends HavenaskSingleNodeTestCase {

    private static SortedSet<String> extract(String path) {
        SortedSet<String> set = new TreeSet<>();
        for (String fieldName : FieldNamesFieldMapper.extractFieldNames(path)) {
            set.add(fieldName);
        }
        return set;
    }

    private static SortedSet<String> set(String... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    void assertFieldNames(Set<String> expected, ParsedDocument doc) {
        IndexableField[] fields = doc.rootDoc().getFields("_field_names");
        List<String> result = new ArrayList<>();
        for (IndexableField field : fields) {
            if (field.name().equals(FieldNamesFieldMapper.Defaults.NAME)) {
                result.add(field.stringValue());
            }
        }
        assertEquals(expected, set(result.toArray(new String[0])));
    }

    public void testExtractFieldNames() {
        assertEquals(set("abc"), extract("abc"));
        assertEquals(set("a", "a.b"), extract("a.b"));
        assertEquals(set("a", "a.b", "a.b.c"), extract("a.b.c"));
        // and now corner cases
        assertEquals(set("", ".a"), extract(".a"));
        assertEquals(set("a", "a."), extract("a."));
        assertEquals(set("", ".", ".."), extract(".."));
    }

    public void testFieldType() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().hasDocValues());

        assertEquals(IndexOptions.DOCS, FieldNamesFieldMapper.Defaults.FIELD_TYPE.indexOptions());
        assertFalse(FieldNamesFieldMapper.Defaults.FIELD_TYPE.tokenized());
        assertFalse(FieldNamesFieldMapper.Defaults.FIELD_TYPE.stored());
        assertTrue(FieldNamesFieldMapper.Defaults.FIELD_TYPE.omitNorms());
    }

    public void testInjectIntoDocDuringParsing() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("a", "100")
                            .startObject("b")
                                .field("c", 42)
                            .endObject()
                        .endObject()),
                XContentType.JSON));

        assertFieldNames(Collections.emptySet(), doc);
    }

    public void testExplicitEnabled() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .startObject("properties")
            .startObject("field").field("type", "keyword").field("doc_values", false).endObject()
            .endObject().endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertTrue(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()),
            XContentType.JSON));

        assertFieldNames(set("field"), doc);
        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE);
    }

    public void testDisabled() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()),
            XContentType.JSON));

        assertNull(doc.rootDoc().get("_field_names"));
        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE.replace("{}", "test"));
    }

    public void testMergingMappings() throws Exception {
        String enabledMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .endObject().endObject());
        String disabledMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject());
        MapperService mapperService = createIndex("test").mapperService();

        mapperService.merge("type", new CompressedXContent(enabledMapping), MapperService.MergeReason.MAPPING_UPDATE);
        DocumentMapper mapperDisabled = mapperService.merge("type", new CompressedXContent(disabledMapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertFalse(mapperDisabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());

        DocumentMapper mapperEnabled
            = mapperService.merge("type", new CompressedXContent(enabledMapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertTrue(mapperEnabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());
        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE.replace("{}", "test"));
    }
}
