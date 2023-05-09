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

package org.havenask.index.mapper;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.util.ULocale;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.havenask.common.Strings;
import org.havenask.common.collect.List;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.plugin.analysis.icu.AnalysisICUPlugin;
import org.havenask.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ICUCollationKeywordFieldMapperTests extends FieldMapperTestCase2<ICUCollationKeywordFieldMapper.Builder> {

    private static final String FIELD_TYPE = "icu_collation_keyword";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnalysisICUPlugin());
    }

    @Override
    protected ICUCollationKeywordFieldMapper.Builder newBuilder() {
        return new ICUCollationKeywordFieldMapper.Builder("icu");
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return org.havenask.common.collect.Set.of("analyzer", "similarity");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("strength", b -> b.field("strength", "secondary"));
        checker.registerConflictCheck("decomposition", b -> b.field("decomposition", "canonical"));
        checker.registerConflictCheck("alternate", b -> b.field("alternate", "non-ignorable"));
        checker.registerConflictCheck("case_level", b -> b.field("case_level", true));
        checker.registerConflictCheck("case_first", b -> b.field("case_first", "lower"));
        checker.registerConflictCheck("numeric", b -> b.field("numeric", true));
        checker.registerConflictCheck("variable_top", b -> b.field("variable_top", ":"));
        checker.registerConflictCheck("hiragana_quaternary_mode", b -> b.field("hiragana_quaternary_mode", true));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", FIELD_TYPE);
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value(1234);
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        Collator collator = Collator.getInstance(ULocale.ROOT);
        RawCollationKey key = collator.getRawCollationKey("1234", null);
        BytesRef expected = new BytesRef(key.bytes, 0, key.size);

        assertEquals(expected, fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(expected, fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("null_value", "1234")));
        doc = mapper.parse(source(b -> {
        }));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);

        doc = mapper.parse(source(b -> b.nullField("field")));
        Collator collator = Collator.getInstance(ULocale.ROOT);
        RawCollationKey key = collator.getRawCollationKey("1234", null);
        BytesRef expected = new BytesRef(key.bytes, 0, key.size);

        fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(expected, fields[0].binaryValue());
    }

    public void testEnableStore() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertTrue(fields[0].fieldType().stored());
    }

    public void testDisableIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.NONE, fields[0].fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fields[0].fieldType().docValuesType());
    }

    public void testDisableDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("doc_values", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
    }

    public void testMultipleValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", List.of("1234", "5678"))));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(4, fields.length);

        Collator collator = Collator.getInstance(ULocale.ROOT);
        RawCollationKey key = collator.getRawCollationKey("1234", null);
        BytesRef expected = new BytesRef(key.bytes, 0, key.size);

        assertEquals(expected, fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(expected, fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());

        collator = Collator.getInstance(ULocale.ROOT);
        key = collator.getRawCollationKey("5678", null);
        expected = new BytesRef(key.bytes, 0, key.size);

        assertEquals(expected, fields[2].binaryValue());
        fieldType = fields[2].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(expected, fields[3].binaryValue());
        fieldType = fields[3].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
    }

    public void testIndexOptions() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("index_options", "freqs")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(IndexOptions.DOCS_AND_FREQS, fields[0].fieldType().indexOptions());

        for (String indexOptions : Arrays.asList("positions", "offsets")) {
            Exception e = expectThrows(MapperParsingException.class,
                () -> createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("index_options", indexOptions))));
            assertThat(
                e.getMessage(),
                containsString("The [" + FIELD_TYPE + "] field does not support positions, got [index_options]=" + indexOptions)
            );
        }
    }

    public void testEnableNorms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("norms", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertFalse(fields[0].fieldType().omitNorms());
    }

    public void testCollator() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", FIELD_TYPE).field("language", "tr").field("strength", "primary"))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "I WİLL USE TURKİSH CASING")));
        Collator collator = Collator.getInstance(new ULocale("tr"));
        collator.setStrength(Collator.PRIMARY);
        RawCollationKey key = collator.getRawCollationKey("ı will use turkish casıng", null); // should collate to same value
        BytesRef expected = new BytesRef(key.bytes, 0, key.size);

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals(expected, fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals(expected, fields[1].binaryValue());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());
    }

    public void testUpdateCollator() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", FIELD_TYPE).field("language", "tr").field("strength", "primary"))
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", FIELD_TYPE).field("language", "en")))
        );
        assertThat(e.getMessage(), containsString("mapper [field] has different [collator]"));
    }


    public void testIgnoreAbove() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", FIELD_TYPE).field("ignore_above", 5)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "elk")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        doc = mapper.parse(source(b -> b.field("field", "havenask")));
        fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testUpdateIgnoreAbove() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        merge(mapperService, fieldMapping(b -> b.field("type", FIELD_TYPE).field("ignore_above", 5)));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "havenask")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

}
