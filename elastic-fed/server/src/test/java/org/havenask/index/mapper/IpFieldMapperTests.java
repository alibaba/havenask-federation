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

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.havenask.common.network.InetAddresses;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.termvectors.TermVectorsService;
import org.havenask.index.mapper.MapperTestCase;

import java.io.IOException;
import java.net.InetAddress;

import static org.hamcrest.Matchers.containsString;

public class IpFieldMapperTests extends MapperTestCase {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("::1");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "ip");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "::1"));
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", false),
            m -> assertFalse(((IpFieldMapper) m).ignoreMalformed()));
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(16, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), dvField.binaryValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("index", false);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("doc_values", false);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());

        fields = doc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(1, fields.length);
        assertEquals("field", fields[0].stringValue());

        FieldMapper m = (FieldMapper) mapper.mappers().getMapper("field");
        Query existsQuery = m.fieldType().existsQuery(null);
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), existsQuery);
    }

    public void testStore() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("store", true);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "::1")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddress.getByName("::1"))),
                storedField.binaryValue());
    }

    public void testIgnoreMalformed() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", ":1")));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("':1' is not an IP string literal"));

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("ignore_malformed", true);
        }));

        ParsedDocument doc = mapper2.parse(source(b -> b.field("field", ":1")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        assertArrayEquals(new String[] { "field" }, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));
    }

    public void testNullValue() throws IOException {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("null_value", "::1");
        }));

        doc = mapper.parse(source(b -> b.nullField("field")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(16, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), dvField.binaryValue());
        assertFalse(dvField.fieldType().stored());

        mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.nullField("null_value");
        }));

        doc = mapper.parse(source(b -> b.nullField("field")));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        createDocumentMapper(fieldMapping(b -> {
            b.field("type", "ip");
            b.field("null_value", ":1");
        }));
        assertWarnings("Error parsing [:1] as IP in [null_value] on field [field]); [null_value] will be ignored");
    }
}
