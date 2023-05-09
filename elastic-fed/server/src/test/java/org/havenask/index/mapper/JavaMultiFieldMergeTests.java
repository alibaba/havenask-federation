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

import org.apache.lucene.index.IndexableField;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.mapper.ParseContext.Document;
import org.havenask.test.HavenaskSingleNodeTestCase;

import static org.havenask.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class JavaMultiFieldMergeTests extends HavenaskSingleNodeTestCase {
    public void testMergeMultiField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/test-mapping1.json");
        MapperService mapperService = createIndex("test").mapperService();

        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());
        assertThat(mapperService.fieldType("name.indexed"), nullValue());

        BytesReference json = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("name", "some name").endObject());
        Document doc = mapperService.documentMapper().parse(
            new SourceToParse("test", "person", "1", json, XContentType.JSON)).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());

        mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/test-mapping2.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), nullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());

        doc = mapperService.documentMapper().parse(new SourceToParse("test", "person", "1", json, XContentType.JSON)).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/test-mapping3.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());

        mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/test-mapping4.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), notNullValue());
    }

    public void testUpgradeFromMultiFieldTypeToMultiFields() throws Exception {
        String mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/test-mapping1.json");
        MapperService mapperService = createIndex("test").mapperService();

        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());
        assertThat(mapperService.fieldType("name.indexed"), nullValue());

        BytesReference json = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("name", "some name").endObject());
        Document doc = mapperService.documentMapper().parse(
            new SourceToParse("test", "person", "1", json, XContentType.JSON)).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());


        mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/upgrade1.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), nullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());

        doc = mapperService.documentMapper().parse(
            new SourceToParse("test", "person", "1", json, XContentType.JSON)).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/upgrade2.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());


        mapping = copyToStringFromClasspath("/org/havenask/index/mapper/multifield/merge/upgrade3.json");
        try {
            mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot update parameter [index] from [true] to [false]"));
            assertThat(e.getMessage(), containsString("Cannot update parameter [store] from [true] to [false]"));
        }

        // There are conflicts, so the `name.not_indexed3` has not been added
        assertTrue(mapperService.fieldType("name").isSearchable());
        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());
    }
}
