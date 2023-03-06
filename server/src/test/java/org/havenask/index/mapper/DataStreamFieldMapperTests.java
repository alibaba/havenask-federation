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

package org.havenask.index.mapper;

import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskSingleNodeTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamFieldMapperTests extends HavenaskSingleNodeTestCase {

    public void testDefaultTimestampField() throws Exception {
        String mapping = Strings.toString(XContentFactory
            .jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject());

        assertDataStreamFieldMapper(mapping, "@timestamp");
    }

    public void testCustomTimestampField() throws Exception {
        String timestampFieldName = "timestamp_" + randomAlphaOfLength(5);

        String mapping = Strings.toString(XContentFactory
            .jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .startObject("timestamp_field")
            .field("name", timestampFieldName)
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        assertDataStreamFieldMapper(mapping, timestampFieldName);
    }

    public void testDeeplyNestedCustomTimestampField() throws Exception {
        String mapping = Strings.toString(XContentFactory
            .jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .startObject("timestamp_field")
            .field("name", "event.meta.created_at")
            .endObject()
            .endObject()
            .endObject()
            .endObject());

        DocumentMapper mapper = createIndex("test")
            .mapperService()
            .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "_doc", "1", BytesReference.bytes(
            XContentFactory
                .jsonBuilder()
                .startObject()
                .startObject("event")
                .startObject("meta")
                .field("created_at", "2020-12-06T11:04:05.000Z")
                .endObject()
                .endObject()
                .endObject()
        ), XContentType.JSON));
        assertThat(doc.rootDoc().getFields("event.meta.created_at").length, equalTo(2));

        MapperException exception = expectThrows(MapperException.class, () -> {
            mapper.parse(new SourceToParse("test", "_doc", "3", BytesReference.bytes(
                XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .startObject("event")
                    .startObject("meta")
                    .array("created_at", "2020-12-06T11:04:05.000Z", "2020-12-07T11:04:05.000Z")
                    .endObject()
                    .endObject()
                    .endObject()
            ), XContentType.JSON));
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field 'event.meta.created_at' of date type")
        );
    }

    private void assertDataStreamFieldMapper(String mapping, String timestampFieldName) throws Exception {
        DocumentMapper mapper = createIndex("test")
            .mapperService()
            .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        // Success case - document has timestamp field correctly populated.
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "_doc", "1", BytesReference.bytes(
            XContentFactory
                .jsonBuilder()
                .startObject()
                .field(timestampFieldName, "2020-12-06T11:04:05.000Z")
                .endObject()
        ), XContentType.JSON));

        // A valid timestamp field will be parsed as LongPoint and SortedNumericDocValuesField.
        assertThat(doc.rootDoc().getFields(timestampFieldName).length, equalTo(2));

        MapperException exception;

        // Failure case - document doesn't have a valid timestamp field.
        exception = expectThrows(MapperException.class, () -> {
            mapper.parse(new SourceToParse("test", "_doc", "2", BytesReference.bytes(
                XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .field("invalid-field-name", "2020-12-06T11:04:05.000Z")
                    .endObject()
            ), XContentType.JSON));
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );

        // Failure case - document contains multiple values for the timestamp field.
        exception = expectThrows(MapperException.class, () -> {
            mapper.parse(new SourceToParse("test", "_doc", "3", BytesReference.bytes(
                XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .array(timestampFieldName, "2020-12-06T11:04:05.000Z", "2020-12-07T11:04:05.000Z")
                    .endObject()
            ), XContentType.JSON));
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );
    }

}
