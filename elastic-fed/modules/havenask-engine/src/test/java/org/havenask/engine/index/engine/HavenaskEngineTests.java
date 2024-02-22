/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.index.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.havenask.action.bulk.BackoffPolicy;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.mapper.VectorField;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.engine.rpc.WriteRequest;
import org.havenask.engine.rpc.WriteResponse;
import org.havenask.index.engine.Engine.Operation;
import org.havenask.index.engine.EngineTestCase;
import org.havenask.index.mapper.KeywordFieldMapper;
import org.havenask.index.mapper.ParseContext;
import org.havenask.index.mapper.ParsedDocument;
import org.havenask.index.shard.ShardId;
import suez.service.proto.ErrorCode;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.havenask.engine.index.engine.HavenaskEngine.DEFAULT_TIMEOUT;
import static org.havenask.engine.index.engine.HavenaskEngine.MAX_RETRY;
import static org.havenask.engine.index.engine.HavenaskEngine.buildProducerRecord;
import static org.havenask.engine.index.engine.HavenaskEngine.toHaIndex;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ThreadLeakFilters(filters = { KafkaThreadLeakFilter.class })
public class HavenaskEngineTests extends EngineTestCase {
    // test toHaIndex
    public void testToHaIndex() throws IOException {
        ParseContext.Document document = testDocument();
        document.add(new KeywordFieldMapper.KeywordField("user.name", new BytesRef("Bob"), new FieldType()));
        document.add(new SortedNumericDocValuesField("user.age", 25));
        document.add(new VectorField("user.image", new float[] { 0.4F, 0.7F }, new FieldType()));
        String source = "\"user\": { \n"
            + "        \"properties\": {\n"
            + "          \"name\": { \n"
            + "            \"type\": \"keyword\"\n"
            + "          },\n"
            + "          \"age\": { \n"
            + "            \"type\": \"integer\"\n"
            + "          },\n"
            + "          \"image\":{\n"
            + "            \"type\":\"vector\",\n"
            + "            \"dims\":\"2\",\n"
            + "            \"similarity\":\"l2_norm\"\n"
            + "          }\n"
            + "        }\n"
            + "      }";
        BytesReference ref = new BytesArray(source);

        ParsedDocument parsedDocument = testParsedDocument("id", "routing", document, ref, null);
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        assertEquals(haDoc.get("_seq_no"), "-2");
        assertEquals(haDoc.get("_primary_term"), "0");
        assertEquals(haDoc.get("_version"), "0");
        assertEquals(haDoc.get("_id").trim(), "id");
        assertEquals(haDoc.get("_routing"), "routing");
        assertEquals(haDoc.get("_source"), source);
        assertEquals(haDoc.get("user_name"), "Bob");
        assertEquals(haDoc.get("user_age"), "25");
        assertEquals(haDoc.get("user_image"), "0.4,0.7");
    }

    // test toHaIndex with multi XContentType in _source
    public void testMultiXContentTypeToHaIndex() throws IOException {
        XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent());
        builder.startObject();
        builder.field("value", "test");
        builder.endObject();
        builder.close();
        BytesReference binaryVal = BytesReference.bytes(builder);
        ParsedDocument parsedDocument = createParsedDoc("id", "routing", binaryVal, XContentType.SMILE);
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        assertEquals(haDoc.get("_seq_no"), "-2");
        assertEquals(haDoc.get("_primary_term"), "0");
        assertEquals(haDoc.get("_version"), "0");
        assertEquals(haDoc.get("_id").trim(), "id");
        assertEquals(haDoc.get("_routing"), "routing");
        assertEquals(haDoc.get("_source"), "{\"value\":\"test\"}");
        assertEquals(haDoc.get("value"), "test");

        builder = XContentBuilder.builder(XContentType.CBOR.xContent());
        builder.startObject();
        builder.field("value", "test");
        builder.endObject();
        builder.close();
        binaryVal = BytesReference.bytes(builder);
        parsedDocument = createParsedDoc("id", "routing", binaryVal, XContentType.CBOR);
        haDoc = toHaIndex(parsedDocument);
        assertEquals(haDoc.get("_source"), "{\"value\":\"test\"}");

        builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        builder.field("value", "test");
        builder.endObject();
        builder.close();
        binaryVal = BytesReference.bytes(builder);
        parsedDocument = createParsedDoc("id", "routing", binaryVal, XContentType.JSON);
        haDoc = toHaIndex(parsedDocument);
        assertEquals(haDoc.get("_source"), "{\"value\":\"test\"}");

        builder = XContentBuilder.builder(XContentType.YAML.xContent());
        builder.startObject();
        builder.field("value", "test");
        builder.endObject();
        builder.close();
        binaryVal = BytesReference.bytes(builder);
        parsedDocument = createParsedDoc("id", "routing", binaryVal, XContentType.YAML);
        haDoc = toHaIndex(parsedDocument);
        assertEquals(haDoc.get("_source"), "{\"value\":\"test\"}");
    }

    // test toHaIndex routing is null
    public void testToHaIndexRoutingIsNull() throws IOException {
        ParsedDocument parsedDocument = createParsedDoc("id", null);
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        assertEquals(haDoc.get("_seq_no"), "-2");
        assertEquals(haDoc.get("_primary_term"), "0");
        assertEquals(haDoc.get("_version"), "0");
        assertEquals(haDoc.get("_id").trim(), "id");
        assertEquals(haDoc.get("_routing"), null);
        assertEquals(haDoc.get("_source"), "{ \"value\" : \"test\" }");
        assertEquals(haDoc.get("value"), "test");
    }

    // test buildProducerRecord
    public void testBuildProducerRecord() throws IOException {
        ParsedDocument parsedDocument = createParsedDoc("id", "routing");
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        ProducerRecord<String, String> record = buildProducerRecord("id", Operation.TYPE.INDEX, "topicName", 1, haDoc);
        assertEquals(record.key(), "id");
        assertEquals(
            record.value(),
            "CMD=add\u001F\n"
                + "_routing=routing\u001F\n"
                + "_seq_no=-2\u001F\n"
                + "_source={ \"value\" : \"test\" }\u001F\n"
                + "_id=id\u001F\n"
                + "value=test\u001F\n"
                + "_version=0\u001F\n"
                + "_primary_term=0\u001F\n\u001E\n"
        );
        assertEquals(record.topic(), "topicName");
        assertEquals(record.partition(), Integer.valueOf(0));
    }

    // test buildProducerRecord type is delete
    public void testBuildProducerRecordTypeIsDelete() throws IOException {
        ParsedDocument parsedDocument = createParsedDoc("id", "routing");
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        ProducerRecord<String, String> record = buildProducerRecord("id", Operation.TYPE.DELETE, "topicName", 1, haDoc);
        assertEquals(record.key(), "id");
        assertEquals(
            record.value(),
            "CMD=delete\u001F\n"
                + "_routing=routing\u001F\n"
                + "_seq_no=-2\u001F\n"
                + "_source={ \"value\" : \"test\" }\u001F\n"
                + "_id=id\u001F\n"
                + "value=test\u001F\n"
                + "_version=0\u001F\n"
                + "_primary_term=0\u001F\n\u001E\n"
        );
        assertEquals(record.topic(), "topicName");
        assertEquals(record.partition(), Integer.valueOf(0));
    }

    // test retryWrite
    public void testRetryWriteTimeout() {
        SearcherClient searcherClient = mock(SearcherClient.class);
        WriteRequest writeRequest = mock(WriteRequest.class);
        WriteResponse writeResponse = mock(WriteResponse.class);
        when(searcherClient.write(writeRequest)).thenReturn(writeResponse);
        when(writeResponse.getErrorCode()).thenReturn(ErrorCode.TBS_ERROR_UNKOWN);
        when(writeResponse.getErrorMessage()).thenReturn("write response is null");
        long start = System.currentTimeMillis();
        WriteResponse response = HavenaskEngine.retryWrite(mock(ShardId.class), searcherClient, writeRequest);
        long cost = System.currentTimeMillis() - start;
        Iterator<TimeValue> backoff = BackoffPolicy.exponentialBackoff(DEFAULT_TIMEOUT, MAX_RETRY).iterator();
        long backoffTime = 0;
        while (backoff.hasNext()) {
            backoffTime += backoff.next().millis();
        }

        assertTrue(cost > backoffTime);
        assertEquals(response, writeResponse);
    }

    public void testRetryWriteQueueFull() {
        SearcherClient searcherClient = mock(SearcherClient.class);
        WriteRequest writeRequest = mock(WriteRequest.class);
        WriteResponse writeResponse = mock(WriteResponse.class);
        when(searcherClient.write(writeRequest)).thenReturn(writeResponse);
        when(writeResponse.getErrorCode()).thenReturn(ErrorCode.TBS_ERROR_OTHERS);
        when(writeResponse.getErrorMessage()).thenReturn("doc queue is full");
        long start = System.currentTimeMillis();
        WriteResponse response = HavenaskEngine.retryWrite(mock(ShardId.class), searcherClient, writeRequest);
        long cost = System.currentTimeMillis() - start;
        Iterator<TimeValue> backoff = BackoffPolicy.exponentialBackoff(DEFAULT_TIMEOUT, MAX_RETRY).iterator();
        long backoffTime = 0;
        while (backoff.hasNext()) {
            backoffTime += backoff.next().millis();
        }

        assertTrue(cost > backoffTime);
        assertEquals(response, writeResponse);
    }

    public void testRetryNoValidTable() {
        SearcherClient searcherClient = mock(SearcherClient.class);
        WriteRequest writeRequest = mock(WriteRequest.class);
        WriteResponse writeResponse = mock(WriteResponse.class);
        when(searcherClient.write(writeRequest)).thenReturn(writeResponse);
        when(writeResponse.getErrorCode()).thenReturn(ErrorCode.TBS_ERROR_OTHERS);
        when(writeResponse.getErrorMessage()).thenReturn("no valid table/range for WriteRequest table: test hashid: 25383");
        long start = System.currentTimeMillis();
        WriteResponse response = HavenaskEngine.retryWrite(mock(ShardId.class), searcherClient, writeRequest);
        long cost = System.currentTimeMillis() - start;
        Iterator<TimeValue> backoff = BackoffPolicy.exponentialBackoff(DEFAULT_TIMEOUT, MAX_RETRY).iterator();
        long backoffTime = 0;
        while (backoff.hasNext()) {
            backoffTime += backoff.next().millis();
        }

        assertTrue(cost > backoffTime);
        assertEquals(response, writeResponse);
    }

    public void testWriteNoRetry() {
        SearcherClient searcherClient = mock(SearcherClient.class);
        WriteRequest writeRequest = mock(WriteRequest.class);
        WriteResponse writeResponse = mock(WriteResponse.class);
        when(searcherClient.write(writeRequest)).thenReturn(writeResponse);
        when(writeResponse.getErrorCode()).thenReturn(ErrorCode.TBS_ERROR_NONE);
        when(writeResponse.getErrorMessage()).thenReturn(null);
        long start = System.currentTimeMillis();
        WriteResponse response = HavenaskEngine.retryWrite(mock(ShardId.class), searcherClient, writeRequest);
        long cost = System.currentTimeMillis() - start;
        assertTrue(cost < DEFAULT_TIMEOUT.millis());
        assertEquals(response, writeResponse);
    }

    public void testBuildDocMessage() throws IOException {
        String basicSourceDoc = "{\n"
            + "  \"my_string\":\"string_test\",\n"
            + "  \"my_int\":1,\n"
            + "  \"my_long\":3000000000,\n"
            + "  \"my_double\":13.5,\n"
            + "  \"my_float\":12.3,\n"
            + "  \"my_boolean\":true,\n"
            + "  \"my_int_array\":[1,2,3,4],\n"
            + "  \"my_string_array\":[\"aaa\",\"bbb\",\"ccc\"],\n"
            + "  \"my_boolean_array\":[true,true,false,true,false],\n"
            + "  \"my_object\":{\n"
            + "    \"first\":\"first\",\n"
            + "    \"last\":\"last\"\n"
            + "  },\n"
            + "  \"my_null\":null\n"
            + "}";
        BytesReference basicSource = new BytesArray(basicSourceDoc);
        ParsedDocument parsedDocument = testParsedDocument("id", "routing", new ParseContext.Document(), basicSource, null);
        String basicRes = HavenaskEngine.buildHaDocMessage(basicSource, parsedDocument, Operation.TYPE.INDEX);

        String expectedBasicRes = "CMD=add\u001F\n"
            + "my_string=string_test\u001F\n"
            + "my_int=1\u001F\n"
            + "my_long=3000000000\u001F\n"
            + "my_double=13.5\u001F\n"
            + "my_float=12.3\u001F\n"
            + "my_boolean=true\u001F\n"
            + "my_int_array=1\u001D2\u001D3\u001D4\u001F\n"
            + "my_string_array=aaa\u001Dbbb\u001Dccc\u001F\n"
            + "my_boolean_array=true\u001Dtrue\u001Dfalse\u001Dtrue\u001Dfalse\u001F\n"
            + "my_object_first=first\u001F\n"
            + "my_object_last=last\u001F\n"
            + "_id=id\u001F\n"
            + "_routing=routing\u001F\n"
            + "_version=0\u001F\n"
            + "_seq_no=-2\u001F\n"
            + "_primary_term=0\u001F\n"
            + "_source={\n"
            + "  \"my_string\":\"string_test\",\n"
            + "  \"my_int\":1,\n"
            + "  \"my_long\":3000000000,\n"
            + "  \"my_double\":13.5,\n"
            + "  \"my_float\":12.3,\n"
            + "  \"my_boolean\":true,\n"
            + "  \"my_int_array\":[1,2,3,4],\n"
            + "  \"my_string_array\":[\"aaa\",\"bbb\",\"ccc\"],\n"
            + "  \"my_boolean_array\":[true,true,false,true,false],\n"
            + "  \"my_object\":{\n"
            + "    \"first\":\"first\",\n"
            + "    \"last\":\"last\"\n"
            + "  },\n"
            + "  \"my_null\":null\n"
            + "}\u001F\n"
            + "\u001E\n";
        assertEquals(expectedBasicRes, basicRes);

        String vectorSourceDoc =
            "{\"image_vector\": [42, 8, -15], \"title_vector\": [25, 1, 4, -12, 2], \"title\": \"alpine lake\", \"file_type\": \"png\"}";
        BytesReference vectorSource = new BytesArray(vectorSourceDoc);
        parsedDocument = testParsedDocument("id", "routing", new ParseContext.Document(), vectorSource, null);
        String vectorRes = HavenaskEngine.buildHaDocMessage(vectorSource, parsedDocument, Operation.TYPE.INDEX);

        String vectorExpectedRes = "CMD=add\u001F\n"
            + "image_vector=42\u001D8\u001D-15\u001F\n"
            + "title_vector=25\u001D1\u001D4\u001D-12\u001D2\u001F\n"
            + "title=alpine lake\u001F\n"
            + "file_type=png\u001F\n"
            + "_id=id\u001F\n"
            + "_routing=routing\u001F\n"
            + "_version=0\u001F\n"
            + "_seq_no=-2\u001F\n"
            + "_primary_term=0\u001F\n"
            + "_source={\"image_vector\": [42, 8, -15], \"title_vector\": [25, 1, 4, -12, 2], "
            + "\"title\": \"alpine lake\", \"file_type\": \"png\"}\u001F\n"
            + "\u001E\n";
        assertEquals(vectorExpectedRes, vectorRes);

        String objectSourceDoc = "{\n"
            + "  \"user\":{\n"
            + "    \"first_name\":\"first\",\n"
            + "    \"last_name\":\"last\"\n"
            + "  }\n"
            + "}";
        BytesReference objectSource = new BytesArray(objectSourceDoc);
        parsedDocument = testParsedDocument("id", "routing", new ParseContext.Document(), objectSource, null);
        String objectRes = HavenaskEngine.buildHaDocMessage(objectSource, parsedDocument, Operation.TYPE.INDEX);

        String expectedObjectRes = "CMD=add\u001F\n"
            + "user_first_name=first\u001F\n"
            + "user_last_name=last\u001F\n"
            + "_id=id\u001F\n"
            + "_routing=routing\u001F\n"
            + "_version=0\u001F\n"
            + "_seq_no=-2\u001F\n"
            + "_primary_term=0\u001F\n"
            + "_source={\n"
            + "  \"user\":{\n"
            + "    \"first_name\":\"first\",\n"
            + "    \"last_name\":\"last\"\n"
            + "  }\n"
            + "}\u001F\n"
            + "\u001E\n";
        assertEquals(expectedObjectRes, objectRes);

        String SourceDocWithDot = "{\n" + "  \"user.first_name\":\"first\",\n" + "  \"user.last_name\":\"last\"\n" + "}";
        BytesReference SourceWithDot = new BytesArray(SourceDocWithDot);
        parsedDocument = testParsedDocument("id", "routing", new ParseContext.Document(), SourceWithDot, null);
        String SourceWithDotRes = HavenaskEngine.buildHaDocMessage(SourceWithDot, parsedDocument, Operation.TYPE.INDEX);
        String expectedSourceWithDotRes = "CMD=add\u001F\n"
            + "user_first_name=first\u001F\n"
            + "user_last_name=last\u001F\n"
            + "_id=id\u001F\n"
            + "_routing=routing\u001F\n"
            + "_version=0\u001F\n"
            + "_seq_no=-2\u001F\n"
            + "_primary_term=0\u001F\n"
            + "_source={\n"
            + "  \"user.first_name\":\"first\",\n"
            + "  \"user.last_name\":\"last\"\n"
            + "}\u001F\n"
            + "\u001E\n";
        ;
        assertEquals(expectedSourceWithDotRes, SourceWithDotRes);
    }
}
