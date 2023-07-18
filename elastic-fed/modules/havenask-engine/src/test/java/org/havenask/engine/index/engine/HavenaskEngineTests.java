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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.havenask.action.bulk.BackoffPolicy;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.engine.rpc.WriteRequest;
import org.havenask.engine.rpc.WriteResponse;
import org.havenask.index.engine.Engine.Operation;
import org.havenask.index.engine.EngineTestCase;
import org.havenask.index.mapper.ParsedDocument;
import suez.service.proto.ErrorCode;

import static org.havenask.engine.index.engine.HavenaskEngine.DEFAULT_TIMEOUT;
import static org.havenask.engine.index.engine.HavenaskEngine.MAX_RETRY;
import static org.havenask.engine.index.engine.HavenaskEngine.buildProducerRecord;
import static org.havenask.engine.index.engine.HavenaskEngine.initKafkaProducer;
import static org.havenask.engine.index.engine.HavenaskEngine.toHaIndex;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ThreadLeakFilters(filters = { KafkaThreadLeakFilter.class })
public class HavenaskEngineTests extends EngineTestCase {

    // test toHaIndex
    public void testToHaIndex() throws IOException {
        ParsedDocument parsedDocument = createParsedDoc("id", "routing");
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        assertEquals(haDoc.get("_seq_no"), "-2");
        assertEquals(haDoc.get("_primary_term"), "0");
        assertEquals(haDoc.get("_version"), "0");
        assertEquals(haDoc.get("_id").trim(), "id");
        assertEquals(haDoc.get("_routing"), "routing");
        assertEquals(haDoc.get("_source"), "{ \"value\" : \"test\" }");
        assertEquals(haDoc.get("value"), "test");
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

    // test initKafkaProducer
    public void testInitKafkaProducer() {
        Settings settings = Settings.builder().put(EngineSettings.HAVENASK_REALTIME_BOOTSTRAP_SERVERS.getKey(), "127.0.0.1:9092").build();
        KafkaProducer<String, String> producer = initKafkaProducer(settings);
        assertNotNull(producer);
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
        WriteResponse response = HavenaskEngine.retryWrite(searcherClient, writeRequest);
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
        WriteResponse response = HavenaskEngine.retryWrite(searcherClient, writeRequest);
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
        WriteResponse response = HavenaskEngine.retryWrite(searcherClient, writeRequest);
        long cost = System.currentTimeMillis() - start;
        assertTrue(cost < DEFAULT_TIMEOUT.millis());
        assertEquals(response, writeResponse);
    }
}
