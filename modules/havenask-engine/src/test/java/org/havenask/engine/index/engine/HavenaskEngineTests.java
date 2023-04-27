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
import java.util.Map;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.havenask.common.settings.Settings;
import org.havenask.index.engine.Engine.Operation;
import org.havenask.index.engine.EngineTestCase;
import org.havenask.index.mapper.ParsedDocument;

import static org.havenask.engine.index.engine.HavenaskEngine.buildProducerRecord;
import static org.havenask.engine.index.engine.HavenaskEngine.initKafkaProducer;
import static org.havenask.engine.index.engine.HavenaskEngine.toHaIndex;

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

    static ProducerRecord<String, String> buildProducerRecord(
        String id,
        Operation.TYPE type,
        String topicName,
        int topicPartition,
        Map<String, String> haDoc
    ) {
        StringBuffer message = new StringBuffer();
        switch (type) {
            case INDEX:
                message.append("CMD=add" + 0x1F + "\n");
                break;
            case DELETE:
                message.append("CMD=delete" + 0x1F + "\n");
                break;
            default:
                throw new IllegalArgumentException("invalid operation type!");
        }

        for (Map.Entry<String, String> entry : haDoc.entrySet()) {
            message.append(entry.getKey()).append("=").append(entry.getValue()).append(0x1F + "\n");
        }
        message.append(0x1E + "\n");
        long hashId = HashAlgorithm.getHashId(id);
        long partition = HashAlgorithm.getPartitionId(hashId, topicPartition);

        return new ProducerRecord<>(topicName, (int) partition, id, message.toString());
    }

    // test buildProducerRecord
    public void testBuildProducerRecord() throws IOException {
        ParsedDocument parsedDocument = createParsedDoc("id", "routing");
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        ProducerRecord<String, String> record = buildProducerRecord("id", Operation.TYPE.INDEX, "topicName", 1, haDoc);
        assertEquals(record.key(), "id");
        assertEquals(record.value(), "CMD=add" + 0x1F + "\n" +
            "_routing=routing" + 0x1F + "\n" +
            "_seq_no=-2" + 0x1F + "\n" +
            "_source={ \"value\" : \"test\" }" + 0x1F + "\n" +
            "_id=id" + 0x1F + "\n" +
            "value=test" + 0x1F + "\n" +
            "_version=0" + 0x1F + "\n" +
            "_primary_term=0" + 0x1F + "\n" +
            0x1E + "\n");
        assertEquals(record.topic(), "topicName");
        assertEquals(record.partition(), Integer.valueOf(0));
    }

    // test buildProducerRecord type is delete
    public void testBuildProducerRecordTypeIsDelete() throws IOException {
        ParsedDocument parsedDocument = createParsedDoc("id", "routing");
        Map<String, String> haDoc = toHaIndex(parsedDocument);
        ProducerRecord<String, String> record = buildProducerRecord("id", Operation.TYPE.DELETE, "topicName", 1, haDoc);
        assertEquals(record.key(), "id");
        assertEquals(record.value(), "CMD=delete" + 0x1F + "\n" +
            "_routing=routing" + 0x1F + "\n" +
            "_seq_no=-2" + 0x1F + "\n" +
            "_source={ \"value\" : \"test\" }" + 0x1F + "\n" +
            "_id=id" + 0x1F + "\n" +
            "value=test" + 0x1F + "\n" +
            "_version=0" + 0x1F + "\n" +
            "_primary_term=0" + 0x1F + "\n" +
            0x1E + "\n");
        assertEquals(record.topic(), "topicName");
        assertEquals(record.partition(), Integer.valueOf(0));
    }


    // test initKafkaProducer
    public void testInitKafkaProducer() {
        Settings settings = Settings.builder().put(EngineSettings.HAVENASK_REALTIME_BOOTSTRAP_SERVERS.getKey(), "127.0.0.1:9092").build();
        KafkaProducer<String, String> producer = initKafkaProducer(settings);
        assertNotNull(producer);
    }
}
