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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.havenask.index.engine.Engine.Operation;
import org.havenask.index.engine.EngineTestCase;
import org.havenask.index.mapper.ParsedDocument;

import static org.havenask.engine.index.engine.HavenaskEngine.buildProducerRecord;
import static org.havenask.engine.index.engine.HavenaskEngine.toHaIndex;

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
            "CMD=add\\x1F\\n_routing=routing\\x1F\\n_seq_no=-2\\x1F\\n_source={ \"value\" : "
                + "\"test\" }\\x1F\\n_id=id\\x1F\\nvalue=test\\x1F\\n_version=0\\x1F\\n_primary_term=0\\x1F\\n\\x1E\\n"
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
            "CMD=delete\\x1F\\n_routing=routing\\x1F\\n_seq_no=-2\\x1F\\n_source={ \"value\" : "
                + "\"test\" }\\x1F\\n_id=id\\x1F\\nvalue=test\\x1F\\n_version=0\\x1F\\n_primary_term=0\\x1F\\n\\x1E\\n"
        );
        assertEquals(record.topic(), "topicName");
        assertEquals(record.partition(), Integer.valueOf(0));
    }
}
