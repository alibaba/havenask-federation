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

package org.havenask.ingest.common;

import org.havenask.ingest.IngestDocument;
import org.havenask.ingest.Processor;
import org.havenask.ingest.RandomDocumentPicks;
import org.havenask.test.HavenaskTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.havenask.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractStringProcessorTestCase<T> extends HavenaskTestCase {

    protected abstract AbstractStringProcessor<T> newProcessor(String field, boolean ignoreMissing, String targetField);

    protected String modifyInput(String input) {
        return input;
    }

    protected abstract T expectedResult(String input);

    protected Class<?> expectedResultType() {
        return String.class;  // most results types are Strings
    }

    public void testProcessor() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldValue = RandomDocumentPicks.randomString(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, modifyInput(fieldValue));
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, expectedResultType()), equalTo(expectedResult(fieldValue)));

        int numItems = randomIntBetween(1, 10);
        List<String> fieldValueList = new ArrayList<>();
        List<T> expectedList = new ArrayList<>();
        for (int i = 0; i < numItems; i++) {
            String randomString = RandomDocumentPicks.randomString(random());
            fieldValueList.add(modifyInput(randomString));
            expectedList.add(expectedResult(randomString));
        }
        String multiValueFieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValueList);
        Processor multiValueProcessor = newProcessor(multiValueFieldName, randomBoolean(), multiValueFieldName);
        multiValueProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(multiValueFieldName, List.class), equalTo(expectedList));
    }

    public void testFieldNotFound() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, false, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
    }

    public void testFieldNotFoundWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, true, fieldName);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValue() throws Exception {
        Processor processor = newProcessor("field", false, "field");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] is null, cannot process it."));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        Processor processor = newProcessor("field", true, "field");
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonStringValue() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, false, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName +
            "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));

        List<Integer> fieldValueList = new ArrayList<>();
        int randomValue = randomInt();
        fieldValueList.add(randomValue);
        ingestDocument.setFieldValue(fieldName, fieldValueList);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("value [" + randomValue + "] of type [java.lang.Integer] in list field [" + fieldName +
            "] cannot be cast to [java.lang.String]"));
    }

    public void testNonStringValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, true, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName +
            "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));

        List<Integer> fieldValueList = new ArrayList<>();
        int randomValue = randomInt();
        fieldValueList.add(randomValue);
        ingestDocument.setFieldValue(fieldName, fieldValueList);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("value [" + randomValue + "] of type [java.lang.Integer] in list field [" + fieldName +
            "] cannot be cast to [java.lang.String]"));
    }

    public void testTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        String fieldValue = RandomDocumentPicks.randomString(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, modifyInput(fieldValue));
        String targetFieldName = fieldName + "foo";
        Processor processor = newProcessor(fieldName, randomBoolean(), targetFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, expectedResultType()), equalTo(expectedResult(fieldValue)));
    }
}
