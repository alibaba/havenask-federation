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
import org.havenask.ingest.IngestDocument.Metadata;
import org.havenask.ingest.Processor;
import org.havenask.ingest.RandomDocumentPicks;
import org.havenask.ingest.TestTemplateService;
import org.havenask.ingest.ValueSource;
import org.havenask.test.HavenaskTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

public class SetProcessorTests extends HavenaskTestCase {

    public void testSetExistingFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        Processor processor = createSetProcessor(fieldName, fieldValue, true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetNewFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        //used to verify that there are no conflicts between subsequent fields going to be added
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), testIngestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, fieldValue, true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetFieldsTypeMismatch() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue("field", "value");
        Processor processor = createSetProcessor("field.inner", "value", true, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot set [inner] with parent object of type [java.lang.String] as " +
                    "part of path [field.inner]"));
        }
    }

    public void testSetNewFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        Processor processor = createSetProcessor(fieldName, fieldValue, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetExistingFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        Object fieldValue = "foo";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, "bar", false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetExistingNullFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        Object fieldValue = null;
        Object newValue = "bar";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, newValue, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(newValue));
    }

    public void testSetMetadataExceptVersion() throws Exception {
        Metadata randomMetadata = randomFrom(Metadata.INDEX, Metadata.TYPE, Metadata.ID, Metadata.ROUTING);
        Processor processor = createSetProcessor(randomMetadata.getFieldName(), "_value", true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(randomMetadata.getFieldName(), String.class), Matchers.equalTo("_value"));
    }

    public void testSetMetadataVersion() throws Exception {
        long version = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.VERSION.getFieldName(), version, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.VERSION.getFieldName(), Long.class), Matchers.equalTo(version));
    }

    public void testSetMetadataVersionType() throws Exception {
        String versionType = randomFrom("internal", "external", "external_gte");
        Processor processor = createSetProcessor(Metadata.VERSION_TYPE.getFieldName(), versionType, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.VERSION_TYPE.getFieldName(), String.class), Matchers.equalTo(versionType));
    }

    public void testSetMetadataIfSeqNo() throws Exception {
        long ifSeqNo = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.IF_SEQ_NO.getFieldName(), ifSeqNo, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.IF_SEQ_NO.getFieldName(), Long.class), Matchers.equalTo(ifSeqNo));
    }

    public void testSetMetadataIfPrimaryTerm() throws Exception {
        long ifPrimaryTerm = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.IF_PRIMARY_TERM.getFieldName(), ifPrimaryTerm, true, false);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.IF_PRIMARY_TERM.getFieldName(), Long.class), Matchers.equalTo(ifPrimaryTerm));
    }

    private static Processor createSetProcessor(String fieldName, Object fieldValue, boolean overrideEnabled, boolean ignoreEmptyValue) {
        return new SetProcessor(randomAlphaOfLength(10), null, new TestTemplateService.MockTemplateScript.Factory(fieldName),
                ValueSource.wrap(fieldValue, TestTemplateService.instance()), overrideEnabled, ignoreEmptyValue);
    }
}
