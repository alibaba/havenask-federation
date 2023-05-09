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
import org.havenask.ingest.TestTemplateService;
import org.havenask.test.HavenaskTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoveProcessorTests extends HavenaskTestCase {

    public void testRemoveFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String field = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Processor processor = new RemoveProcessor(randomAlphaOfLength(10),
                null, Collections.singletonList(new TestTemplateService.MockTemplateScript.Factory(field)), false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(field), equalTo(false));
    }

    public void testRemoveNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        String processorTag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config);
        try {
            processor.execute(ingestDocument);
            fail("remove field should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config);
        processor.execute(ingestDocument);
    }
}
