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

package org.havenask.action.bulk;

import org.havenask.action.index.IndexRequest;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.xcontent.XContentType;
import org.havenask.rest.action.document.RestBulkAction;
import org.havenask.test.HavenaskTestCase;
import org.hamcrest.Matchers;
import org.havenask.action.bulk.BulkRequestParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkRequestParserTests extends HavenaskTestCase {

    public void testIndexRequest() throws IOException {
        BytesArray request = new BytesArray("{ \"index\":{ \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                indexRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", indexRequest.index());
                    assertEquals("bar", indexRequest.id());
                    assertFalse(indexRequest.isRequireAlias());
                    parsed.set(true);
                },
                req -> fail(), req -> fail());
        assertTrue(parsed.get());

        parser.parse(request, "foo", null, null, null, true, false, XContentType.JSON,
            indexRequest -> {
                assertTrue(indexRequest.isRequireAlias());
            },
            req -> fail(), req -> fail());

        request = new BytesArray("{ \"index\":{ \"_id\": \"bar\", \"require_alias\": true } }\n{}\n");
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
            indexRequest -> {
                assertTrue(indexRequest.isRequireAlias());
            },
            req -> fail(), req -> fail());

        request = new BytesArray("{ \"index\":{ \"_id\": \"bar\", \"require_alias\": false } }\n{}\n");
        parser.parse(request, "foo", null, null, null, true, false, XContentType.JSON,
            indexRequest -> {
                assertFalse(indexRequest.isRequireAlias());
            },
            req -> fail(), req -> fail());
    }

    public void testDeleteRequest() throws IOException {
        BytesArray request = new BytesArray("{ \"delete\":{ \"_id\": \"bar\" } }\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                req -> fail(), req -> fail(),
                deleteRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", deleteRequest.index());
                    assertEquals("bar", deleteRequest.id());
                    parsed.set(true);
                });
        assertTrue(parsed.get());
    }

    public void testUpdateRequest() throws IOException {
        BytesArray request = new BytesArray("{ \"update\":{ \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                req -> fail(),
                updateRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", updateRequest.index());
                    assertEquals("bar", updateRequest.id());
                    assertFalse(updateRequest.isRequireAlias());
                    parsed.set(true);
                },
                req -> fail());
        assertTrue(parsed.get());

        parser.parse(request, "foo", null, null, null, true, false, XContentType.JSON,
            req -> fail(),
            updateRequest -> {
                assertTrue(updateRequest.isRequireAlias());
            },
            req -> fail());

        request = new BytesArray("{ \"update\":{ \"_id\": \"bar\", \"require_alias\": true } }\n{}\n");
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
            req -> fail(),
            updateRequest -> {
                assertTrue(updateRequest.isRequireAlias());
            },
            req -> fail());

        request = new BytesArray("{ \"update\":{ \"_id\": \"bar\", \"require_alias\": false } }\n{}\n");
        parser.parse(request, "foo", null, null, null, true, false, XContentType.JSON,
            req -> fail(),
            updateRequest -> {
                assertFalse(updateRequest.isRequireAlias());
            },
            req -> fail());
    }

    public void testBarfOnLackOfTrailingNewline() {
        BytesArray request = new BytesArray("{ \"index\":{ \"_id\": \"bar\" } }\n{}");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                        indexRequest -> fail(), req -> fail(), req -> fail()));
        assertEquals("The bulk request must be terminated by a newline [\\n]", e.getMessage());
    }

    public void testFailOnExplicitIndex() {
        BytesArray request = new BytesArray("{ \"index\":{ \"_index\": \"foo\", \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> parser.parse(request, null, null, null, null, null, false, XContentType.JSON,
                        req -> fail(), req -> fail(), req -> fail()));
        assertEquals("explicit index in bulk is not allowed", ex.getMessage());
    }

    public void testTypeWarning() throws IOException {
        BytesArray request = new BytesArray("{ \"index\":{ \"_type\": \"quux\", \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(true);
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                indexRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", indexRequest.index());
                    assertEquals("bar", indexRequest.id());
                    parsed.set(true);
                },
                req -> fail(), req -> fail());
        assertTrue(parsed.get());

        assertWarnings(RestBulkAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testParseDeduplicatesParameterStrings() throws IOException {
        BytesArray request = new BytesArray(
                "{ \"index\":{ \"_index\": \"bar\", \"pipeline\": \"foo\", \"routing\": \"blub\"} }\n{}\n"
                + "{ \"index\":{ \"_index\": \"bar\", \"pipeline\": \"foo\", \"routing\": \"blub\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        final List<IndexRequest> indexRequests = new ArrayList<>();
        parser.parse(request, null, null, null, null, null, true, XContentType.JSON,
                indexRequests::add,
                req -> fail(), req -> fail());
        assertThat(indexRequests, Matchers.hasSize(2));
        final IndexRequest first = indexRequests.get(0);
        final IndexRequest second = indexRequests.get(1);
        assertSame(first.index(), second.index());
        assertSame(first.getPipeline(), second.getPipeline());
        assertSame(first.routing(), second.routing());
    }
}
