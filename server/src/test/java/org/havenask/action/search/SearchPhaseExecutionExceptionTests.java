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

package org.havenask.action.search;

import org.havenask.HavenaskException;
import org.havenask.action.OriginalIndices;
import org.havenask.action.TimestampParsingException;
import org.havenask.common.ParsingException;
import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.util.concurrent.HavenaskRejectedExecutionException;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContent;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.shard.IndexShardClosedException;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.InvalidIndexTemplateException;
import org.havenask.rest.RestStatus;
import org.havenask.search.SearchShardTarget;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class SearchPhaseExecutionExceptionTests extends HavenaskTestCase {

    public void testToXContent() throws IOException {
        SearchPhaseExecutionException exception = new SearchPhaseExecutionException("test", "all shards failed",
                new ShardSearchFailure[]{
                        new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                                new SearchShardTarget("node_1", new ShardId("foo", "_na_", 0), null, OriginalIndices.NONE)),
                        new ShardSearchFailure(new IndexShardClosedException(new ShardId("foo", "_na_", 1)),
                                new SearchShardTarget("node_2", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE)),
                        new ShardSearchFailure(new ParsingException(5, 7, "foobar", null),
                                new SearchShardTarget("node_3", new ShardId("foo", "_na_", 2), null, OriginalIndices.NONE)),
                });

        // Failures are grouped (by default)
        final String expectedJson = XContentHelper.stripWhitespace(
            "{"
                + "  \"type\": \"search_phase_execution_exception\","
                + "  \"reason\": \"all shards failed\","
                + "  \"phase\": \"test\","
                + "  \"grouped\": true,"
                + "  \"failed_shards\": ["
                + "    {"
                + "      \"shard\": 0,"
                + "      \"index\": \"foo\","
                + "      \"node\": \"node_1\","
                + "      \"reason\": {"
                + "        \"type\": \"parsing_exception\","
                + "        \"reason\": \"foobar\","
                + "        \"line\": 1,"
                + "        \"col\": 2"
                + "      }"
                + "    },"
                + "    {"
                + "      \"shard\": 1,"
                + "      \"index\": \"foo\","
                + "      \"node\": \"node_2\","
                + "      \"reason\": {"
                + "        \"type\": \"index_shard_closed_exception\","
                + "        \"reason\": \"CurrentState[CLOSED] Closed\","
                + "        \"index\": \"foo\","
                + "        \"index_uuid\": \"_na_\","
                + "        \"shard\": \"1\""
                + "      }"
                + "    }"
                + "  ]"
                + "}"
        );
        assertEquals(expectedJson, Strings.toString(exception));
    }

    public void testToAndFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[randomIntBetween(1, 5)];
        for (int i = 0; i < shardSearchFailures.length; i++) {
            Exception cause = randomFrom(
                    new ParsingException(1, 2, "foobar", null),
                    new InvalidIndexTemplateException("foo", "bar"),
                    new TimestampParsingException("foo", null),
                    new NullPointerException()
            );
            shardSearchFailures[i] = new  ShardSearchFailure(cause, new SearchShardTarget("node_" + i,
                new ShardId("test", "_na_", i), null, OriginalIndices.NONE));
        }

        final String phase = randomFrom("query", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures", shardSearchFailures);

        BytesReference exceptionBytes = toShuffledXContent(actual, xContent.type(), ToXContent.EMPTY_PARAMS, randomBoolean());

        HavenaskException parsedException;
        try (XContentParser parser = createParser(xContent, exceptionBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = HavenaskException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsedException);
        assertThat(parsedException.getHeaderKeys(), hasSize(0));
        assertThat(parsedException.getMetadataKeys(), hasSize(1));
        assertThat(parsedException.getMetadata("havenask.phase"), hasItem(phase));
        // SearchPhaseExecutionException has no cause field
        assertNull(parsedException.getCause());
    }

    public void testPhaseFailureWithoutSearchShardFailure() {
        final ShardSearchFailure[] searchShardFailures = new ShardSearchFailure[0];
        final String phase = randomFrom("fetch", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures",
            new HavenaskRejectedExecutionException("Havenask rejected execution of fetch phase"), searchShardFailures);

        assertEquals(actual.status(), RestStatus.TOO_MANY_REQUESTS);
    }

    public void testPhaseFailureWithoutSearchShardFailureAndCause() {
        final ShardSearchFailure[] searchShardFailures = new ShardSearchFailure[0];
        final String phase = randomFrom("fetch", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures", null, searchShardFailures);

        assertEquals(actual.status(), RestStatus.SERVICE_UNAVAILABLE);
    }

    public void testPhaseFailureWithSearchShardFailure() {
        final ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[randomIntBetween(1, 5)];
        for (int i = 0; i < shardSearchFailures.length; i++) {
            Exception cause = randomFrom(
                new ParsingException(1, 2, "foobar", null),
                new InvalidIndexTemplateException("foo", "bar")
            );
            shardSearchFailures[i] = new ShardSearchFailure(cause, new SearchShardTarget("node_" + i,
                new ShardId("test", "_na_", i), null, OriginalIndices.NONE));
        }

        final String phase = randomFrom("fetch", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures",
            new HavenaskRejectedExecutionException("Havenask rejected execution of fetch phase"), shardSearchFailures);

        assertEquals(actual.status(), RestStatus.BAD_REQUEST);
    }
}
