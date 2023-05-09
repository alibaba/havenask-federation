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

package org.havenask.search.query;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.OriginalIndices;
import org.havenask.action.OriginalIndicesTests;
import org.havenask.action.search.SearchRequest;
import org.havenask.common.Strings;
import org.havenask.common.UUIDs;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.lucene.search.TopDocsAndMaxScore;
import org.havenask.common.settings.Settings;
import org.havenask.index.shard.ShardId;
import org.havenask.search.DocValueFormat;
import org.havenask.search.SearchModule;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.aggregations.Aggregations;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.aggregations.InternalAggregationsTests;
import org.havenask.search.internal.AliasFilter;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.search.internal.ShardSearchRequest;
import org.havenask.search.suggest.SuggestTests;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.Base64;

import static java.util.Collections.emptyList;

public class QuerySearchResultTests extends HavenaskTestCase {

    private final NamedWriteableRegistry namedWriteableRegistry;

    public QuerySearchResultTests() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        this.namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    private static QuerySearchResult createTestInstance() throws Exception {
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(OriginalIndicesTests.randomOriginalIndices(), searchRequest,
            shardId, 1, new AliasFilter(null, Strings.EMPTY_ARRAY), 1.0f, randomNonNegativeLong(), null, new String[0]);
        QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId(UUIDs.base64UUID(), randomLong()),
            new SearchShardTarget("node", shardId, null, OriginalIndices.NONE), shardSearchRequest);
        if (randomBoolean()) {
            result.terminatedEarly(randomBoolean());
        }
        TopDocs topDocs = new TopDocs(new TotalHits(randomLongBetween(0, Long.MAX_VALUE), TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        result.topDocs(new TopDocsAndMaxScore(topDocs, randomBoolean() ? Float.NaN : randomFloat()), new DocValueFormat[0]);
        result.size(randomInt());
        result.from(randomInt());
        if (randomBoolean()) {
            result.suggest(SuggestTests.createTestItem());
        }
        if (randomBoolean()) {
            result.aggregations(InternalAggregationsTests.createTestInstance());
        }
        return result;
    }

    public void testSerialization() throws Exception {
        QuerySearchResult querySearchResult = createTestInstance();
        QuerySearchResult deserialized = copyWriteable(querySearchResult, namedWriteableRegistry, QuerySearchResult::new);
        assertEquals(querySearchResult.getContextId().getId(), deserialized.getContextId().getId());
        assertNull(deserialized.getSearchShardTarget());
        assertEquals(querySearchResult.topDocs().maxScore, deserialized.topDocs().maxScore, 0f);
        assertEquals(querySearchResult.topDocs().topDocs.totalHits, deserialized.topDocs().topDocs.totalHits);
        assertEquals(querySearchResult.from(), deserialized.from());
        assertEquals(querySearchResult.size(), deserialized.size());
        assertEquals(querySearchResult.hasAggs(), deserialized.hasAggs());
        if (deserialized.hasAggs()) {
            Aggregations aggs = querySearchResult.consumeAggs().expand();
            Aggregations deserializedAggs = deserialized.consumeAggs().expand();
            assertEquals(aggs.asList(), deserializedAggs.asList());
        }
        assertEquals(querySearchResult.terminatedEarly(), deserialized.terminatedEarly());
    }

    public void testReadFromPre_7_1_0() throws IOException {
        String message = "AAAAAAAAAGQAAAEAAAB/wAAAAAEBBnN0ZXJtcwVJblhNRgoDBVNhdWpvAAVrS3l3cwVHSVVZaAAFZXRUbEUFZGN0WVoABXhzYnVrAAEDAfoN" +
            "A3JhdwUBAAJRAAAAAAAAA30DBnN0ZXJtcwVNdVVFRwoAAAEDAfoNA3JhdwUBAAdDAAAAAAAAA30AAApQVkFhaUxSdHh5TAAAAAAAAAN9AAAKTVRUeUxnd1hyd" +
            "y0AAAAAAAADfQAACnZRQXZ3cWp0SmwPAAAAAAAAA30AAApmYXNyUUhNVWZBCwAAAAAAAAN9AAAKT3FIQ2RMZ1JZUwUAAAAAAAADfQAACm9jT05aZmZ4ZmUmAA" +
            "AAAAAAA30AAApvb0tJTkdvbHdzBnN0ZXJtcwVtRmlmZAoAAAEDAfoNA3JhdwUBAARXAAAAAAAAA30AAApZd3BwQlpBZEhpMQAAAAAAAAN9AAAKREZ3UVpTSXh" +
            "DSE4AAAAAAAADfQAAClVMZW1YZGtkSHUUAAAAAAAAA30AAApBUVdKVk1kTlF1BnN0ZXJtcwVxbkJGVgoAAAEDAfoNA3JhdwUBAAYJAAAAAAAAA30AAApBS2NL" +
            "U1ZVS25EIQAAAAAAAAN9AAAKWGpCbXZBZmduRhsAAAAAAAADfQAACk54TkJEV3pLRmI7AAAAAAAAA30AAApydkdaZnJycXhWSAAAAAAAAAN9AAAKSURVZ3JhQ" +
            "lFHSy4AAAAAAAADfQAACmJmZ0x5YlFlVksAClRJZHJlSkpVc1Y4AAAAAAAAA30DBnN0ZXJtcwVNdVVFRwoAAAEDAfoNA3JhdwUBAAdDAAAAAAAAA30AAApQVk" +
            "FhaUxSdHh5TAAAAAAAAAN9AAAKTVRUeUxnd1hydy0AAAAAAAADfQAACnZRQXZ3cWp0SmwPAAAAAAAAA30AAApmYXNyUUhNVWZBCwAAAAAAAAN9AAAKT3FIQ2R" +
            "MZ1JZUwUAAAAAAAADfQAACm9jT05aZmZ4ZmUmAAAAAAAAA30AAApvb0tJTkdvbHdzBnN0ZXJtcwVtRmlmZAoAAAEDAfoNA3JhdwUBAARXAAAAAAAAA30AAApZ" +
            "d3BwQlpBZEhpMQAAAAAAAAN9AAAKREZ3UVpTSXhDSE4AAAAAAAADfQAAClVMZW1YZGtkSHUUAAAAAAAAA30AAApBUVdKVk1kTlF1BnN0ZXJtcwVxbkJGVgoAA" +
            "AEDAfoNA3JhdwUBAAYJAAAAAAAAA30AAApBS2NLU1ZVS25EIQAAAAAAAAN9AAAKWGpCbXZBZmduRhsAAAAAAAADfQAACk54TkJEV3pLRmI7AAAAAAAAA30AAA" +
            "pydkdaZnJycXhWSAAAAAAAAAN9AAAKSURVZ3JhQlFHSy4AAAAAAAADfQAACmJmZ0x5YlFlVksACm5rdExLUHp3cGgBCm1heF9idWNrZXQFbmFtZTEBB2J1Y2t" +
            "ldDH/A3JhdwEBCm1heF9idWNrZXQFbmFtZTEBB2J1Y2tldDH/A3JhdwEAAAIAAf////8AAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        byte[] bytes = Base64.getDecoder().decode(message);
        try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), namedWriteableRegistry)) {
            in.setVersion(LegacyESVersion.V_7_0_0);
            QuerySearchResult querySearchResult = new QuerySearchResult(in);
            assertEquals(100, querySearchResult.getContextId().getId());
            assertTrue(querySearchResult.hasAggs());
            InternalAggregations aggs = querySearchResult.consumeAggs().expand();
            assertEquals(1, aggs.asList().size());
            // We deserialize and throw away top level pipeline aggs
        }
    }

    public void testNullResponse() throws Exception {
        QuerySearchResult querySearchResult = QuerySearchResult.nullInstance();
        QuerySearchResult deserialized =
            copyWriteable(querySearchResult, namedWriteableRegistry, QuerySearchResult::new, Version.CURRENT);
        assertEquals(querySearchResult.isNull(), deserialized.isNull());
    }
}
