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

package org.havenask.engine.search.fetch;

import org.havenask.client.ha.SqlResponse;
import org.havenask.engine.search.HavenaskFetchPhase;
import org.havenask.engine.search.HavenaskFetchPhase.DocIdToIndex;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchHit;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.fetch.FetchSearchResult;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.engine.search.fetch.FetchSubPhase.HitContent;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskFetchPhaseTests extends HavenaskTestCase {
    public void testHitExecute() throws IOException {
        String indexName = "test";
        Boolean[] needFilter = new Boolean[] { true, true, true, true, false, true, true };
        String[][] includes = new String[][] {
            { "name" },
            {},
            { "key1", "length" },
            { "name", "length" },
            {},
            { "na*" },
            { "na*", "len*" } };
        String[][] excludes = new String[][] { {}, { "key1" }, {}, { "name" }, {}, {}, { "name" } };
        String[] resSourceStr = new String[] {
            "{\"name\":\"alice\"}",
            "{\"name\":\"alice\",\"length\":1}",
            "{\"key1\":\"doc1\",\"length\":1}",
            "{\"length\":1}",
            "",
            "{\"name\":\"alice\"}",
            "{\"length\":1}" };

        for (int i = 0; i < includes.length; i++) {
            FetchSourceContext fetchSourceContext = new FetchSourceContext(needFilter[i], includes[i], excludes[i]);

            SearchHit searchHit = new SearchHit(-1);
            String sourceStr = "{\n" + "  \"key1\" :\"doc1\",\n" + "  \"name\" :\"alice\",\n" + "  \"length\":1\n" + "}\n";
            Object source = sourceStr;
            HitContent hit = new HitContent(searchHit, source);

            // mock SearchContext
            SearchContext searchContext = mock(SearchContext.class);
            IndexShard indexShard = mock(IndexShard.class);
            ShardId shardId = mock(ShardId.class);
            when(indexShard.shardId()).thenReturn(shardId);
            when(shardId.getIndexName()).thenReturn(indexName);
            when(searchContext.indexShard()).thenReturn(indexShard);
            when(searchContext.fetchSourceContext()).thenReturn(fetchSourceContext);

            searchContext.fetchSourceContext(fetchSourceContext);
            FetchSubPhaseProcessor processor = new FetchSourcePhase().getProcessor(searchContext);
            if (processor != null) {
                processor.process(hit);
            } else {
                continue;
            }

            SearchHit res = hit.getHit();
            assertEquals(resSourceStr[i], res.getSourceAsString());
        }
    }

    public void testTransferSqlResponse2FetchResult() throws IOException {
        int docsNum = 4;
        int loadSize = 3;
        String[] resStr = new String[] {
            "{\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"4\",\n"
                + "  \"_score\" : null,\n"
                + "  \"_source\" : {\n"
                + "    \"image\" : [\n"
                + "      4.1,\n"
                + "      4.1\n"
                + "    ]\n"
                + "  }\n"
                + "}",
            "{\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"3\",\n"
                + "  \"_score\" : null,\n"
                + "  \"_source\" : {\n"
                + "    \"warn\" : \"source not found\"\n"
                + "  }\n"
                + "}",
            "{\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"2\",\n"
                + "  \"_score\" : null,\n"
                + "  \"_source\" : {\n"
                + "    \"image\" : [\n"
                + "      2.1,\n"
                + "      2.1\n"
                + "    ]\n"
                + "  }\n"
                + "}",
            "{\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"1\",\n"
                + "  \"_score\" : null,\n"
                + "  \"_source\" : {\n"
                + "    \"image\" : [\n"
                + "      1.1,\n"
                + "      1.1\n"
                + "    ]\n"
                + "  }\n"
                + "}" };

        // mock SearchContext
        SearchContext searchContext = mock(SearchContext.class);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        SearchShardTarget shardTarget = new SearchShardTarget(null, null, null, null);
        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.fetchResult()).thenReturn(fetchSearchResult);
        when(searchContext.docIdsToLoadSize()).thenReturn(loadSize);

        // mock SqlResponse
        Object[][] Data = {
            { "{\n" + "  \"image\":[1.1, 1.1]\n" + "}\n", "1" },
            { "{\n" + "  \"image\":[2.1, 2.1]\n" + "}", "2" },
            { "{\n" + "  \"image\":[4.1, 4.1]\n" + "}\n", "4" } };
        SqlResponse sqlResponse = mock(SqlResponse.class);
        when(sqlResponse.getRowCount()).thenReturn(Data.length);
        SqlResponse.SqlResult sqlResult = mock(SqlResponse.SqlResult.class);
        when(sqlResponse.getSqlResult()).thenReturn(sqlResult);
        when(sqlResult.getData()).thenReturn(Data);

        DocIdToIndex[] docs = new DocIdToIndex[docsNum];
        for (int i = 0; i < docsNum; i++) {
            docs[i] = new DocIdToIndex(i, i);
        }
        ArrayList<String> idList = new ArrayList<String>();
        for (int i = 0; i < docsNum; i++) {
            idList.add(String.valueOf(docsNum - i));
        }

        HavenaskFetchPhase havenaskFetchPhase = new HavenaskFetchPhase(null, new ArrayList<>());
        havenaskFetchPhase.transferSqlResponse2FetchResult(docs, idList, sqlResponse, searchContext);
        for (int i = 0; i < loadSize; i++) {
            assertEquals(resStr[i], fetchSearchResult.hits().getHits()[i].toString());
        }
    }
}
