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
import java.util.List;
import java.util.UUID;

import org.apache.lucene.search.QueryCachingPolicy;
import org.havenask.common.UUIDs;
import org.havenask.index.IndexService;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.search.internal.ReaderContext;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskIndexSearcherTests extends HavenaskTestCase {
    public static final String IDS_CONTEXT = "havenask_ids";

    public void testBuildQuerySearchResult() throws IOException {
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        try {
            IndexService indexService = mock(IndexService.class);
            QueryShardContext queryShardContext = mock(QueryShardContext.class);
            when(indexService.newQueryShardContext(eq(shardId.id()), anyObject(), anyObject(), anyString())).thenReturn(queryShardContext);

            ReaderContext readerContext = new ReaderContext(newContextId(), indexService, indexShard, null, randomNonNegativeLong(), false);

            double delta = 0.0001;
            QuerySearchResult querySearchResult = new QuerySearchResult();
            String sqlResponseStr1 = "{\"total_time\":2.016,\"has_soft_failure\":false,\"covered_percent\":1.0,"
                + "\"row_count\":3,\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\","
                + "\"table_leader_info\":{},\"table_build_watermark\":{},\"sql_query\":\"select _id from "
                + "in1&&kvpair=databaseName:database;formatType:full_json\",\"iquan_plan\":{\"error_code\":0,"
                + "\"error_message\":\"\",\"result\":{\"rel_plan_version\":\"\",\"rel_plan\":[],"
                + "\"exec_params\":{}}},\"navi_graph\":\"\",\"trace\":[],\"sql_result\":{\"data\":[[\"wRSgaYoBtIvm0jEE9eGc\"],"
                + "[\"wBSgaYoBtIvm0jEE9OG2\"],[\"shRlY4oBtIvm0jEEEOFm\"]],\"column_name\":[\"_id\"],"
                + "\"column_type\":[\"multi_char\"]},"
                + "\"error_info\":{\"ErrorCode\":0,\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}  ";

            String sqlResponseStr2 = "{\"total_time\":1.811,\"has_soft_failure\":false,\"covered_percent\":1.0,"
                + "\"row_count\":4,\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\","
                + "\"table_leader_info\":{},\"table_build_watermark\":{},"
                + "\"sql_query\":\"select _id from idtable&&kvpair=databaseName:database;formatType:full_json\","
                + "\"iquan_plan\":{\"error_code\":0,\"error_message\":\"\",\"result\":{\"rel_plan_version\":\"\","
                + "\"rel_plan\":[],\"exec_params\":{}}},\"navi_graph\":\"\",\"trace\":[],"
                + "\"sql_result\":{\"data\":[[\"qwerty\"],[\"asdfgh\"],[\"zxcvbn\"],[\"yuiopl\"]],"
                + "\"column_name\":[\"_id\"],\"column_type\":[\"multi_char\"]},"
                + "\"error_info\":{\"ErrorCode\":0,\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}   ";

            String[] resStr1 = new String[] { "wRSgaYoBtIvm0jEE9eGc", "wBSgaYoBtIvm0jEE9OG2", "shRlY4oBtIvm0jEEEOFm" };
            float[] resFloat1 = new float[] { 3.0F, 2.0F, 1.0F };
            int rowNum1 = 3;
            HavenaskIndexSearcher.buildQuerySearchResult(querySearchResult, sqlResponseStr1, readerContext);
            assertEquals(3L, querySearchResult.topDocs().topDocs.totalHits.value);
            assertEquals(3.0F, querySearchResult.getMaxScore(), delta);
            List<String> ids1 = readerContext.getFromContext(IDS_CONTEXT);
            for (int i = 0; i < rowNum1; i++) {
                assertEquals(resFloat1[i], querySearchResult.topDocs().topDocs.scoreDocs[i].score, delta);
                assertEquals(-1, querySearchResult.topDocs().topDocs.scoreDocs[i].shardIndex);
                assertEquals(i, querySearchResult.topDocs().topDocs.scoreDocs[i].doc);
                assertEquals(resStr1[i], ids1.get(i));
            }

            String[] resStr2 = new String[] { "qwerty", "asdfgh", "zxcvbn", "yuiopl" };
            float[] resFloat2 = new float[] { 4.0F, 3.0F, 2.0F, 1.0F };
            int rowNum2 = 4;
            HavenaskIndexSearcher.buildQuerySearchResult(querySearchResult, sqlResponseStr2, readerContext);
            List<String> ids2 = readerContext.getFromContext(IDS_CONTEXT);
            assertEquals(4L, querySearchResult.topDocs().topDocs.totalHits.value);
            assertEquals(4.0F, querySearchResult.getMaxScore(), delta);
            for (int i = 0; i < rowNum2; i++) {
                assertEquals(resFloat2[i], querySearchResult.topDocs().topDocs.scoreDocs[i].score, delta);
                assertEquals(-1, querySearchResult.topDocs().topDocs.scoreDocs[i].shardIndex);
                assertEquals(i, querySearchResult.topDocs().topDocs.scoreDocs[i].doc);
                assertEquals(resStr2[i], ids2.get(i));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    private ShardSearchContextId newContextId() {
        return new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
    }
}
