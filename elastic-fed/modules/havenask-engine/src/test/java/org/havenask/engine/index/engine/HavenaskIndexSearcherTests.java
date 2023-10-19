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

import org.havenask.search.internal.ReaderContext;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.test.HavenaskTestCase;
import java.util.UUID;

import org.apache.lucene.search.QueryCachingPolicy;
import org.havenask.common.UUIDs;
import org.havenask.index.IndexService;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.search.internal.ShardSearchContextId;
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
            String sqlResponseStr1 = "{\"total_time\":8.126,\"has_soft_failure\":false,\"covered_percent\":1.0,"
                + "\"row_count\":4,\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\","
                + "\"table_leader_info\":{},\"table_build_watermark\":{},\"sql_query\":"
                + "\"query=select _id, vectorscore('image') as _score from vector_test_0 where "
                + "MATCHINDEX('image', '1.1, 1.1') order by _score desc&&kvpair=format:full_json;databaseName:general"
                + "\",\"iquan_plan\":{\"error_code\":0,\"error_message\":\"\",\"result\":{\"rel_plan_version\":\"\","
                + "\"rel_plan\":[],\"exec_params\":{}}},\"navi_graph\":\"\",\"trace\":[],\"sql_result\":{\"data\":[[\"4"
                + "\",9.680000305175782],[\"3\",7.260000228881836],[\"2\",4.840000152587891],[\"1\",2.4200000762939455]],"
                + "\"column_name\":[\"_id\",\"_score\"],\"column_type\":[\"multi_char\",\"float\"]},\"error_info\":{"
                + "\"ErrorCode\":0,\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}";

            String[] resStr1 = new String[] { "4", "3", "2", "1" };
            float[] resFloat1 = new float[] { 9.6800F, 7.2600F, 4.8400F, 2.4200F };
            int rowNum1 = resStr1.length;
            HavenaskIndexSearcher.buildQuerySearchResult(querySearchResult, sqlResponseStr1, readerContext);
            assertEquals(4L, querySearchResult.topDocs().topDocs.totalHits.value);
            assertEquals(9.6800F, querySearchResult.getMaxScore(), delta);
            List<String> ids1 = readerContext.getFromContext(IDS_CONTEXT);
            for (int i = 0; i < rowNum1; i++) {
                assertEquals(resFloat1[i], querySearchResult.topDocs().topDocs.scoreDocs[i].score, delta);
                assertEquals(-1, querySearchResult.topDocs().topDocs.scoreDocs[i].shardIndex);
                assertEquals(i, querySearchResult.topDocs().topDocs.scoreDocs[i].doc);
                assertEquals(resStr1[i], ids1.get(i));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    private ShardSearchContextId newContextId() {
        return new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
    }
}
