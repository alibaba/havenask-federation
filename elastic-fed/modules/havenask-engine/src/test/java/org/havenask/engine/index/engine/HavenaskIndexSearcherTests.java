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
import java.util.ArrayList;
import java.util.List;

import org.havenask.search.internal.ReaderContext;
import org.havenask.search.query.QuerySearchResult;
import org.havenask.test.HavenaskTestCase;
import org.mockito.Mockito;

public class HavenaskIndexSearcherTests extends HavenaskTestCase {
    public static final String IDS_CONTEXT = "havenask_ids";

    public void testBuildQuerySearchResult() throws IOException {
        ReaderContext readerContext1 = Mockito.mock(ReaderContext.class);
        ReaderContext readerContext2 = Mockito.mock(ReaderContext.class);

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
        List<String> readerContextIds1 = new ArrayList<>(resStr1.length);
        for (int i = 0; i < resStr1.length; i++) {
            readerContextIds1.add(resStr1[i]);
        }
        Mockito.doNothing().when(readerContext1).putInContext(Mockito.any(String.class), Mockito.any());
        Mockito.when(readerContext1.getFromContext(Mockito.eq(IDS_CONTEXT))).thenReturn(readerContextIds1);
        float[] resFloat1 = new float[] { 3.0F, 2.0F, 1.0F };
        int rowNum1 = 3;
        HavenaskIndexSearcher.buildQuerySearchResult(querySearchResult, sqlResponseStr1, readerContext1);
        assertEquals(3L, querySearchResult.topDocs().topDocs.totalHits.value);
        assertEquals(3.0F, querySearchResult.getMaxScore(), delta);
        List<String> ids1 = readerContext1.getFromContext(IDS_CONTEXT);
        for (int i = 0; i < rowNum1; i++) {
            assertEquals(resFloat1[i], querySearchResult.topDocs().topDocs.scoreDocs[i].score, delta);
            assertEquals(-1, querySearchResult.topDocs().topDocs.scoreDocs[i].shardIndex);
            assertEquals(i, querySearchResult.topDocs().topDocs.scoreDocs[i].doc);
            assertEquals(resStr1[i], ids1.get(i));
        }

        String[] resStr2 = new String[] { "qwerty", "asdfgh", "zxcvbn", "yuiopl" };
        List<String> readerContextIds2 = new ArrayList<>(resStr2.length);
        for (int i = 0; i < resStr2.length; i++) {
            readerContextIds2.add(resStr2[i]);
        }
        Mockito.doNothing().when(readerContext2).putInContext(Mockito.any(String.class), Mockito.any());
        Mockito.when(readerContext2.getFromContext(Mockito.eq(IDS_CONTEXT))).thenReturn(readerContextIds2);

        float[] resFloat2 = new float[] { 4.0F, 3.0F, 2.0F, 1.0F };
        int rowNum2 = 4;
        HavenaskIndexSearcher.buildQuerySearchResult(querySearchResult, sqlResponseStr2, readerContext2);
        List<String> ids2 = readerContext2.getFromContext(IDS_CONTEXT);
        assertEquals(4L, querySearchResult.topDocs().topDocs.totalHits.value);
        assertEquals(4.0F, querySearchResult.getMaxScore(), delta);
        for (int i = 0; i < rowNum2; i++) {
            assertEquals(resFloat2[i], querySearchResult.topDocs().topDocs.scoreDocs[i].score, delta);
            assertEquals(-1, querySearchResult.topDocs().topDocs.scoreDocs[i].shardIndex);
            assertEquals(i, querySearchResult.topDocs().topDocs.scoreDocs[i].doc);
            assertEquals(resStr2[i], ids2.get(i));
        }
    }
}
