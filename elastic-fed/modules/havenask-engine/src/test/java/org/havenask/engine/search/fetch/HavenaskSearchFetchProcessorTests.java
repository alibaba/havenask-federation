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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.lucene.search.TopDocsAndMaxScore;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.search.fetch.HavenaskFetchSubPhase.HitContent;
import org.havenask.engine.search.HavenaskSearchFetchProcessor;
import org.havenask.search.SearchHit;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.search.internal.InternalSearchResponse;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskSearchFetchProcessorTests extends HavenaskTestCase {
    private QrsClient qrsClient = mock(QrsClient.class);

    public void testGetQrsFetchPhaseSqlRequest() throws IOException {
        List<String> idList = List.of("1", "2", "3");
        String tableName = "table";
        QrsSqlRequest sqlRequest = HavenaskSearchFetchProcessor.getQrsFetchPhaseSqlRequest(idList, tableName);
        assertEquals(sqlRequest.getSql(), "select _id, _source, _routing from `table_summary_` where contain(_id,'1|2|3') limit 3");
    }

    public void testBuildQuerySearchResult() throws IOException {
        String sqlResponseStr = "{\"total_time\":8.126,\"has_soft_failure\":false,\"covered_percent\":1.0,"
            + "\"row_count\":4,\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\","
            + "\"table_leader_info\":{},\"table_build_watermark\":{},\"sql_query\":"
            + "\"query=select _id, vector_score('image') as _score from vector_test_0 where "
            + "MATCHINDEX('image', '1.1, 1.1') order by _score desc&&kvpair=format:full_json;databaseName:general"
            + "\",\"iquan_plan\":{\"error_code\":0,\"error_message\":\"\",\"result\":{\"rel_plan_version\":\"\","
            + "\"rel_plan\":[],\"exec_params\":{}}},\"navi_graph\":\"\",\"trace\":[],\"sql_result\":{\"data\":[[\"4"
            + "\",9.680000305175782],[\"3\",7.260000228881836],[\"2\",4.840000152587891],[\"1\",2.4200000762939455]],"
            + "\"column_name\":[\"_id\",\"_score\"],\"column_type\":[\"multi_char\",\"float\"]},\"error_info\":{"
            + "\"ErrorCode\":0,\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}";
        SqlResponse queryPhaseSqlResponse = SqlResponse.parse(sqlResponseStr);
        String[] resStr = new String[] { "4", "3", "2", "1" };
        float[] resFloat = new float[] { 9.6800F, 7.2600F, 4.8400F, 2.4200F };
        int rowNum = resStr.length;
        double delta = 0.0001;

        List<String> idList = new ArrayList<>(queryPhaseSqlResponse.getRowCount());
        HavenaskSearchFetchProcessor havenaskSearchFetchProcessor = new HavenaskSearchFetchProcessor(qrsClient);
        TopDocsAndMaxScore topDocsAndMaxScore = havenaskSearchFetchProcessor.buildQuerySearchResult(queryPhaseSqlResponse, idList, 0);
        assertEquals(4L, topDocsAndMaxScore.topDocs.totalHits.value);
        assertEquals(9.6800F, topDocsAndMaxScore.maxScore, delta);
        for (int i = 0; i < rowNum; i++) {
            assertEquals(resFloat[i], topDocsAndMaxScore.topDocs.scoreDocs[i].score, delta);
            assertEquals(-1, topDocsAndMaxScore.topDocs.scoreDocs[i].shardIndex);
            assertEquals(i, topDocsAndMaxScore.topDocs.scoreDocs[i].doc);
            assertEquals(resStr[i], idList.get(i));
        }
    }

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
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.fetchSource(fetchSourceContext);

            SearchHit searchHit = new SearchHit(-1);
            String sourceStr = "{\n" + "  \"key1\" :\"doc1\",\n" + "  \"name\" :\"alice\",\n" + "  \"length\":1\n" + "}\n";
            Object source = sourceStr;
            HitContent hit = new HitContent(searchHit, source);

            HavenaskFetchSubPhaseProcessor processor = new HavenaskFetchSourcePhase().getProcessor(indexName, searchSourceBuilder);
            if (processor != null) {
                processor.process(hit);
            } else {
                continue;
            }

            SearchHit res = hit.getHit();
            assertEquals(resSourceStr[i], res.getSourceAsString());
        }
    }

    public void testExecuteFetch() throws Exception {
        String indexName = "table";
        int docsNum = 3;
        String[] resStr = new String[] {
            "{\n" + "  \"_index\" : \"table\",\n" + "  \"_type\" : \"_doc\",\n" + "  \"_id\" : \"1\",\n" + "  \"_score\" : 1.0\n" + "}",
            "{\n" + "  \"_index\" : \"table\",\n" + "  \"_type\" : \"_doc\",\n" + "  \"_id\" : \"2\",\n" + "  \"_score\" : 1.0\n" + "}",
            "{\n" + "  \"_index\" : \"table\",\n" + "  \"_type\" : \"_doc\",\n" + "  \"_id\" : \"4\",\n" + "  \"_score\" : 1.0\n" + "}" };

        // mock SqlResponse
        Object[][] data = { { "1" }, { "2" }, { "4" } };
        SqlResponse sqlResponse = mock(SqlResponse.class);
        when(sqlResponse.getRowCount()).thenReturn(data.length);
        SqlResponse.SqlResult sqlResult = mock(SqlResponse.SqlResult.class);
        when(sqlResponse.getSqlResult()).thenReturn(sqlResult);
        when(sqlResult.getData()).thenReturn(data);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        HavenaskSearchFetchProcessor havenaskSearchFetchProcessor = new HavenaskSearchFetchProcessor(qrsClient);
        InternalSearchResponse res = havenaskSearchFetchProcessor.executeFetch(sqlResponse, indexName, searchSourceBuilder, false);
        for (int i = 0; i < docsNum; i++) {
            assertEquals(resStr[i], res.hits().getHits()[i].toString());
        }
    }

    public void testTransferSqlResponse2FetchResult() throws IOException {
        String indexName = "table";
        int docsNum = 4;
        int loadSize = 3;
        String[] resStr = new String[] {
            "{\n"
                + "  \"_index\" : \"table\",\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"4\",\n"
                + "  \"_score\" : 4.0,\n"
                + "  \"_routing\" : \"r4\",\n"
                + "  \"_source\" : {\n"
                + "    \"image\" : [\n"
                + "      4.1,\n"
                + "      4.1\n"
                + "    ]\n"
                + "  }\n"
                + "}",
            "{\n"
                + "  \"_index\" : \"table\",\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"3\",\n"
                + "  \"_score\" : 3.0,\n"
                + "  \"_source\" : {\n"
                + "    \"warn\" : \"source not found\"\n"
                + "  }\n"
                + "}",
            "{\n"
                + "  \"_index\" : \"table\",\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"2\",\n"
                + "  \"_score\" : 2.0,\n"
                + "  \"_source\" : {\n"
                + "    \"image\" : [\n"
                + "      2.1,\n"
                + "      2.1\n"
                + "    ]\n"
                + "  }\n"
                + "}",
            "{\n"
                + "  \"_index\" : \"table\",\n"
                + "  \"_type\" : \"_doc\",\n"
                + "  \"_id\" : \"1\",\n"
                + "  \"_score\" : 1.0,\n"
                + "  \"_source\" : {\n"
                + "    \"image\" : [\n"
                + "      1.1,\n"
                + "      1.1\n"
                + "    ]\n"
                + "  }\n"
                + "}" };

        // mock SqlResponse
        Object[][] data = {
            { "1", "{\n" + "  \"image\":[1.1, 1.1]\n" + "}\n", null },
            { "2", "{\n" + "  \"image\":[2.1, 2.1]\n" + "}", "2" },
            { "4", "{\n" + "  \"image\":[4.1, 4.1]\n" + "}\n", "r4" } };
        SqlResponse sqlResponse = mock(SqlResponse.class);
        when(sqlResponse.getRowCount()).thenReturn(data.length);
        SqlResponse.SqlResult sqlResult = mock(SqlResponse.SqlResult.class);
        when(sqlResponse.getSqlResult()).thenReturn(sqlResult);
        when(sqlResult.getData()).thenReturn(data);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        TopDocs topDocs = new TopDocs(
            new TotalHits(sqlResponse.getRowCount(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(0, 4), new ScoreDoc(1, 3), new ScoreDoc(2, 2), new ScoreDoc(3, 1) }
        );
        float maxScore = 4;
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);

        List<String> idList = new ArrayList<>(loadSize);
        for (int i = 0; i < docsNum; i++) {
            idList.add(String.valueOf(docsNum - i));
        }

        HavenaskSearchFetchProcessor havenaskSearchFetchProcessor = new HavenaskSearchFetchProcessor(qrsClient);
        InternalSearchResponse internalSearchResponse = havenaskSearchFetchProcessor.transferSqlResponse2FetchResult(
            indexName,
            idList,
            sqlResponse,
            topDocsAndMaxScore,
            searchSourceBuilder,
            true
        );
        for (int i = 0; i < loadSize; i++) {
            assertEquals(resStr[i], internalSearchResponse.hits().getHits()[i].toString());
        }
    }
}
