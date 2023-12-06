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
package org.havenask.engine.search;

import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.test.HavenaskTestCase;

public class HavenaskSearchQueryPhaseResponseTests extends HavenaskTestCase {
    public void testGetCoveredPercent() {
        double delta = 0.00001;
        String result = "{\"total_time\":29.287,\"has_soft_failure\":false,\"covered_percent\":0.5,\"row_count\":5,"
            + "\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\",\"table_leader_info\":{},"
            + "\"table_build_watermark\":{},\"sql_query\":"
            + "\"query=select _id, _source from havenask_test_summary_ where _id in('1','3','2','1241dsf2fas','252fnwlekf1n') "
            + "limit 5&&kvpair=format:full_json;timeout:10000;databaseName:general\",\"iquan_plan\":{\"error_code\":0,"
            + "\"error_message\":\"\",\"result\":{\"rel_plan_version\":\"\",\"rel_plan\":[],\"exec_params\":{}}},"
            + "\"navi_graph\":\"\",\"trace\":[],\"sql_result\":{\"data\":[[\"1\",\"{ \\\"foo1\\\": \\\"1\\\" }\"],"
            + "[\"3\",\"{ \\\"foo1\\\": \\\"3\\\" }\"],[\"1241dsf2fas\",\"{ \\\"foo1\\\": \\\"55\\\" }\"],[\"2\",\""
            + "{ \\\"foo1\\\": \\\"2\\\" }\"],[\"252fnwlekf1n\",\"{ \\\"foo1\\\": \\\"66\\\" }\"]],\"column_name\":"
            + "[\"_id\",\"_source\"],\"column_type\":[\"multi_char\",\"multi_char\"]},\"error_info\":{\"ErrorCode\":0,"
            + "\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}";
        QrsSqlResponse qrsSqlResponse = new QrsSqlResponse(result, 200);
        HavenaskSearchQueryPhaseResponse havenaskSearchQueryPhaseResponse = new HavenaskSearchQueryPhaseResponse(qrsSqlResponse, null);
        double coveredPercent = havenaskSearchQueryPhaseResponse.getCoveredPercent();
        assertEquals(0.5, coveredPercent, delta);
    }
}
