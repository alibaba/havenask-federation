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

package org.havenask.engine;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

import com.sun.net.httpserver.HttpServer;
import org.havenask.common.SuppressForbidden;
import org.havenask.test.HavenaskIntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@SuppressForbidden(reason = "use a http server")
public abstract class HavenaskITTestCase extends HavenaskIntegTestCase {
    protected static HttpServer qrsServer;
    protected static HttpServer searcherServer;

    @BeforeClass
    public static void setup() throws IOException {
        if (qrsServer == null) {
            qrsServer = HttpServer.create(new InetSocketAddress(49200), 0);
            qrsServer.createContext("/sql", exchange -> {
                exchange.sendResponseHeaders(200, 0);
                URI uri = exchange.getRequestURI();
                String query = uri.getQuery();
                String strResponse;
                if (query.contains("select count(*) from ")) {
                    strResponse = "{\"total_time\":2.757,\"has_soft_failure\":false,\"covered_percent\":1.0,"
                        + "\"row_count\":1,\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\","
                        + "\"table_leader_info\":{},\"table_build_watermark\":{},\"sql_query\":\"query=select count"
                        + "(*) from test&&kvpair=databaseName:general;format:full_json\","
                        + "\"iquan_plan\":{\"error_code\":0,\"error_message\":\"\","
                        + "\"result\":{\"rel_plan_version\":\"\",\"rel_plan\":[],\"exec_params\":{}}},"
                        + "\"navi_graph\":\"\",\"trace\":[],\"sql_result\":{\"data\":[[19]],\"column_name\":[\"COUNT"
                        + "(*)\"],\"column_type\":[\"int64\"]},\"error_info\":{\"ErrorCode\":0,"
                        + "\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}";
                } else if (query.contains("select _routing,_seq_no,_primary_term,_version,_source from")) {
                    strResponse = "{\"total_time\":9.449,\"has_soft_failure\":false,\"covered_percent\":1.0,"
                        + "\"row_count\":1,\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\","
                        + "\"table_leader_info\":{},\"table_build_watermark\":{},\"sql_query\":\"query=select "
                        + "_routing,_seq_no,_primary_term,_version,_source from test_summary_ where "
                        + "_id='nBRaY4oBtIvm0jEEpeG8'&&kvpair=databaseName:general;format:full_json\","
                        + "\"iquan_plan\":{\"error_code\":0,\"error_message\":\"\","
                        + "\"result\":{\"rel_plan_version\":\"\",\"rel_plan\":[],\"exec_params\":{}}},"
                        + "\"navi_graph\":\"\",\"trace\":[],\"sql_result\":{\"data\":[[\"\",10,1,1,\"\"\"{\n"
                        + "  \"foo\" : \"test2\",\n"
                        + "  \"vec\" : [1,2,3,4]\n"
                        + "}\n"
                        + "\"\"\"]],\"column_name\":[\"_routing\",\"_seq_no\",\"_primary_term\",\"_version\","
                        + "\"_source\"],\"column_type\":[\"multi_char\",\"int64\",\"int64\",\"int64\","
                        + "\"multi_char\"]},\"error_info\":{\"ErrorCode\":0,\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}";
                } else if (query.contains("MATCHINDEX")) {
                    strResponse = "{\n" +
                            "    \"total_time\": 3.771,\n" +
                            "    \"has_soft_failure\": false,\n" +
                            "    \"covered_percent\": 1.0,\n" +
                            "    \"row_count\": 2,\n" +
                            "    \"format_type\": \"full_json\",\n" +
                            "    \"search_info\": {},\n" +
                            "    \"rpc_info\": \"\",\n" +
                            "    \"table_leader_info\": {\n" +
                            "        \"knn_fetch_test\": true\n" +
                            "    },\n" +
                            "    \"table_build_watermark\": {},\n" +
                            "    \"sql_query\": \"query=select _id from knn_fetch_test where _id " +
                            "in('0QZYiYoBGeSNvliAKP4J', '0gZYiYoBGeSNvliAof4t')&&kvpair=format:full_json;databaseName:general\",\n" +
                            "    \"iquan_plan\": {\n" +
                            "        \"error_code\": 0,\n" +
                            "        \"error_message\": \"\",\n" +
                            "        \"result\": {\n" +
                            "            \"rel_plan_version\": \"\",\n" +
                            "            \"rel_plan\": [],\n" +
                            "            \"exec_params\": {}\n" +
                            "        }\n" +
                            "    },\n" +
                            "    \"navi_graph\": \"\",\n" +
                            "    \"trace\": [],\n" +
                            "    \"sql_result\": {\n" +
                            "        \"data\": [\n" +
                            "            [\n" +
                            "                \"0QZYiYoBGeSNvliAKP4J\"\n" +
                            "            ],\n" +
                            "            [\n" +
                            "                \"0gZYiYoBGeSNvliAof4t\"\n" +
                            "            ]\n" +
                            "        ],\n" +
                            "        \"column_name\": [\n" +
                            "            \"_id\"\n" +
                            "        ],\n" +
                            "        \"column_type\": [\n" +
                            "            \"multi_char\"\n" +
                            "        ]\n" +
                            "    },\n" +
                            "    \"error_info\": {\n" +
                            "        \"ErrorCode\": 0,\n" +
                            "        \"Error\": \"ERROR_NONE\",\n" +
                            "        \"Message\": \"\"\n" +
                            "    }\n" +
                            "}";
                } else if (query.contains("select _source,_id from")) {
                    strResponse = "{\n" +
                            "    \"total_time\": 3.979,\n" +
                            "    \"has_soft_failure\": false,\n" +
                            "    \"covered_percent\": 1.0,\n" +
                            "    \"row_count\": 2,\n" +
                            "    \"format_type\": \"full_json\",\n" +
                            "    \"search_info\": {},\n" +
                            "    \"rpc_info\": \"\",\n" +
                            "    \"table_leader_info\": {\n" +
                            "        \"knn_fetch_test\": true\n" +
                            "    },\n" +
                            "    \"table_build_watermark\": {},\n" +
                            "    \"sql_query\": \"query=select _source,_id,_routing from knn_fetch_test_summary_ where _id" +
                            " in('0QZYiYoBGeSNvliAKP4J', '0gZYiYoBGeSNvliAof4t')&&kvpair=format:full_json;databaseName:general\",\n" +
                            "    \"iquan_plan\": {\n" +
                            "        \"error_code\": 0,\n" +
                            "        \"error_message\": \"\",\n" +
                            "        \"result\": {\n" +
                            "            \"rel_plan_version\": \"\",\n" +
                            "            \"rel_plan\": [],\n" +
                            "            \"exec_params\": {}\n" +
                            "        }\n" +
                            "    },\n" +
                            "    \"navi_graph\": \"\",\n" +
                            "    \"trace\": [],\n" +
                            "    \"sql_result\": {\n" +
                            "        \"data\": [\n" +
                            "            [\n" +
                            "                \"{\\n  \\\"key1\\\" :\\\"doc1\\\",\\n  \\\"name\\\" :\\\"alice\\\",\\n  \\\"length\\\":1\\n}\\n\",\n" +
                            "                \"0QZYiYoBGeSNvliAKP4J\",\n" +
                            "                \"\"\n" +
                            "            ],\n" +
                            "            [\n" +
                            "                \"{\\n  \\\"key1\\\" :\\\"doc2\\\",\\n  \\\"name\\\" :\\\"bob\\\",\\n  \\\"length\\\":2\\n}\\n\",\n" +
                            "                \"0gZYiYoBGeSNvliAof4t\",\n" +
                            "                \"\"\n" +
                            "            ]\n" +
                            "        ],\n" +
                            "        \"column_name\": [\n" +
                            "            \"_source\",\n" +
                            "            \"_id\",\n" +
                            "            \"_routing\"\n" +
                            "        ],\n" +
                            "        \"column_type\": [\n" +
                            "            \"multi_char\",\n" +
                            "            \"multi_char\",\n" +
                            "            \"multi_char\"\n" +
                            "        ]\n" +
                            "    },\n" +
                            "    \"error_info\": {\n" +
                            "        \"ErrorCode\": 0,\n" +
                            "        \"Error\": \"ERROR_NONE\",\n" +
                            "        \"Message\": \"\"\n" +
                            "    }\n" +
                            "}";
                } else {
                    strResponse = "{\"total_time\":2.016,\"has_soft_failure\":false,\"covered_percent\":1.0,"
                        + "\"row_count\":3,\"format_type\":\"full_json\",\"search_info\":{},\"rpc_info\":\"\","
                        + "\"table_leader_info\":{},\"table_build_watermark\":{},\"sql_query\":\"select _id from "
                        + "in1&&kvpair=databaseName:database;formatType:full_json\",\"iquan_plan\":{\"error_code\":0,"
                        + "\"error_message\":\"\",\"result\":{\"rel_plan_version\":\"\",\"rel_plan\":[],"
                        + "\"exec_params\":{}}},\"navi_graph\":\"\",\"trace\":[],\"sql_result\":{\"data\":[[\"wRSgaYoBtIvm0jEE9eGc\"],"
                        + "[\"wBSgaYoBtIvm0jEE9OG2\"],[\"shRlY4oBtIvm0jEEEOFm\"]],\"column_name\":[\"_id\"],"
                        + "\"column_type\":[\"multi_char\"]},"
                        + "\"error_info\":{\"ErrorCode\":0,\"Error\":\"ERROR_NONE\",\"Message\":\"\"}}  ";
                }
                OutputStream os = exchange.getResponseBody();
                os.write(strResponse.getBytes());
                os.close();
            });
            qrsServer.createContext("/sqlClientInfo", exchange -> {
                exchange.sendResponseHeaders(200, 0);
                String response;
                if (randomBoolean()) {
                    response = "{\"error_message\":\"execute failed\",\"error_code\":400}";
                } else {
                    response = "{\"error_message\":\"\",\"result\":{\"default\":{\"general\":{\"tables\":{\"test2"
                        + "\":{\"catalog_name\":\"default\",\"database_name\":\"general\",\"version\":1,"
                        + "\"content\":{\"valid\":true,\"table_name\":\"test2\",\"table_type\":\"normal\","
                        + "\"fields\":[{\"valid\":true,\"field_name\":\"_routing\","
                        + "\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,\"type\":\"STRING\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"STRING\",\"index_name\":\"_routing\"},{\"valid\":true,\"field_name\":\"f1\","
                        + "\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,\"type\":\"STRING\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"STRING\",\"index_name\":\"f1\"},{\"valid\":true,\"field_name\":\"f3\","
                        + "\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,\"type\":\"LONG\",\"extend_infos\":{},"
                        + "\"key_type\":null,\"value_type\":null,\"record_types\":null},\"index_type\":\"NUMBER\","
                        + "\"index_name\":\"f3\"},{\"valid\":true,\"field_name\":\"_seq_no\","
                        + "\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,\"type\":\"LONG\",\"extend_infos\":{},"
                        + "\"key_type\":null,\"value_type\":null,\"record_types\":null},\"index_type\":\"NUMBER\","
                        + "\"index_name\":\"_seq_no\"},{\"valid\":true,\"field_name\":\"_id\","
                        + "\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,\"type\":\"STRING\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"PRIMARYKEY64\",\"index_name\":\"_id\"},{\"valid\":true,"
                        + "\"field_name\":\"_version\",\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,"
                        + "\"type\":\"LONG\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"\",\"index_name\":\"\"},{\"valid\":true,"
                        + "\"field_name\":\"_primary_term\",\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,"
                        + "\"type\":\"LONG\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"\",\"index_name\":\"\"}],\"sub_tables\":[],"
                        + "\"distribution\":{\"valid\":true,\"partition_cnt\":1,\"hash_fields\":[\"_id\"],"
                        + "\"hash_function\":\"HASH\",\"hash_params\":{}},\"join_info\":{\"valid\":true,\"table_name\":\"\","
                        + "\"join_field\":\"\"},\"properties\":{}}},\"test2_summary_\":{\"catalog_name\":\"default\","
                        + "\"database_name\":\"general\",\"version\":1,\"content\":{\"valid\":true,"
                        + "\"table_name\":\"test2_summary_\",\"table_type\":\"summary\",\"fields\":[{\"valid\":true,"
                        + "\"field_name\":\"_routing\",\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,"
                        + "\"type\":\"STRING\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"STRING\",\"index_name\":\"_routing\"},{\"valid\":true,"
                        + "\"field_name\":\"f1\",\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,"
                        + "\"type\":\"STRING\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"STRING\",\"index_name\":\"f1\"},{\"valid\":true,"
                        + "\"field_name\":\"f3\",\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,\"type\":\"LONG\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"NUMBER\",\"index_name\":\"f3\"},{\"valid\":true,\"field_name\":\"_seq_no\","
                        + "\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,\"type\":\"LONG\",\"extend_infos\":{},"
                        + "\"key_type\":null,\"value_type\":null,\"record_types\":null},\"index_type\":\"NUMBER\","
                        + "\"index_name\":\"_seq_no\"},{\"valid\":true,\"field_name\":\"_source\","
                        + "\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,\"type\":\"STRING\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"\",\"index_name\":\"\"},{\"valid\":true,\"field_name\":\"_id\","
                        + "\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,\"type\":\"STRING\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"PRIMARYKEY64\",\"index_name\":\"_id\"},{\"valid\":true,"
                        + "\"field_name\":\"_version\",\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,"
                        + "\"type\":\"LONG\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"\",\"index_name\":\"\"},{\"valid\":true,"
                        + "\"field_name\":\"_primary_term\",\"field_type\":{\"fieldType\":\"FT_INT64\",\"valid\":true,"
                        + "\"type\":\"LONG\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"\",\"index_name\":\"\"}],\"sub_tables\":[],"
                        + "\"distribution\":{\"valid\":true,\"partition_cnt\":1,\"hash_fields\":[\"_id\"],"
                        + "\"hash_function\":\"HASH\",\"hash_params\":{}},\"join_info\":{\"valid\":true,\"table_name\":\"\","
                        + "\"join_field\":\"\"},\"properties\":{}}},\"in0_summary_\":{\"catalog_name\":\"default\","
                        + "\"database_name\":\"general\",\"version\":2,\"content\":{\"valid\":true,"
                        + "\"table_name\":\"in0_summary_\",\"table_type\":\"summary\",\"fields\":[{\"valid\":true,"
                        + "\"field_name\":\"title\",\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,"
                        + "\"type\":\"TEXT\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"TEXT\",\"index_name\":\"title\"},{\"valid\":true,"
                        + "\"field_name\":\"subject\",\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,"
                        + "\"type\":\"TEXT\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"\",\"index_name\":\"\"},{\"valid\":true,"
                        + "\"field_name\":\"id\",\"field_type\":{\"fieldType\":\"FT_UINT32\",\"valid\":true,"
                        + "\"type\":\"UINT32\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"PRIMARYKEY64\",\"index_name\":\"id\"},{\"valid\":true,"
                        + "\"field_name\":\"hits\",\"field_type\":{\"fieldType\":\"FT_UINT32\",\"valid\":true,"
                        + "\"type\":\"UINT32\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"\",\"index_name\":\"\"},{\"valid\":true,"
                        + "\"field_name\":\"createtime\",\"field_type\":{\"fieldType\":\"FT_UINT64\",\"valid\":true,"
                        + "\"type\":\"UINT64\",\"extend_infos\":{},\"key_type\":null,\"value_type\":null,"
                        + "\"record_types\":null},\"index_type\":\"\",\"index_name\":\"\"}],\"sub_tables\":[],"
                        + "\"distribution\":{\"valid\":true,\"partition_cnt\":1,\"hash_fields\":[\"id\"],"
                        + "\"hash_function\":\"HASH\",\"hash_params\":{}},\"join_info\":{\"valid\":true,\"table_name\":\"\","
                        + "\"join_field\":\"\"},\"properties\":{}}},\"in0\":{\"catalog_name\":\"default\","
                        + "\"database_name\":\"general\",\"version\":2,\"content\":{\"valid\":true,\"table_name\":\"in0\","
                        + "\"table_type\":\"normal\",\"fields\":[{\"valid\":true,\"field_name\":\"title\","
                        + "\"field_type\":{\"fieldType\":\"FT_STRING\",\"valid\":true,\"type\":\"TEXT\",\"extend_infos\":{},"
                        + "\"key_type\":null,\"value_type\":null,\"record_types\":null},\"index_type\":\"TEXT\","
                        + "\"index_name\":\"title\"},{\"valid\":true,\"field_name\":\"id\","
                        + "\"field_type\":{\"fieldType\":\"FT_UINT32\",\"valid\":true,\"type\":\"UINT32\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"PRIMARYKEY64\",\"index_name\":\"id\"},{\"valid\":true,\"field_name\":\"hits\","
                        + "\"field_type\":{\"fieldType\":\"FT_UINT32\",\"valid\":true,\"type\":\"UINT32\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"\",\"index_name\":\"\"},{\"valid\":true,\"field_name\":\"createtime\","
                        + "\"field_type\":{\"fieldType\":\"FT_UINT64\",\"valid\":true,\"type\":\"UINT64\","
                        + "\"extend_infos\":{},\"key_type\":null,\"value_type\":null,\"record_types\":null},"
                        + "\"index_type\":\"\",\"index_name\":\"\"}],\"sub_tables\":[],\"distribution\":{\"valid\":true,"
                        + "\"partition_cnt\":1,\"hash_fields\":[\"id\"],\"hash_function\":\"HASH\",\"hash_params\":{}},"
                        + "\"join_info\":{\"valid\":true,\"table_name\":\"\",\"join_field\":\"\"},\"properties\":{}}}},"
                        + "\"functions\":{\"printTableTvf\":{\"isDeterministic\":true,\"name\":\"printTableTvf\","
                        + "\"type\":\"TVF\",\"version\":1,\"signatures\":[\"(,TABLE_auto{})TVF_printTableTvf(string,"
                        + "TABLE_auto{})\"]},\"distinctTopNTvf\":{\"isDeterministic\":true,\"name\":\"distinctTopNTvf\","
                        + "\"type\":\"TVF\",\"version\":1,\"signatures\":[\"(,TABLE_auto{})TVF_distinctTopNTvf(string,string,"
                        + "string,string,string,string,TABLE_auto{})\"]},\"range\":{\"isDeterministic\":true,"
                        + "\"name\":\"range\",\"type\":\"UDF\",\"version\":1,\"signatures\":[\"(boolean)UDXF_range(int32,"
                        + "string)\",\"(boolean)UDXF_range(int64,string)\",\"(boolean)UDXF_range(float,string)\",\"(boolean)"
                        + "UDXF_range(double,string)\"]},\"ha_in\":{\"isDeterministic\":true,\"name\":\"ha_in\","
                        + "\"type\":\"UDF\",\"version\":1,\"signatures\":[\"(boolean)UDXF_ha_in(int32,string)\",\"(boolean)"
                        + "UDXF_ha_in(int64,string)\",\"(boolean)UDXF_ha_in(string,string)\"]},"
                        + "\"MATCHINDEX\":{\"isDeterministic\":true,\"name\":\"MATCHINDEX\",\"type\":\"UDF\",\"version\":1,"
                        + "\"signatures\":[\"(boolean)UDXF_MATCHINDEX(string,string)\",\"(boolean)UDXF_MATCHINDEX(string,"
                        + "string,string)\"]},\"inputTableTvf\":{\"isDeterministic\":true,\"name\":\"inputTableTvf\","
                        + "\"type\":\"TVF\",\"version\":1,\"signatures\":[\"(,TABLE_auto{})TVF_inputTableTvf(string,"
                        + "TABLE_auto{})\"]},\"matchscore\":{\"isDeterministic\":true,\"name\":\"matchscore\","
                        + "\"type\":\"UDF\",\"version\":1,\"signatures\":[\"(double)UDXF_matchscore(int32,string,double)\",\""
                        + "(double)UDXF_matchscore(int64,string,double)\",\"(double)UDXF_matchscore(array|int32,string,"
                        + "double)\",\"(double)UDXF_matchscore(array|int64,string,double)\"]},"
                        + "\"matchscore2\":{\"isDeterministic\":true,\"name\":\"matchscore2\",\"type\":\"UDF\",\"version\":1,"
                        + "\"signatures\":[\"(double)UDXF_matchscore2(array|int32,string,double)\",\"(double)UDXF_matchscore2"
                        + "(array|int64,string,double)\"]},\"rankTvf\":{\"isDeterministic\":true,\"name\":\"rankTvf\","
                        + "\"type\":\"TVF\",\"version\":1,\"signatures\":[\"(,TABLE_auto{})TVF_rankTvf(string,string,string,"
                        + "TABLE_auto{})\"]},\"graphSearchTvf\":{\"isDeterministic\":true,\"name\":\"graphSearchTvf\","
                        + "\"type\":\"TVF\",\"version\":1,\"signatures\":[\"(__buildin_score__|float,TABLE_auto{})"
                        + "TVF_graphSearchTvf(string,string,string,TABLE_auto{})\"]},\"GATHER\":{\"isDeterministic\":true,"
                        + "\"name\":\"GATHER\",\"type\":\"UDAF\",\"version\":1,\"signatures\":[\"(array|int8)UDXF_GATHER"
                        + "(int8)[array|int8]\",\"(array|int16)UDXF_GATHER(int16)[array|int16]\",\"(array|int32)UDXF_GATHER"
                        + "(int32)[array|int32]\",\"(array|int64)UDXF_GATHER(int64)[array|int64]\",\"(array|float)UDXF_GATHER"
                        + "(float)[array|float]\",\"(array|double)UDXF_GATHER(double)[array|double]\",\"(array|string)"
                        + "UDXF_GATHER(string)[array|string]\"]},\"notcontain\":{\"isDeterministic\":true,"
                        + "\"name\":\"notcontain\",\"type\":\"UDF\",\"version\":1,\"signatures\":[\"(boolean)UDXF_notcontain"
                        + "(int32,string)\",\"(boolean)UDXF_notcontain(array|int32,string)\",\"(boolean)UDXF_notcontain"
                        + "(int64,string)\",\"(boolean)UDXF_notcontain(array|int64,string)\",\"(boolean)UDXF_notcontain"
                        + "(string,string)\",\"(boolean)UDXF_notcontain(array|string,string)\"]},"
                        + "\"normalizescore\":{\"isDeterministic\":true,\"name\":\"normalizescore\",\"type\":\"UDF\","
                        + "\"version\":1,\"signatures\":[\"(double)UDXF_normalizescore(int32,double)\",\"(double)"
                        + "UDXF_normalizescore(int64,double)\",\"(double)UDXF_normalizescore(double,double)\"]},"
                        + "\"rangevalue\":{\"isDeterministic\":true,\"name\":\"rangevalue\",\"type\":\"UDF\",\"version\":1,"
                        + "\"signatures\":[\"(float)UDXF_rangevalue(float,string)\"]},"
                        + "\"LOGIC_AND\":{\"isDeterministic\":true,\"name\":\"LOGIC_AND\",\"type\":\"UDAF\",\"version\":1,"
                        + "\"signatures\":[\"(int8)UDXF_LOGIC_AND(int8)[int8]\",\"(int16)UDXF_LOGIC_AND(int16)[int16]\",\""
                        + "(int32)UDXF_LOGIC_AND(int32)[int32]\",\"(int64)UDXF_LOGIC_AND(int64)[int64]\"]},"
                        + "\"ARBITRARY\":{\"isDeterministic\":true,\"name\":\"ARBITRARY\",\"type\":\"UDAF\",\"version\":1,"
                        + "\"signatures\":[\"(int8)UDXF_ARBITRARY(int8)[int8]\",\"(int16)UDXF_ARBITRARY(int16)[int16]\",\""
                        + "(int32)UDXF_ARBITRARY(int32)[int32]\",\"(int64)UDXF_ARBITRARY(int64)[int64]\",\"(float)"
                        + "UDXF_ARBITRARY(float)[float]\",\"(double)UDXF_ARBITRARY(double)[double]\",\"(string)UDXF_ARBITRARY"
                        + "(string)[string]\",\"(array|int8)UDXF_ARBITRARY(array|int8)[array|int8]\",\"(array|int16)"
                        + "UDXF_ARBITRARY(array|int16)[array|int16]\",\"(array|int32)UDXF_ARBITRARY(array|int32)"
                        + "[array|int32]\",\"(array|int64)UDXF_ARBITRARY(array|int64)[array|int64]\",\"(array|float)"
                        + "UDXF_ARBITRARY(array|float)[array|float]\",\"(array|double)UDXF_ARBITRARY(array|double)"
                        + "[array|double]\",\"(array|string)UDXF_ARBITRARY(array|string)[array|string]\"]},"
                        + "\"contain\":{\"isDeterministic\":true,\"name\":\"contain\",\"type\":\"UDF\",\"version\":1,"
                        + "\"signatures\":[\"(boolean)UDXF_contain(int32,string)\",\"(boolean)UDXF_contain(array|int32,"
                        + "string)\",\"(boolean)UDXF_contain(int64,string)\",\"(boolean)UDXF_contain(array|int64,string)\",\""
                        + "(boolean)UDXF_contain(string,string)\",\"(boolean)UDXF_contain(array|string,string)\"]},"
                        + "\"hashcombine\":{\"isDeterministic\":true,\"name\":\"hashcombine\",\"type\":\"UDF\",\"version\":1,"
                        + "\"signatures\":[\"(int64)UDXF_hashcombine(int64,int64,int64)\"]},"
                        + "\"unpackMultiValue\":{\"isDeterministic\":true,\"name\":\"unpackMultiValue\",\"type\":\"TVF\","
                        + "\"version\":1,\"signatures\":[\"(,TABLE_auto{})TVF_unpackMultiValue(string,TABLE_auto{})\"]},"
                        + "\"sp_query_match\":{\"isDeterministic\":true,\"name\":\"sp_query_match\",\"type\":\"UDF\","
                        + "\"version\":1,\"signatures\":[\"(boolean)UDXF_sp_query_match(string)\"]},"
                        + "\"enableShuffleTvf\":{\"isDeterministic\":true,\"name\":\"enableShuffleTvf\",\"type\":\"TVF\","
                        + "\"version\":1,\"signatures\":[\"(,TABLE_auto{})TVF_enableShuffleTvf(,TABLE_auto{})\"]},"
                        + "\"QUERY\":{\"isDeterministic\":true,\"name\":\"QUERY\",\"type\":\"UDF\",\"version\":1,"
                        + "\"signatures\":[\"(boolean)UDXF_QUERY(string,string)\",\"(boolean)UDXF_QUERY(string,string,string)"
                        + "\"]},\"sortTvf\":{\"isDeterministic\":true,\"name\":\"sortTvf\",\"type\":\"TVF\",\"version\":1,"
                        + "\"signatures\":[\"(,TABLE_auto{})TVF_sortTvf(string,string,TABLE_auto{})\"]},"
                        + "\"LOGIC_OR\":{\"isDeterministic\":true,\"name\":\"LOGIC_OR\",\"type\":\"UDAF\",\"version\":1,"
                        + "\"signatures\":[\"(int8)UDXF_LOGIC_OR(int8)[int8]\",\"(int16)UDXF_LOGIC_OR(int16)[int16]\",\""
                        + "(int32)UDXF_LOGIC_OR(int32)[int32]\",\"(int64)UDXF_LOGIC_OR(int64)[int64]\"]},"
                        + "\"MULTICAST\":{\"isDeterministic\":true,\"name\":\"MULTICAST\",\"type\":\"UDF\",\"version\":1,"
                        + "\"signatures\":[\"(string)UDXF_MULTICAST(array|string)\",\"(int32)UDXF_MULTICAST(array|int32)\",\""
                        + "(int64)UDXF_MULTICAST(array|int64)\"]},\"sphere_distance\":{\"isDeterministic\":true,"
                        + "\"name\":\"sphere_distance\",\"type\":\"UDF\",\"version\":1,\"signatures\":[\"(double)"
                        + "UDXF_sphere_distance(array|double,double,double)\"]},\"MAXLABEL\":{\"isDeterministic\":true,"
                        + "\"name\":\"MAXLABEL\",\"type\":\"UDAF\",\"version\":1,\"signatures\":[\"(array|int8)UDXF_MAXLABEL"
                        + "(array|int8,array|int8)[array|int8,array|int8]\",\"(array|int8)UDXF_MAXLABEL(array|int8,"
                        + "array|int16)[array|int8,array|int16]\",\"(array|int8)UDXF_MAXLABEL(array|int8,array|int32)"
                        + "[array|int8,array|int32]\",\"(array|int8)UDXF_MAXLABEL(array|int8,array|int64)[array|int8,"
                        + "array|int64]\",\"(array|int8)UDXF_MAXLABEL(array|int8,array|float)[array|int8,array|float]\",\""
                        + "(array|int8)UDXF_MAXLABEL(array|int8,array|double)[array|int8,array|double]\",\"(array|int8)"
                        + "UDXF_MAXLABEL(array|int8,int8)[array|int8,int8]\",\"(array|int8)UDXF_MAXLABEL(array|int8,int16)"
                        + "[array|int8,int16]\",\"(array|int8)UDXF_MAXLABEL(array|int8,int32)[array|int8,int32]\",\""
                        + "(array|int8)UDXF_MAXLABEL(array|int8,int64)[array|int8,int64]\",\"(array|int8)UDXF_MAXLABEL"
                        + "(array|int8,float)[array|int8,float]\",\"(array|int8)UDXF_MAXLABEL(array|int8,double)[array|int8,"
                        + "double]\",\"(array|int16)UDXF_MAXLABEL(array|int16,array|int8)[array|int16,array|int8]\",\""
                        + "(array|int16)UDXF_MAXLABEL(array|int16,array|int16)[array|int16,array|int16]\",\"(array|int16)"
                        + "UDXF_MAXLABEL(array|int16,array|int32)[array|int16,array|int32]\",\"(array|int16)UDXF_MAXLABEL"
                        + "(array|int16,array|int64)[array|int16,array|int64]\",\"(array|int16)UDXF_MAXLABEL(array|int16,"
                        + "array|float)[array|int16,array|float]\",\"(array|int16)UDXF_MAXLABEL(array|int16,array|double)"
                        + "[array|int16,array|double]\",\"(array|int16)UDXF_MAXLABEL(array|int16,int8)[array|int16,int8]\",\""
                        + "(array|int16)UDXF_MAXLABEL(array|int16,int16)[array|int16,int16]\",\"(array|int16)UDXF_MAXLABEL"
                        + "(array|int16,int32)[array|int16,int32]\",\"(array|int16)UDXF_MAXLABEL(array|int16,int64)"
                        + "[array|int16,int64]\",\"(array|int16)UDXF_MAXLABEL(array|int16,float)[array|int16,float]\",\""
                        + "(array|int16)UDXF_MAXLABEL(array|int16,double)[array|int16,double]\",\"(array|int32)UDXF_MAXLABEL"
                        + "(array|int32,array|int8)[array|int32,array|int8]\",\"(array|int32)UDXF_MAXLABEL(array|int32,"
                        + "array|int16)[array|int32,array|int16]\",\"(array|int32)UDXF_MAXLABEL(array|int32,array|int32)"
                        + "[array|int32,array|int32]\",\"(array|int32)UDXF_MAXLABEL(array|int32,array|int64)[array|int32,"
                        + "array|int64]\",\"(array|int32)UDXF_MAXLABEL(array|int32,array|float)[array|int32,array|float]\",\""
                        + "(array|int32)UDXF_MAXLABEL(array|int32,array|double)[array|int32,array|double]\",\"(array|int32)"
                        + "UDXF_MAXLABEL(array|int32,int8)[array|int32,int8]\",\"(array|int32)UDXF_MAXLABEL(array|int32,"
                        + "int16)[array|int32,int16]\",\"(array|int32)UDXF_MAXLABEL(array|int32,int32)[array|int32,int32]\","
                        + "\"(array|int32)UDXF_MAXLABEL(array|int32,int64)[array|int32,int64]\",\"(array|int32)UDXF_MAXLABEL"
                        + "(array|int32,float)[array|int32,float]\",\"(array|int32)UDXF_MAXLABEL(array|int32,double)"
                        + "[array|int32,double]\",\"(array|int64)UDXF_MAXLABEL(array|int64,array|int8)[array|int64,"
                        + "array|int8]\",\"(array|int64)UDXF_MAXLABEL(array|int64,array|int16)[array|int64,array|int16]\",\""
                        + "(array|int64)UDXF_MAXLABEL(array|int64,array|int32)[array|int64,array|int32]\",\"(array|int64)"
                        + "UDXF_MAXLABEL(array|int64,array|int64)[array|int64,array|int64]\",\"(array|int64)UDXF_MAXLABEL"
                        + "(array|int64,array|float)[array|int64,array|float]\",\"(array|int64)UDXF_MAXLABEL(array|int64,"
                        + "array|double)[array|int64,array|double]\",\"(array|int64)UDXF_MAXLABEL(array|int64,int8)"
                        + "[array|int64,int8]\",\"(array|int64)UDXF_MAXLABEL(array|int64,int16)[array|int64,int16]\",\""
                        + "(array|int64)UDXF_MAXLABEL(array|int64,int32)[array|int64,int32]\",\"(array|int64)UDXF_MAXLABEL"
                        + "(array|int64,int64)[array|int64,int64]\",\"(array|int64)UDXF_MAXLABEL(array|int64,float)"
                        + "[array|int64,float]\",\"(array|int64)UDXF_MAXLABEL(array|int64,double)[array|int64,double]\",\""
                        + "(array|float)UDXF_MAXLABEL(array|float,array|int8)[array|float,array|int8]\",\"(array|float)"
                        + "UDXF_MAXLABEL(array|float,array|int16)[array|float,array|int16]\",\"(array|float)UDXF_MAXLABEL"
                        + "(array|float,array|int32)[array|float,array|int32]\",\"(array|float)UDXF_MAXLABEL(array|float,"
                        + "array|int64)[array|float,array|int64]\",\"(array|float)UDXF_MAXLABEL(array|float,array|float)"
                        + "[array|float,array|float]\",\"(array|float)UDXF_MAXLABEL(array|float,array|double)[array|float,"
                        + "array|double]\",\"(array|float)UDXF_MAXLABEL(array|float,int8)[array|float,int8]\",\"(array|float)"
                        + "UDXF_MAXLABEL(array|float,int16)[array|float,int16]\",\"(array|float)UDXF_MAXLABEL(array|float,"
                        + "int32)[array|float,int32]\",\"(array|float)UDXF_MAXLABEL(array|float,int64)[array|float,int64]\","
                        + "\"(array|float)UDXF_MAXLABEL(array|float,float)[array|float,float]\",\"(array|float)UDXF_MAXLABEL"
                        + "(array|float,double)[array|float,double]\",\"(array|double)UDXF_MAXLABEL(array|double,array|int8)"
                        + "[array|double,array|int8]\",\"(array|double)UDXF_MAXLABEL(array|double,array|int16)[array|double,"
                        + "array|int16]\",\"(array|double)UDXF_MAXLABEL(array|double,array|int32)[array|double,"
                        + "array|int32]\",\"(array|double)UDXF_MAXLABEL(array|double,array|int64)[array|double,"
                        + "array|int64]\",\"(array|double)UDXF_MAXLABEL(array|double,array|float)[array|double,"
                        + "array|float]\",\"(array|double)UDXF_MAXLABEL(array|double,array|double)[array|double,"
                        + "array|double]\",\"(array|double)UDXF_MAXLABEL(array|double,int8)[array|double,int8]\",\""
                        + "(array|double)UDXF_MAXLABEL(array|double,int16)[array|double,int16]\",\"(array|double)"
                        + "UDXF_MAXLABEL(array|double,int32)[array|double,int32]\",\"(array|double)UDXF_MAXLABEL"
                        + "(array|double,int64)[array|double,int64]\",\"(array|double)UDXF_MAXLABEL(array|double,float)"
                        + "[array|double,float]\",\"(array|double)UDXF_MAXLABEL(array|double,double)[array|double,double]\","
                        + "\"(int8)UDXF_MAXLABEL(int8,array|int8)[int8,array|int8]\",\"(int8)UDXF_MAXLABEL(int8,array|int16)"
                        + "[int8,array|int16]\",\"(int8)UDXF_MAXLABEL(int8,array|int32)[int8,array|int32]\",\"(int8)"
                        + "UDXF_MAXLABEL(int8,array|int64)[int8,array|int64]\",\"(int8)UDXF_MAXLABEL(int8,array|float)[int8,"
                        + "array|float]\",\"(int8)UDXF_MAXLABEL(int8,array|double)[int8,array|double]\",\"(int8)UDXF_MAXLABEL"
                        + "(int8,int8)[int8,int8]\",\"(int8)UDXF_MAXLABEL(int8,int16)[int8,int16]\",\"(int8)UDXF_MAXLABEL"
                        + "(int8,int32)[int8,int32]\",\"(int8)UDXF_MAXLABEL(int8,int64)[int8,int64]\",\"(int8)UDXF_MAXLABEL"
                        + "(int8,float)[int8,float]\",\"(int8)UDXF_MAXLABEL(int8,double)[int8,double]\",\"(int16)"
                        + "UDXF_MAXLABEL(int16,array|int8)[int16,array|int8]\",\"(int16)UDXF_MAXLABEL(int16,array|int16)"
                        + "[int16,array|int16]\",\"(int16)UDXF_MAXLABEL(int16,array|int32)[int16,array|int32]\",\"(int16)"
                        + "UDXF_MAXLABEL(int16,array|int64)[int16,array|int64]\",\"(int16)UDXF_MAXLABEL(int16,array|float)"
                        + "[int16,array|float]\",\"(int16)UDXF_MAXLABEL(int16,array|double)[int16,array|double]\",\"(int16)"
                        + "UDXF_MAXLABEL(int16,int8)[int16,int8]\",\"(int16)UDXF_MAXLABEL(int16,int16)[int16,int16]\",\""
                        + "(int16)UDXF_MAXLABEL(int16,int32)[int16,int32]\",\"(int16)UDXF_MAXLABEL(int16,int64)[int16,"
                        + "int64]\",\"(int16)UDXF_MAXLABEL(int16,float)[int16,float]\",\"(int16)UDXF_MAXLABEL(int16,double)"
                        + "[int16,double]\",\"(int32)UDXF_MAXLABEL(int32,array|int8)[int32,array|int8]\",\"(int32)"
                        + "UDXF_MAXLABEL(int32,array|int16)[int32,array|int16]\",\"(int32)UDXF_MAXLABEL(int32,array|int32)"
                        + "[int32,array|int32]\",\"(int32)UDXF_MAXLABEL(int32,array|int64)[int32,array|int64]\",\"(int32)"
                        + "UDXF_MAXLABEL(int32,array|float)[int32,array|float]\",\"(int32)UDXF_MAXLABEL(int32,array|double)"
                        + "[int32,array|double]\",\"(int32)UDXF_MAXLABEL(int32,int8)[int32,int8]\",\"(int32)UDXF_MAXLABEL"
                        + "(int32,int16)[int32,int16]\",\"(int32)UDXF_MAXLABEL(int32,int32)[int32,int32]\",\"(int32)"
                        + "UDXF_MAXLABEL(int32,int64)[int32,int64]\",\"(int32)UDXF_MAXLABEL(int32,float)[int32,float]\",\""
                        + "(int32)UDXF_MAXLABEL(int32,double)[int32,double]\",\"(int64)UDXF_MAXLABEL(int64,array|int8)[int64,"
                        + "array|int8]\",\"(int64)UDXF_MAXLABEL(int64,array|int16)[int64,array|int16]\",\"(int64)"
                        + "UDXF_MAXLABEL(int64,array|int32)[int64,array|int32]\",\"(int64)UDXF_MAXLABEL(int64,array|int64)"
                        + "[int64,array|int64]\",\"(int64)UDXF_MAXLABEL(int64,array|float)[int64,array|float]\",\"(int64)"
                        + "UDXF_MAXLABEL(int64,array|double)[int64,array|double]\",\"(int64)UDXF_MAXLABEL(int64,int8)[int64,"
                        + "int8]\",\"(int64)UDXF_MAXLABEL(int64,int16)[int64,int16]\",\"(int64)UDXF_MAXLABEL(int64,int32)"
                        + "[int64,int32]\",\"(int64)UDXF_MAXLABEL(int64,int64)[int64,int64]\",\"(int64)UDXF_MAXLABEL(int64,"
                        + "float)[int64,float]\",\"(int64)UDXF_MAXLABEL(int64,double)[int64,double]\",\"(float)UDXF_MAXLABEL"
                        + "(float,array|int8)[float,array|int8]\",\"(float)UDXF_MAXLABEL(float,array|int16)[float,"
                        + "array|int16]\",\"(float)UDXF_MAXLABEL(float,array|int32)[float,array|int32]\",\"(float)"
                        + "UDXF_MAXLABEL(float,array|int64)[float,array|int64]\",\"(float)UDXF_MAXLABEL(float,array|float)"
                        + "[float,array|float]\",\"(float)UDXF_MAXLABEL(float,array|double)[float,array|double]\",\"(float)"
                        + "UDXF_MAXLABEL(float,int8)[float,int8]\",\"(float)UDXF_MAXLABEL(float,int16)[float,int16]\",\""
                        + "(float)UDXF_MAXLABEL(float,int32)[float,int32]\",\"(float)UDXF_MAXLABEL(float,int64)[float,"
                        + "int64]\",\"(float)UDXF_MAXLABEL(float,float)[float,float]\",\"(float)UDXF_MAXLABEL(float,double)"
                        + "[float,double]\",\"(double)UDXF_MAXLABEL(double,array|int8)[double,array|int8]\",\"(double)"
                        + "UDXF_MAXLABEL(double,array|int16)[double,array|int16]\",\"(double)UDXF_MAXLABEL(double,"
                        + "array|int32)[double,array|int32]\",\"(double)UDXF_MAXLABEL(double,array|int64)[double,"
                        + "array|int64]\",\"(double)UDXF_MAXLABEL(double,array|float)[double,array|float]\",\"(double)"
                        + "UDXF_MAXLABEL(double,array|double)[double,array|double]\",\"(double)UDXF_MAXLABEL(double,int8)"
                        + "[double,int8]\",\"(double)UDXF_MAXLABEL(double,int16)[double,int16]\",\"(double)UDXF_MAXLABEL"
                        + "(double,int32)[double,int32]\",\"(double)UDXF_MAXLABEL(double,int64)[double,int64]\",\"(double)"
                        + "UDXF_MAXLABEL(double,float)[double,float]\",\"(double)UDXF_MAXLABEL(double,double)[double,"
                        + "double]\",\"(string)UDXF_MAXLABEL(string,array|int8)[string,array|int8]\",\"(string)UDXF_MAXLABEL"
                        + "(string,array|int16)[string,array|int16]\",\"(string)UDXF_MAXLABEL(string,array|int32)[string,"
                        + "array|int32]\",\"(string)UDXF_MAXLABEL(string,array|int64)[string,array|int64]\",\"(string)"
                        + "UDXF_MAXLABEL(string,array|float)[string,array|float]\",\"(string)UDXF_MAXLABEL(string,"
                        + "array|double)[string,array|double]\",\"(string)UDXF_MAXLABEL(string,int8)[string,int8]\",\""
                        + "(string)UDXF_MAXLABEL(string,int16)[string,int16]\",\"(string)UDXF_MAXLABEL(string,int32)[string,"
                        + "int32]\",\"(string)UDXF_MAXLABEL(string,int64)[string,int64]\",\"(string)UDXF_MAXLABEL(string,"
                        + "float)[string,float]\",\"(string)UDXF_MAXLABEL(string,double)[string,double]\",\"(array|string)"
                        + "UDXF_MAXLABEL(array|string,array|int8)[array|string,array|int8]\",\"(array|string)UDXF_MAXLABEL"
                        + "(array|string,array|int16)[array|string,array|int16]\",\"(array|string)UDXF_MAXLABEL(array|string,"
                        + "array|int32)[array|string,array|int32]\",\"(array|string)UDXF_MAXLABEL(array|string,array|int64)"
                        + "[array|string,array|int64]\",\"(array|string)UDXF_MAXLABEL(array|string,array|float)[array|string,"
                        + "array|float]\",\"(array|string)UDXF_MAXLABEL(array|string,array|double)[array|string,"
                        + "array|double]\",\"(array|string)UDXF_MAXLABEL(array|string,int8)[array|string,int8]\",\""
                        + "(array|string)UDXF_MAXLABEL(array|string,int16)[array|string,int16]\",\"(array|string)"
                        + "UDXF_MAXLABEL(array|string,int32)[array|string,int32]\",\"(array|string)UDXF_MAXLABEL"
                        + "(array|string,int64)[array|string,int64]\",\"(array|string)UDXF_MAXLABEL(array|string,float)"
                        + "[array|string,float]\",\"(array|string)UDXF_MAXLABEL(array|string,double)[array|string,"
                        + "double]\"]},\"MULTIGATHER\":{\"isDeterministic\":true,\"name\":\"MULTIGATHER\",\"type\":\"UDAF\","
                        + "\"version\":1,\"signatures\":[\"(array|int8)UDXF_MULTIGATHER(array|int8)[array|int8]\",\""
                        + "(array|int16)UDXF_MULTIGATHER(array|int16)[array|int16]\",\"(array|int32)UDXF_MULTIGATHER"
                        + "(array|int32)[array|int32]\",\"(array|int64)UDXF_MULTIGATHER(array|int64)[array|int64]\",\""
                        + "(array|float)UDXF_MULTIGATHER(array|float)[array|float]\",\"(array|double)UDXF_MULTIGATHER"
                        + "(array|double)[array|double]\",\"(array|string)UDXF_MULTIGATHER(array|string)[array|string]\"]}}},"
                        + "\"__default\":{\"tables\":{},\"functions\":{}}}},\"error_code\":0}";
                }
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            });
            qrsServer.createContext("/HeartbeatService/heartbeat", exchange -> {
                exchange.sendResponseHeaders(200, 0);
                if (exchange.getRequestMethod().equals("POST")) {
                    String response = "{\n"
                        + "\"customInfo\":\n"
                        + "  \"{\\n\\\"app_info\\\":\\n  {\\n  \\\"config_path\\\":\\n    \\\"\\\",\\n  "
                        + "\\\"keep_count\\\":\\n    20\\n  },\\n\\\"biz_info\\\":\\n  {\\n  \\\"default\\\":\\n    {\\n "
                        + "   \\\"config_path\\\":\\n      \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/config\\\\\\/bizs\\\\\\/0\\\",\\n    \\\"custom_biz_info\\\":\\n      {\\n      },\\n  "
                        + "  \\\"keep_count\\\":\\n      5\\n    }\\n  },\\n\\\"custom_app_info\\\":\\n  {\\n  },"
                        + "\\n\\\"service_info\\\":\\n  {\\n  \\\"cm2\\\":\\n    {\\n    \\\"topo_info\\\":\\n      "
                        + "\\\"ha3.general.default:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_2:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_4:1:0:1457961441:100:2400309353:-1:true|\\\"\\n    }\\n  },"
                        + "\\n\\\"table_info\\\":\\n  {\\n  }\\n}\",\n"
                        + "\"serviceInfo\":\n"
                        + "  \"{\\n\\\"cm2\\\":\\n  {\\n  \\\"topo_info\\\":\\n    \\\"ha3.general"
                        + ".default:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_2:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_4:1:0:1457961441:100:2400309353:-1:true|\\\"\\n  }\\n}\",\n"
                        + "\"signature\":\n"
                        + "  \"{\\\"table_info\\\": {}, \\\"biz_info\\\": {\\\"default\\\": {\\\"config_path\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/config\\/bizs\\/0\\\"}}, \\\"service_info\\\":"
                        + " {\\\"zone_name\\\": \\\"qrs\\\", \\\"cm2_config\\\": {\\\"local\\\": [{\\\"part_count\\\": 1,"
                        + " \\\"biz_name\\\": \\\"general.default\\\", \\\"ip\\\": \\\"172.17.0.2\\\", \\\"version\\\": "
                        + "1976225101, \\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, {\\\"part_count\\\": 1, "
                        + "\\\"biz_name\\\": \\\"general.para_search_4\\\", \\\"ip\\\": \\\"172.17.0.2\\\", "
                        + "\\\"version\\\": 1976225101, \\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, "
                        + "{\\\"part_count\\\": 1, \\\"biz_name\\\": \\\"general.default_sql\\\", \\\"ip\\\": \\\"172.17"
                        + ".0.2\\\", \\\"version\\\": 1976225101, \\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, "
                        + "{\\\"part_count\\\": 1, \\\"biz_name\\\": \\\"general.para_search_2\\\", \\\"ip\\\": \\\"172"
                        + ".17.0.2\\\", \\\"version\\\": 1976225101, \\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, "
                        + "{\\\"part_count\\\": 1, \\\"biz_name\\\": \\\"general.default_agg\\\", \\\"ip\\\": \\\"172.17"
                        + ".0.2\\\", \\\"version\\\": 1976225101, \\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}]}, "
                        + "\\\"part_id\\\": 0, \\\"part_count\\\": 0}, \\\"clean_disk\\\": false}\"\n"
                        + "}";
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                } else {
                    String response = "{\n"
                        + "\"customInfo\":\n"
                        + "  \"{\\n\\\"app_info\\\":\\n  {\\n  \\\"config_path\\\":\\n    \\\"\\\",\\n  "
                        + "\\\"keep_count\\\":\\n    20\\n  },\\n\\\"biz_info\\\":\\n  {\\n  \\\"default\\\":\\n    {\\n    "
                        + "\\\"config_path\\\":\\n      \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/config\\\\\\/bizs\\\\\\/0\\\",\\n    \\\"custom_biz_info\\\":\\n      {\\n      },\\n    "
                        + "\\\"keep_count\\\":\\n      5\\n    }\\n  },\\n\\\"custom_app_info\\\":\\n  {\\n  },"
                        + "\\n\\\"service_info\\\":\\n  {\\n  \\\"cm2\\\":\\n    {\\n    \\\"topo_info\\\":\\n      \\\"ha3"
                        + ".general.default:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_2:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_4:1:0:1457961441:100:2400309353:-1:true|\\\"\\n    }\\n  },\\n\\\"table_info\\\":\\n "
                        + " {\\n  }\\n}\",\n"
                        + "\"serviceInfo\":\n"
                        + "  \"{\\n\\\"cm2\\\":\\n  {\\n  \\\"topo_info\\\":\\n    \\\"ha3.general"
                        + ".default:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_2:1:0:1457961441:100:2400309353:-1:true|ha3.general"
                        + ".para_search_4:1:0:1457961441:100:2400309353:-1:true|\\\"\\n  }\\n}\",\n"
                        + "\"signature\":\n"
                        + "  \"{\\\"table_info\\\": {}, \\\"biz_info\\\": {\\\"default\\\": {\\\"config_path\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/config\\/bizs\\/0\\\"}}, \\\"service_info\\\": "
                        + "{\\\"zone_name\\\": \\\"qrs\\\", \\\"cm2_config\\\": {\\\"local\\\": [{\\\"part_count\\\": 1, "
                        + "\\\"biz_name\\\": \\\"general.default\\\", \\\"ip\\\": \\\"172.17.0.2\\\", \\\"version\\\": "
                        + "553898268, \\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, {\\\"part_count\\\": 1, \\\"biz_name\\\":"
                        + " \\\"general.para_search_4\\\", \\\"ip\\\": \\\"172.17.0.2\\\", \\\"version\\\": 553898268, "
                        + "\\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, {\\\"part_count\\\": 1, \\\"biz_name\\\": "
                        + "\\\"general.default_sql\\\", \\\"ip\\\": \\\"172.17.0.2\\\", \\\"version\\\": 553898268, "
                        + "\\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, {\\\"part_count\\\": 1, \\\"biz_name\\\": "
                        + "\\\"general.para_search_2\\\", \\\"ip\\\": \\\"172.17.0.2\\\", \\\"version\\\": 553898268, "
                        + "\\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}, {\\\"part_count\\\": 1, \\\"biz_name\\\": "
                        + "\\\"general.default_agg\\\", \\\"ip\\\": \\\"172.17.0.2\\\", \\\"version\\\": 553898268, "
                        + "\\\"part_id\\\": 0, \\\"tcp_port\\\": 39300}]}, \\\"part_id\\\": 0, \\\"part_count\\\": 0}, "
                        + "\\\"clean_disk\\\": false}\"\n"
                        + "}";
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                }

            });
            qrsServer.start();
        }

        if (searcherServer == null) {
            searcherServer = HttpServer.create(new InetSocketAddress(39200), 0);
            searcherServer.createContext("/HeartbeatService/heartbeat", exchange -> {
                exchange.sendResponseHeaders(200, 0);
                if (exchange.getRequestMethod().equals("POST")) {
                    String response = "{\n"
                        + "\"customInfo\":\n"
                        + "  \"{\\n\\\"app_info\\\":\\n  {\\n  \\\"config_path\\\":\\n    \\\"\\\",\\n  "
                        + "\\\"keep_count\\\":\\n    20\\n  },\\n\\\"biz_info\\\":\\n  {\\n  \\\"default\\\":\\n    {\\n "
                        + "   \\\"config_path\\\":\\n      \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/config\\\\\\/bizs\\\\\\/0\\\",\\n    \\\"custom_biz_info\\\":\\n      {\\n      },\\n  "
                        + "  \\\"keep_count\\\":\\n      5\\n    }\\n  },\\n\\\"custom_app_info\\\":\\n  {\\n  },"
                        + "\\n\\\"service_info\\\":\\n  {\\n  \\\"cm2\\\":\\n    {\\n    \\\"topo_info\\\":\\n      "
                        + "\\\"general.default:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".default_agg:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".default_sql:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".para_search_2:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".para_search_4:1:0:1976225101:100:2400309353:-1:true|\\\"\\n    }\\n  },"
                        + "\\n\\\"table_info\\\":\\n  {\\n  \\\"in0\\\":\\n    {\\n    \\\"0\\\":\\n      {\\n      "
                        + "\\\"config_path\\\":\\n        \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/config\\\\\\/table\\\\\\/0\\\",\\n      \\\"force_online\\\":\\n        false,\\n      "
                        + "\\\"group_name\\\":\\n        \\\"default_group_name\\\",\\n      \\\"index_root\\\":\\n      "
                        + "  \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\",\\n      \\\"partitions\\\":\\n        {\\n        "
                        + "\\\"0_65535\\\":\\n          {\\n          \\\"check_index_path\\\":\\n            \\\"\\\","
                        + "\\n          \\\"deploy_status\\\":\\n            2,\\n          \\\"deploy_status_map\\\":\\n"
                        + "            [\\n              [\\n                1,\\n                {\\n                "
                        + "\\\"deploy_status\\\":\\n                  2,\\n                \\\"local_config_path\\\":\\n "
                        + "                 \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/local_search_12000\\\\\\/general_0\\\\\\/zone_config\\\\\\/table\\\\\\/in0\\\\\\/0"
                        + "\\\\\\/\\\"\\n                }\\n              ]\\n            ],\\n          "
                        + "\\\"inc_version\\\":\\n            1,\\n          \\\"keep_count\\\":\\n            1,\\n     "
                        + "     \\\"loaded_config_path\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/config\\\\\\/table\\\\\\/0"
                        + "\\\",\\n          \\\"loaded_index_root\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\",\\n          \\\"local_index_path\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\\\\/in0\\\\\\/generation_0\\\\\\/partition_0_65535\\\","
                        + "\\n          \\\"rt_status\\\":\\n            0,\\n          \\\"schema_version\\\":\\n       "
                        + "     0,\\n          \\\"table_load_type\\\":\\n            0,\\n          "
                        + "\\\"table_status\\\":\\n            6,\\n          \\\"table_type\\\":\\n            0\\n     "
                        + "     }\\n        },\\n      \\\"raw_index_root\\\":\\n        \\\"\\\",\\n      "
                        + "\\\"rt_status\\\":\\n        0,\\n      \\\"timestamp_to_skip\\\":\\n        -1\\n      }\\n  "
                        + "  }\\n  }\\n}\",\n"
                        + "\"serviceInfo\":\n"
                        + "  \"{\\n\\\"cm2\\\":\\n  {\\n  \\\"topo_info\\\":\\n    \\\"general"
                        + ".default:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".default_agg:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".default_sql:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".para_search_2:1:0:1976225101:100:2400309353:-1:true|general"
                        + ".para_search_4:1:0:1976225101:100:2400309353:-1:true|\\\"\\n  }\\n}\",\n"
                        + "\"signature\":\n"
                        + "  \"{\\\"table_info\\\": {\\\"in0\\\": {\\\"0\\\": {\\\"index_root\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/local_search_12000\\/general_0\\/runtimedata"
                        + "\\\", \\\"partitions\\\": {\\\"0_65535\\\": {\\\"inc_version\\\": 1}}, \\\"config_path\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/config\\/table\\/0\\\"}}}, \\\"biz_info\\\": "
                        + "{\\\"default\\\": {\\\"config_path\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/config\\/bizs\\/0\\\"}}, \\\"service_info\\\":"
                        + " {\\\"zone_name\\\": \\\"general\\\", \\\"version\\\": 0, \\\"part_count\\\": 1, "
                        + "\\\"part_id\\\": 0}, \\\"clean_disk\\\": false}\"\n"
                        + "}";
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                } else {
                    String response = "{\n"
                        + "\"customInfo\":\n"
                        + "  \"{\\n\\\"app_info\\\":\\n  {\\n  \\\"config_path\\\":\\n    \\\"\\\",\\n  "
                        + "\\\"keep_count\\\":\\n    20\\n  },\\n\\\"biz_info\\\":\\n  {\\n  \\\"default\\\":\\n    {\\n    "
                        + "\\\"config_path\\\":\\n      \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/config\\\\\\/bizs\\\\\\/0\\\",\\n    \\\"custom_biz_info\\\":\\n      {\\n      },\\n    "
                        + "\\\"keep_count\\\":\\n      5\\n    }\\n  },\\n\\\"custom_app_info\\\":\\n  {\\n  },"
                        + "\\n\\\"service_info\\\":\\n  {\\n  \\\"cm2\\\":\\n    {\\n    \\\"topo_info\\\":\\n      "
                        + "\\\"general.default:1:0:553898268:100:2400309353:-1:true|general"
                        + ".default_agg:1:0:553898268:100:2400309353:-1:true|general"
                        + ".default_sql:1:0:553898268:100:2400309353:-1:true|general"
                        + ".para_search_2:1:0:553898268:100:2400309353:-1:true|general"
                        + ".para_search_4:1:0:553898268:100:2400309353:-1:true|\\\"\\n    }\\n  },\\n\\\"table_info\\\":\\n  "
                        + "{\\n  \\\"in0\\\":\\n    {\\n    \\\"0\\\":\\n      {\\n      \\\"config_path\\\":\\n        "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/config\\\\\\/table\\\\\\/0"
                        + "\\\",\\n      \\\"force_online\\\":\\n        false,\\n      \\\"group_name\\\":\\n        "
                        + "\\\"default_group_name\\\",\\n      \\\"index_root\\\":\\n        "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\",\\n      \\\"partitions\\\":\\n        {\\n        "
                        + "\\\"0_65535\\\":\\n          {\\n          \\\"check_index_path\\\":\\n            \\\"\\\",\\n   "
                        + "       \\\"deploy_status\\\":\\n            2,\\n          \\\"deploy_status_map\\\":\\n          "
                        + "  [\\n              [\\n                1,\\n                {\\n                "
                        + "\\\"deploy_status\\\":\\n                  2,\\n                \\\"local_config_path\\\":\\n     "
                        + "             \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/local_search_12000\\\\\\/general_0\\\\\\/zone_config\\\\\\/table\\\\\\/in0\\\\\\/0"
                        + "\\\\\\/\\\"\\n                }\\n              ]\\n            ],\\n          "
                        + "\\\"inc_version\\\":\\n            1,\\n          \\\"keep_count\\\":\\n            1,\\n         "
                        + " \\\"loaded_config_path\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/config\\\\\\/table\\\\\\/0"
                        + "\\\",\\n          \\\"loaded_index_root\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\",\\n          \\\"local_index_path\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\\\\/in0\\\\\\/generation_0\\\\\\/partition_0_65535\\\",\\n   "
                        + "       \\\"rt_status\\\":\\n            0,\\n          \\\"schema_version\\\":\\n            0,\\n"
                        + "          \\\"table_load_type\\\":\\n            0,\\n          \\\"table_status\\\":\\n          "
                        + "  6,\\n          \\\"table_type\\\":\\n            0\\n          }\\n        },\\n      "
                        + "\\\"raw_index_root\\\":\\n        \\\"\\\",\\n      \\\"rt_status\\\":\\n        0,\\n      "
                        + "\\\"timestamp_to_skip\\\":\\n        -1\\n      }\\n    },\\n  \\\"test_ha\\\":\\n    {\\n    "
                        + "\\\"0\\\":\\n      {\\n      \\\"config_path\\\":\\n        "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/config\\\\\\/table\\\\\\/0"
                        + "\\\",\\n      \\\"force_online\\\":\\n        false,\\n      \\\"group_name\\\":\\n        "
                        + "\\\"default_group_name\\\",\\n      \\\"index_root\\\":\\n        "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\",\\n      \\\"partitions\\\":\\n        {\\n        "
                        + "\\\"0_65535\\\":\\n          {\\n          \\\"check_index_path\\\":\\n            \\\"\\\",\\n   "
                        + "       \\\"deploy_status\\\":\\n            2,\\n          \\\"deploy_status_map\\\":\\n          "
                        + "  [\\n              [\\n                0,\\n                {\\n                "
                        + "\\\"deploy_status\\\":\\n                  2,\\n                \\\"local_config_path\\\":\\n     "
                        + "             \\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask"
                        + "\\\\\\/local_search_12000\\\\\\/general_0\\\\\\/zone_config\\\\\\/table\\\\\\/test_ha\\\\\\/0"
                        + "\\\\\\/\\\"\\n                }\\n              ]\\n            ],\\n          "
                        + "\\\"inc_version\\\":\\n            0,\\n          \\\"keep_count\\\":\\n            1,\\n         "
                        + " \\\"loaded_config_path\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/config\\\\\\/table\\\\\\/0"
                        + "\\\",\\n          \\\"loaded_index_root\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\",\\n          \\\"local_index_path\\\":\\n            "
                        + "\\\"\\\\\\/usr\\\\\\/share\\\\\\/havenask\\\\\\/data_havenask\\\\\\/local_search_12000"
                        + "\\\\\\/general_0\\\\\\/runtimedata\\\\\\/test_ha\\\\\\/generation_0\\\\\\/partition_0_65535\\\","
                        + "\\n          \\\"rt_status\\\":\\n            0,\\n          \\\"schema_version\\\":\\n           "
                        + " 0,\\n          \\\"table_load_type\\\":\\n            0,\\n          \\\"table_status\\\":\\n    "
                        + "        6,\\n          \\\"table_type\\\":\\n            0\\n          }\\n        },\\n      "
                        + "\\\"raw_index_root\\\":\\n        \\\"\\\",\\n      \\\"rt_status\\\":\\n        0,\\n      "
                        + "\\\"timestamp_to_skip\\\":\\n        -1\\n      }\\n    }\\n  }\\n}\",\n"
                        + "\"serviceInfo\":\n"
                        + "  \"{\\n\\\"cm2\\\":\\n  {\\n  \\\"topo_info\\\":\\n    \\\"general"
                        + ".default:1:0:553898268:100:2400309353:-1:true|general"
                        + ".default_agg:1:0:553898268:100:2400309353:-1:true|general"
                        + ".default_sql:1:0:553898268:100:2400309353:-1:true|general"
                        + ".para_search_2:1:0:553898268:100:2400309353:-1:true|general"
                        + ".para_search_4:1:0:553898268:100:2400309353:-1:true|\\\"\\n  }\\n}\",\n"
                        + "\"signature\":\n"
                        + "  \"{\\\"table_info\\\": {\\\"in0\\\": {\\\"0\\\": {\\\"index_root\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/local_search_12000\\/general_0\\/runtimedata\\\", "
                        + "\\\"partitions\\\": {\\\"0_65535\\\": {\\\"inc_version\\\": 1}}, \\\"config_path\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/config\\/table\\/0\\\"}}, \\\"test_ha\\\": "
                        + "{\\\"0\\\": {\\\"index_root\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/local_search_12000\\/general_0\\/runtimedata\\\", "
                        + "\\\"partitions\\\": {\\\"0_65535\\\": {\\\"inc_version\\\": 0}}, \\\"config_path\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/config\\/table\\/0\\\"}}}, \\\"biz_info\\\": "
                        + "{\\\"default\\\": {\\\"config_path\\\": "
                        + "\\\"\\/usr\\/share\\/havenask\\/data_havenask\\/config\\/bizs\\/0\\\"}}, \\\"service_info\\\": "
                        + "{\\\"zone_name\\\": \\\"general\\\", \\\"version\\\": 0, \\\"part_count\\\": 1, \\\"part_id\\\": "
                        + "0}, \\\"clean_disk\\\": false}\"\n"
                        + "}";
                    OutputStream os = exchange.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                }
            });
            searcherServer.start();
        }
    }

    @AfterClass
    public static void stop() throws Exception {
        if (qrsServer != null) {
            qrsServer.removeContext("/sql");
            qrsServer.removeContext("/sqlClientInfo");
            qrsServer.stop(0);
            qrsServer = null;
        }

        if (searcherServer != null) {
            searcherServer.removeContext("/HeartbeatService/heartbeat");
            searcherServer.stop(0);
            searcherServer = null;
        }
    }

}
