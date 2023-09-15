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

package org.havenask.engine.search.action;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.havenask.ArpcThreadLeakFilter;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.common.SuppressForbidden;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.HavenaskITTestCase;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction.Request;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.transport.nio.MockNioTransportPlugin;

import static org.hamcrest.CoreMatchers.containsString;

@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class, ArpcThreadLeakFilter.class })
@HavenaskIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, scope = HavenaskIntegTestCase.Scope.TEST)
public class SqlActionIT extends HavenaskITTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .putList("node.roles", Arrays.asList("master", "data", "ingest"));
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class, MockNioTransportPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testSqlAction() {
        HavenaskSqlRequest request = new HavenaskSqlRequest("select * from test", null);
        HavenaskSqlResponse response = client().execute(HavenaskSqlAction.INSTANCE, request).actionGet();
        assertEquals(200, response.getResultCode());
        assertThat(response.getResult(), containsString("total_time"));
    }

    // TODO 暂时注释该方法,可能导致server停掉后,其他tests失败
    // public void testSqlActionFailed() {
    // // stop mock server
    // server.stop(0);
    // HavenaskSqlRequest request = new HavenaskSqlRequest("select * from test", null);
    // HavenaskSqlResponse response = client().execute(HavenaskSqlAction.INSTANCE, request).actionGet();
    // assertEquals(500, response.getResultCode());
    // assertThat(response.getResult(), Matchers.containsString("execute sql failed"));
    // }

    // test SqlClientInfo
    public void testSqlClientInfoAction() {
        HavenaskSqlClientInfoAction.Request request = new Request();
        HavenaskSqlClientInfoAction.Response response = client().execute(HavenaskSqlClientInfoAction.INSTANCE, request).actionGet();
        if (response.getErrorCode() == 0) {
            assertEquals("", response.getErrorMessage());
            Map<String, Object> exceptResult = XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                "{\"default\":{\"general\":{\"tables\":{\"test2_0\":{\"catalog_name\":\"default\","
                    + "\"database_name\":\"general\",\"version\":1,\"content\":{\"valid\":true,"
                    + "\"join_info\":{\"valid\":true,\"table_name\":\"\",\"join_field\":\"\"},\"sub_tables\":[],"
                    + "\"fields\":[{\"valid\":true,\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,"
                    + "\"extend_infos\":{},\"type\":\"STRING\",\"fieldType\":\"FT_STRING\",\"record_types\":null},"
                    + "\"index_name\":\"_routing\",\"field_name\":\"_routing\",\"index_type\":\"STRING\"},"
                    + "{\"valid\":true,\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,"
                    + "\"extend_infos\":{},\"type\":\"STRING\",\"fieldType\":\"FT_STRING\",\"record_types\":null},"
                    + "\"index_name\":\"f1\",\"field_name\":\"f1\",\"index_type\":\"STRING\"},{\"valid\":true,"
                    + "\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,\"extend_infos\":{},"
                    + "\"type\":\"LONG\",\"fieldType\":\"FT_INT64\",\"record_types\":null},\"index_name\":\"f3\","
                    + "\"field_name\":\"f3\",\"index_type\":\"NUMBER\"},{\"valid\":true,\"field_type\":{\"valid\":true,"
                    + "\"value_type\":null,\"key_type\":null,\"extend_infos\":{},\"type\":\"LONG\","
                    + "\"fieldType\":\"FT_INT64\",\"record_types\":null},\"index_name\":\"_seq_no\","
                    + "\"field_name\":\"_seq_no\",\"index_type\":\"NUMBER\"},{\"valid\":true,"
                    + "\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,\"extend_infos\":{},"
                    + "\"type\":\"STRING\",\"fieldType\":\"FT_STRING\",\"record_types\":null},\"index_name\":\"_id\","
                    + "\"field_name\":\"_id\",\"index_type\":\"PRIMARYKEY64\"},{\"valid\":true,"
                    + "\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,\"extend_infos\":{},"
                    + "\"type\":\"LONG\",\"fieldType\":\"FT_INT64\",\"record_types\":null},\"index_name\":\"\","
                    + "\"field_name\":\"_version\",\"index_type\":\"\"},{\"valid\":true,\"field_type\":{\"valid\":true,"
                    + "\"value_type\":null,\"key_type\":null,\"extend_infos\":{},\"type\":\"LONG\","
                    + "\"fieldType\":\"FT_INT64\",\"record_types\":null},\"index_name\":\"\","
                    + "\"field_name\":\"_primary_term\",\"index_type\":\"\"}],\"distribution\":{\"valid\":true,"
                    + "\"hash_fields\":[\"_id\"],\"hash_function\":\"HASH\",\"hash_params\":{},\"partition_cnt\":1},"
                    + "\"table_name\":\"test2_0\",\"properties\":{},\"table_type\":\"normal\"}},"
                    + "\"test2_0_summary_\":{\"catalog_name\":\"default\",\"database_name\":\"general\",\"version\":1,"
                    + "\"content\":{\"valid\":true,\"join_info\":{\"valid\":true,\"table_name\":\"\","
                    + "\"join_field\":\"\"},\"sub_tables\":[],\"fields\":[{\"valid\":true,\"field_type\":{\"valid\":true,"
                    + "\"value_type\":null,\"key_type\":null,\"extend_infos\":{},\"type\":\"STRING\","
                    + "\"fieldType\":\"FT_STRING\",\"record_types\":null},\"index_name\":\"_routing\","
                    + "\"field_name\":\"_routing\",\"index_type\":\"STRING\"},{\"valid\":true,"
                    + "\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,\"extend_infos\":{},"
                    + "\"type\":\"STRING\",\"fieldType\":\"FT_STRING\",\"record_types\":null},\"index_name\":\"f1\","
                    + "\"field_name\":\"f1\",\"index_type\":\"STRING\"},{\"valid\":true,\"field_type\":{\"valid\":true,"
                    + "\"value_type\":null,\"key_type\":null,\"extend_infos\":{},\"type\":\"LONG\","
                    + "\"fieldType\":\"FT_INT64\",\"record_types\":null},\"index_name\":\"f3\",\"field_name\":\"f3\","
                    + "\"index_type\":\"NUMBER\"},{\"valid\":true,\"field_type\":{\"valid\":true,\"value_type\":null,"
                    + "\"key_type\":null,\"extend_infos\":{},\"type\":\"LONG\",\"fieldType\":\"FT_INT64\","
                    + "\"record_types\":null},\"index_name\":\"_seq_no\",\"field_name\":\"_seq_no\","
                    + "\"index_type\":\"NUMBER\"},{\"valid\":true,\"field_type\":{\"valid\":true,\"value_type\":null,"
                    + "\"key_type\":null,\"extend_infos\":{},\"type\":\"STRING\",\"fieldType\":\"FT_STRING\","
                    + "\"record_types\":null},\"index_name\":\"\",\"field_name\":\"_source\",\"index_type\":\"\"},"
                    + "{\"valid\":true,\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,"
                    + "\"extend_infos\":{},\"type\":\"STRING\",\"fieldType\":\"FT_STRING\",\"record_types\":null},"
                    + "\"index_name\":\"_id\",\"field_name\":\"_id\",\"index_type\":\"PRIMARYKEY64\"},{\"valid\":true,"
                    + "\"field_type\":{\"valid\":true,\"value_type\":null,\"key_type\":null,\"extend_infos\":{},"
                    + "\"type\":\"LONG\",\"fieldType\":\"FT_INT64\",\"record_types\":null},\"index_name\":\"\","
                    + "\"field_name\":\"_version\",\"index_type\":\"\"},{\"valid\":true,\"field_type\":{\"valid\":true,"
                    + "\"value_type\":null,\"key_type\":null,\"extend_infos\":{},\"type\":\"LONG\","
                    + "\"fieldType\":\"FT_INT64\",\"record_types\":null},\"index_name\":\"\","
                    + "\"field_name\":\"_primary_term\",\"index_type\":\"\"}],\"distribution\":{\"valid\":true,"
                    + "\"hash_fields\":[\"_id\"],\"hash_function\":\"HASH\",\"hash_params\":{},\"partition_cnt\":1},"
                    + "\"table_name\":\"test2_0_summary_\",\"properties\":{},\"table_type\":\"summary\"}}},"
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
                    + "\"__default\":{\"tables\":{},\"functions\":{}}}}",
                true
            );
            assertEquals(exceptResult, response.getResult());
        } else {
            assertEquals(400, response.getErrorCode());
            assertEquals("execute failed", response.getErrorMessage());
            assertEquals(Collections.emptyMap(), response.getResult());
        }
    }
}
