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

package org.havenask.engine.rpc.http;

import java.io.IOException;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.engine.HavenaskITTestCase;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.rpc.SqlClientInfoResponse;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;

import static org.hamcrest.CoreMatchers.containsString;

@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class })
public class QrsHttpClientIT extends HavenaskITTestCase {

    public void testSql() throws IOException {
        QrsClient client = new QrsHttpClient(49200);
        String sql = "select * from test";
        QrsSqlRequest request = new QrsSqlRequest(sql, null);
        QrsSqlResponse response = client.executeSql(request);
        assertEquals(200, response.getResultCode());
        assertThat(response.getResult(), containsString("total_time"));
    }

    public void testSqlClientInfo() throws IOException {
        QrsClient client = new QrsHttpClient(49200);
        SqlClientInfoResponse response = client.executeSqlClientInfo();
        if (response.getErrorCode() != 0) {
            assertEquals(response.getErrorCode(), 400);
            assertThat(response.getErrorMessage(), containsString("execute failed"));
        } else {
            assertThat(response.getResult().toString(), containsString("default"));
        }
    }

    public void testGetHeartbeatTarget() throws IOException {
        QrsClient client = new QrsHttpClient(49200);
        HeartbeatTargetResponse response = client.getHeartbeatTarget();
        String expectCustomInfo = "{\n"
            + "\t\"app_info\":{\n"
            + "\t\t\"config_path\":\"\",\n"
            + "\t\t\"keep_count\":20\n"
            + "\t},\n"
            + "\t\"biz_info\":{\n"
            + "\t\t\"default\":{\n"
            + "\t\t\t\"config_path\":\"/usr/share/havenask/data_havenask/config/bizs/0\",\n"
            + "\t\t\t\"custom_biz_info\":{},\n"
            + "\t\t\t\"keep_count\":5\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"custom_app_info\":{},\n"
            + "\t\"service_info\":{\n"
            + "\t\t\"cm2\":{\n"
            + "\t\t\t\"topo_info\":\"ha3.general.default:1:0:1457961441:100:2400309353:-1:true|"
            + "ha3.general.para_search_2:1:0:1457961441:100:2400309353:-1:true|"
            + "ha3.general.para_search_4:1:0:1457961441:100:2400309353:-1:true|\"\n"
            + "\t\t},\n"
            + "\t\t\"part_count\":0,\n"
            + "\t\t\"part_id\":0,\n"
            + "\t\t\"version\":0\n"
            + "\t},\n"
            + "\t\"table_info\":{},\n"
            + "\t\"target_version\":0\n"
            + "}";
        String expectServiceInfo = "{\n"
            + "\"cm2\":\n"
            + "  {\n"
            + "  \"topo_info\":\n"
            + "    \"ha3.general.default:1:0:1457961441:100:2400309353:-1:true|"
            + "ha3.general.para_search_2:1:0:1457961441:100:2400309353:-1:true|"
            + "ha3.general.para_search_4:1:0:1457961441:100:2400309353:-1:true|\"\n"
            + "  }\n"
            + "}";
        String expectSignature = "{\n"
            + "\t\"biz_info\":{\n"
            + "\t\t\"default\":{\n"
            + "\t\t\t\"config_path\":\"/usr/share/havenask/data_havenask/config/bizs/0\"\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"clean_disk\":false,\n"
            + "\t\"service_info\":{\n"
            + "\t\t\"cm2_config\":{\n"
            + "\t\t\t\"local\":[\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.default\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":553898268\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.para_search_4\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":553898268\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.default_sql\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":553898268\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.para_search_2\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":553898268\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.default_agg\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":553898268\n"
            + "\t\t\t\t}\n"
            + "\t\t\t]\n"
            + "\t\t},\n"
            + "\t\t\"part_count\":0,\n"
            + "\t\t\"part_id\":0,\n"
            + "\t\t\"version\":0,\n"
            + "\t\t\"zone_name\":\"qrs\"\n"
            + "\t},\n"
            + "\t\"table_info\":{},\n"
            + "\t\"target_version\":0\n"
            + "}";

        // assertEquals(expectCustomInfo, response.getCustomInfo().toString());
        // assertEquals(expectServiceInfo, response.getServiceInfo());
        // assertEquals(expectSignature, response.getSignature().toString());
    }

    public void testUpdateHeartbeatTarget() throws IOException {
        String targetStr = "{\"table_info\": {}, \"biz_info\": {\"default\": {\"config_path\": "
            + "\"/usr/share/havenask/data_havenask/config/bizs/0\"}}, \"service_info\": {\"zone_name\": \"qrs\", "
            + "\"cm2_config\": {\"local\": [{\"part_count\": 1, \"biz_name\": \"general.default\", \"ip\": \"172.17.0"
            + ".2\", \"version\": 1976225101, \"part_id\": 0, \"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\":"
            + " \"general.para_search_4\", \"ip\": \"172.17.0.2\", \"version\": 1976225101, \"part_id\": 0, "
            + "\"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\": \"general.default_sql\", \"ip\": \"172.17.0"
            + ".2\", \"version\": 1976225101, \"part_id\": 0, \"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\":"
            + " \"general.para_search_2\", \"ip\": \"172.17.0.2\", \"version\": 1976225101, \"part_id\": 0, "
            + "\"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\": \"general.default_agg\", \"ip\": \"172.17.0"
            + ".2\", \"version\": 1976225101, \"part_id\": 0, \"tcp_port\": 39300}]}, \"part_id\": 0, \"part_count\":"
            + " 0}, \"clean_disk\": false}";
        TargetInfo targetInfo = TargetInfo.parse(targetStr);
        UpdateHeartbeatTargetRequest request = new UpdateHeartbeatTargetRequest(targetInfo);
        QrsClient client = new QrsHttpClient(49200);
        HeartbeatTargetResponse response = client.updateHeartbeatTarget(request);
        String responseTargetStr = "{\n"
            + "\t\"app_info\":{\n"
            + "\t\t\"config_path\":\"\",\n"
            + "\t\t\"keep_count\":20\n"
            + "\t},\n"
            + "\t\"biz_info\":{\n"
            + "\t\t\"default\":{\n"
            + "\t\t\t\"config_path\":\"/usr/share/havenask/data_havenask/config/bizs/0\",\n"
            + "\t\t\t\"custom_biz_info\":{},\n"
            + "\t\t\t\"keep_count\":5\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"custom_app_info\":{},\n"
            + "\t\"service_info\":{\n"
            + "\t\t\"cm2\":{\n"
            + "\t\t\t\"topo_info\":\"ha3.general.default:1:0:1457961441:100:2400309353:-1:true|ha3.general.para_search"
            + "_2:1:0:1457961441:100:2400309353:-1:true|ha3.general.para_search_4:1:0:1457961441:100:2400309353:-1:true|\"\n"
            + "\t\t},\n"
            + "\t\t\"part_count\":0,\n"
            + "\t\t\"part_id\":0,\n"
            + "\t\t\"version\":0\n"
            + "\t},\n"
            + "\t\"table_info\":{},\n"
            + "\t\"target_version\":0\n"
            + "}";
        String serviceInfoStr = "{\n"
            + "\"cm2\":\n"
            + "  {\n"
            + "  \"topo_info\":\n"
            + "    \"ha3.general.default:1:0:1457961441:100:2400309353:-1:true|"
            + "ha3.general.para_search_2:1:0:1457961441:100:2400309353:-1:true|"
            + "ha3.general.para_search_4:1:0:1457961441:100:2400309353:-1:true|\"\n"
            + "  }\n"
            + "}";
        String signatureStr = "{\n"
            + "\t\"biz_info\":{\n"
            + "\t\t\"default\":{\n"
            + "\t\t\t\"config_path\":\"/usr/share/havenask/data_havenask/config/bizs/0\"\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"clean_disk\":false,\n"
            + "\t\"service_info\":{\n"
            + "\t\t\"cm2_config\":{\n"
            + "\t\t\t\"local\":[\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.default\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":1976225101\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.para_search_4\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":1976225101\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.default_sql\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":1976225101\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.para_search_2\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":1976225101\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t{\n"
            + "\t\t\t\t\t\"biz_name\":\"general.default_agg\",\n"
            + "\t\t\t\t\t\"grpc_port\":0,\n"
            + "\t\t\t\t\t\"ip\":\"172.17.0.2\",\n"
            + "\t\t\t\t\t\"part_count\":1,\n"
            + "\t\t\t\t\t\"part_id\":0,\n"
            + "\t\t\t\t\t\"support_heartbeat\":false,\n"
            + "\t\t\t\t\t\"tcp_port\":39300,\n"
            + "\t\t\t\t\t\"version\":1976225101\n"
            + "\t\t\t\t}\n"
            + "\t\t\t]\n"
            + "\t\t},\n"
            + "\t\t\"part_count\":0,\n"
            + "\t\t\"part_id\":0,\n"
            + "\t\t\"version\":0,\n"
            + "\t\t\"zone_name\":\"qrs\"\n"
            + "\t},\n"
            + "\t\"table_info\":{},\n"
            + "\t\"target_version\":0\n"
            + "}";

        // assertEquals(signatureStr, response.getSignature().toString());
        // assertEquals(responseTargetStr, response.getCustomInfo().toString());
        // assertEquals(serviceInfoStr, response.getServiceInfo());

        // TODO 结果是对比这两个是否相等
        // assertEquals(targetInfo, response.getSignature());
    }
}
