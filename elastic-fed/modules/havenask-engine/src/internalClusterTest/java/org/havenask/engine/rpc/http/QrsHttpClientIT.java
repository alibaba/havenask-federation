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

import static org.hamcrest.CoreMatchers.containsString;

@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class })
public class QrsHttpClientIT extends HavenaskITTestCase {

    public void testSql() throws IOException {
        QrsClient client = new QrsHttpClient(49200);
        String sql = "select * from test";
        QrsSqlRequest request = new QrsSqlRequest(sql, null);
        QrsSqlResponse response = client.executeSql(request);
        assertEquals(200, response.getResultCode());
        assertEquals("sql result", response.getResult());
    }

    public void testSqlClientInfo() throws IOException {
        QrsClient client = new QrsHttpClient(49200);
        String result = client.executeSqlClientInfo();
        assertThat(result, containsString("error_message"));
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
            + "\t\t\t\"topo_info\":\"ha3.general.default:1:0:1457961441:100:2400309353:-1:true|ha3.general"
            + ".para_search_2:1:0:1457961441:100:2400309353:-1:true|ha3.general"
            + ".para_search_4:1:0:1457961441:100:2400309353:-1:true|\"\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"table_info\":{}\n"
            + "}";
        String expectServiceInfo = "{\n"
            + "\"cm2\":\n"
            + "  {\n"
            + "  \"topo_info\":\n"
            + "    \"ha3.general.default:1:0:1457961441:100:2400309353:-1:true|ha3.general"
            + ".para_search_2:1:0:1457961441:100:2400309353:-1:true|ha3.general"
            + ".para_search_4:1:0:1457961441:100:2400309353:-1:true|\"\n"
            + "  }\n"
            + "}";
        String expectSignature = "{\"table_info\": {}, \"biz_info\": {\"default\": {\"config_path\": "
            + "\"/usr/share/havenask/data_havenask/config/bizs/0\"}}, \"service_info\": {\"zone_name\": \"qrs\", "
            + "\"cm2_config\": {\"local\": [{\"part_count\": 1, \"biz_name\": \"general.default\", \"ip\": \"172.17.0"
            + ".2\", \"version\": 553898268, \"part_id\": 0, \"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\": "
            + "\"general.para_search_4\", \"ip\": \"172.17.0.2\", \"version\": 553898268, \"part_id\": 0, "
            + "\"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\": \"general.default_sql\", \"ip\": \"172.17.0"
            + ".2\", \"version\": 553898268, \"part_id\": 0, \"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\": "
            + "\"general.para_search_2\", \"ip\": \"172.17.0.2\", \"version\": 553898268, \"part_id\": 0, "
            + "\"tcp_port\": 39300}, {\"part_count\": 1, \"biz_name\": \"general.default_agg\", \"ip\": \"172.17.0"
            + ".2\", \"version\": 553898268, \"part_id\": 0, \"tcp_port\": 39300}]}, \"part_id\": 0, \"part_count\": "
            + "0}, \"clean_disk\": false}";
        assertEquals(expectCustomInfo, response.getCustomInfo().toString());
        assertEquals(expectServiceInfo, response.getServiceInfo());
        assertEquals(expectSignature, response.getSignature());
    }
}
