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
}
