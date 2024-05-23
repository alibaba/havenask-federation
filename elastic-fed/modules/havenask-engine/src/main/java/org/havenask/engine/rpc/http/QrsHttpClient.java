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

import org.apache.http.Consts;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.client.Request;
import org.havenask.client.Response;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.rpc.SqlClientInfoResponse;

import com.alibaba.fastjson.JSONObject;

public class QrsHttpClient extends HavenaskHttpClient implements QrsClient {
    private static final Logger logger = LogManager.getLogger(QrsHttpClient.class);
    private static final String SQL_URL = "/sql";
    private static final String SQL_TABLE_INFO_URL = "/sqlClientInfo";

    public QrsHttpClient(int port) {
        super(port);
    }

    public QrsHttpClient(int port, int socketTimeout) {
        super(port, socketTimeout);
    }

    @Override
    public QrsSqlResponse executeSql(QrsSqlRequest qrsSqlRequest) throws IOException {
        long start = System.nanoTime();
        Request request = new Request("POST", SQL_URL);
        String query = qrsSqlRequest.getSql();
        if (qrsSqlRequest.getKvpair() != null) {
            query += "&&kvpair=" + qrsSqlRequest.getKvpair();
        }
        request.setEntity(new NStringEntity(query, ContentType.create("text/plain", Consts.UTF_8)));
        Response response = getClient().performRequest(request);
        long end = System.nanoTime();
        logger.debug("execute sql: {} cost: {} us", qrsSqlRequest.getSql(), (end - start) / 1000);
        String responseString = EntityUtils.toString(response.getEntity(), Consts.UTF_8);
        return new QrsSqlResponse(responseString, response.getStatusLine().getStatusCode());
    }

    @Override
    public SqlClientInfoResponse executeSqlClientInfo() throws IOException {
        Request request = new Request("GET", SQL_TABLE_INFO_URL);
        Response response = getClient().performRequest(request);
        String responseStr = EntityUtils.toString(response.getEntity());
        JSONObject jsonObject = JSONObject.parseObject(responseStr);
        int errorCode = -1;
        String errorMessage = "execute sql client info api failed";
        JSONObject result = null;
        if (jsonObject.containsKey("error_code")) {
            errorCode = (int) jsonObject.get("error_code");
        }
        if (jsonObject.containsKey("error_message")) {
            errorMessage = (String) jsonObject.get("error_message");
        }
        if (jsonObject.containsKey("result")) {
            result = jsonObject.getJSONObject("result");
        }
        if (errorCode != 0) {
            return new SqlClientInfoResponse(errorMessage, errorCode);
        }
        return new SqlClientInfoResponse(result);
    }
}
