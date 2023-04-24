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

import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;

public class QrsHttpClient extends HavenaskHttpClient implements QrsClient {
    private static final Logger logger = LogManager.getLogger(QrsHttpClient.class);
    private static final String SQL_URL = "/sql";

    public QrsHttpClient(int port) {
        super(port);
    }

    @Override
    public QrsSqlResponse executeSql(QrsSqlRequest qrsSqlRequest) throws IOException {
        HttpUrl.Builder urlBuilder = HttpUrl.parse(url + SQL_URL).newBuilder();
        String query = qrsSqlRequest.getSql();
        if (qrsSqlRequest.getKvpair() != null) {
            query += "&&kvpair=" + qrsSqlRequest.getKvpair();
        }
        urlBuilder.addQueryParameter("query", query);
        String url = urlBuilder.build().toString();
        logger.debug("execute sql: {}", url);
        Request request = new Request.Builder().url(url).build();
        Response response = client.newCall(request).execute();
        return new QrsSqlResponse(response.body().string(), response.code());
    }
}
