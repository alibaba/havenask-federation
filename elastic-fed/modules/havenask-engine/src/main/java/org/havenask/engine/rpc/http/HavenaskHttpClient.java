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

import static org.havenask.common.xcontent.XContentType.JSON;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.client.Request;
import org.havenask.client.Response;
import org.havenask.client.RestClient;
import org.havenask.client.RestClientBuilder;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.rpc.HavenaskClient;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;

public class HavenaskHttpClient implements HavenaskClient {
    private static final Logger LOGGER = LogManager.getLogger(HavenaskHttpClient.class);
    private static final String HEART_BEAT_URL = "/HeartbeatService/heartbeat";

    private static final int SOCKET_TIMEOUT = 1200000;
    private static final int MAX_CONNECTION = 1000;
    private static final String LOCALHOST = "127.0.0.1";

    private RestClient client;
    private final int port;
    private final int socketTimeout;

    public HavenaskHttpClient(int port) {
        this(port, SOCKET_TIMEOUT);
    }

    public HavenaskHttpClient(int port, final int socketTimeout) {
        this.port = port;
        this.socketTimeout = socketTimeout;
    }

    protected synchronized RestClient getClient() {
        if (client == null) {
            client = RestClient.builder(new HttpHost(LOCALHOST, port, "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setMaxConnPerRoute(MAX_CONNECTION).setMaxConnTotal(MAX_CONNECTION);

                    }
                })
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setSocketTimeout(socketTimeout);
                    }
                })
                .build();
        }
        return client;
    }

    @Override
    public HeartbeatTargetResponse getHeartbeatTarget() throws IOException {
        Request request = new org.havenask.client.Request("GET", HEART_BEAT_URL);
        Response response = getClient().performRequest(request);
        try (
            XContentParser parser = JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent())
        ) {
            HeartbeatTargetResponse heartbeatTargetResponse = HeartbeatTargetResponse.fromXContent(parser);
            return heartbeatTargetResponse;
        }
    }

    @Override
    public HeartbeatTargetResponse updateHeartbeatTarget(UpdateHeartbeatTargetRequest updateHeartbeatTargetRequest) throws IOException {
        Long start = System.nanoTime();
        Request request = new org.havenask.client.Request("POST", HEART_BEAT_URL);
        request.setJsonEntity(Strings.toString(updateHeartbeatTargetRequest));

        Response response = getClient().performRequest(request);
        Long end = System.nanoTime();
        LOGGER.trace("updateHeartbeatTarget cost [{}] ms", (end - start) / 1000000);
        try (
            XContentParser parser = JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent())
        ) {
            HeartbeatTargetResponse heartbeatTargetResponse = HeartbeatTargetResponse.fromXContent(parser);
            return heartbeatTargetResponse;
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
