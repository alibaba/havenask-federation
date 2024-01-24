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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.rpc.HavenaskClient;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class HavenaskHttpClient implements HavenaskClient {
    private static final Logger LOGGER = LogManager.getLogger(HavenaskHttpClient.class);
    private static final String HEART_BEAT_URL = "/HeartbeatService/heartbeat";

    private final static long SOCKET_TIMEOUT = 120;
    protected OkHttpClient client;
    protected final String url;

    public HavenaskHttpClient(int port) {
        this(port, SOCKET_TIMEOUT);
    }

    public HavenaskHttpClient(int port, long socketTimeout) {
        this.url = "http://127.0.0.1:" + port;
        client = AccessController.doPrivileged(
            (PrivilegedAction<OkHttpClient>) () -> new OkHttpClient.Builder().readTimeout(socketTimeout, TimeUnit.SECONDS).build()
        );
    }

    @Override
    public HeartbeatTargetResponse getHeartbeatTarget() throws IOException {
        Request request = new Request.Builder().url(url + HEART_BEAT_URL).build();
        Response response = AccessController.doPrivileged((PrivilegedAction<Response>) () -> {
            try {
                return client.newCall(request).execute();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        try (
            XContentParser parser = JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.body().byteStream())
        ) {
            HeartbeatTargetResponse heartbeatTargetResponse = HeartbeatTargetResponse.fromXContent(parser);
            return heartbeatTargetResponse;
        }
    }

    @Override
    public HeartbeatTargetResponse updateHeartbeatTarget(UpdateHeartbeatTargetRequest request) throws IOException {
        RequestBody body = RequestBody.create(MediaType.parse("application/json"), Strings.toString(request));
        Long start = System.nanoTime();
        Request httpRequest = new Request.Builder().url(url + HEART_BEAT_URL).post(body).build();

        Response response = AccessController.doPrivileged((PrivilegedAction<Response>) () -> {
            try {
                return client.newCall(httpRequest).execute();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Long end = System.nanoTime();
        LOGGER.trace("updateHeartbeatTarget cost [{}] ms", (end - start) / 1000000);
        try (
            XContentParser parser = JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.body().byteStream())
        ) {
            HeartbeatTargetResponse heartbeatTargetResponse = HeartbeatTargetResponse.fromXContent(parser);
            return heartbeatTargetResponse;
        }
    }
}
