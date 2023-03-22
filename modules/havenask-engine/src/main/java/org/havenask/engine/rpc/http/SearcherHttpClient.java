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

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;

import static org.havenask.common.xcontent.XContentType.JSON;

public class SearcherHttpClient implements SearcherClient {
    private static final Logger logger = LogManager.getLogger(SearcherHttpClient.class);
    private static final String HEART_BEAT_URL = "/HeartbeatService/heartbeat";

    private OkHttpClient client = new OkHttpClient();
    private final String url;

    public SearcherHttpClient(int port) {
        this.url = "http://127.0.0.1:" + port;
    }

    @Override
    public HeartbeatTargetResponse getHeartbeatTarget() throws IOException {
        Request request = new Request.Builder()
            .url(url + HEART_BEAT_URL)
            .build();
        Response response = client.newCall(request).execute();
        try (XContentParser parser = JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE, response.body()
                .byteStream())) {
            HeartbeatTargetResponse heartbeatTargetResponse = HeartbeatTargetResponse.fromXContent(parser);
            return heartbeatTargetResponse;
        }
    }

    @Override
    public HeartbeatTargetResponse updateHeartbeatTarget(UpdateHeartbeatTargetRequest request) throws IOException {
        RequestBody body = RequestBody.create(MediaType.parse("application/json"), Strings.toString(request));
        Request httpRequest = new Request.Builder()
            .url(url + HEART_BEAT_URL)
            .post(body)
            .build();
        Response response = client.newCall(httpRequest).execute();
        try (XContentParser parser = JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE, response.body()
                .byteStream())) {
            // TODO change to debug
            if (logger.isInfoEnabled()) {
                logger.info("updateHeartbeatTarget, request[{}], response[{}]", Strings.toString(request),
                    response.body().string());
            }

            HeartbeatTargetResponse heartbeatTargetResponse = HeartbeatTargetResponse.fromXContent(parser);
            return heartbeatTargetResponse;
        }
    }
}
