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

package org.havenask.utils;

import java.io.IOException;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class HttpClientHelper {

    private static OkHttpClient httpClient;

    private HttpClientHelper() {}

    public static synchronized OkHttpClient getHTTPClient() {
        if (null == httpClient) {
            try {
                PermissionHelper.doPrivileged(() -> {
                    httpClient = new OkHttpClient();
                    return httpClient;
                });
            } catch (IOException e) {
                return httpClient;
            }
        }
        return httpClient;
    }

    public static Response httpRequest(String url) throws IOException {
        return PermissionHelper.doPrivileged(() -> {
            Request request = new Request.Builder().get().url(url).build();
            return getHTTPClient().newCall(request).execute();
        });
    }
}
