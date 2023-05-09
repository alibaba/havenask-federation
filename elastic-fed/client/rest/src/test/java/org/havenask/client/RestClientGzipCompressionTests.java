/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RestClientGzipCompressionTests extends RestClientTestCase {

    private static HttpServer httpServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/", new GzipResponseHandler());
        httpServer.start();
    }

    @AfterClass
    public static void stopHttpServers() throws IOException {
        httpServer.stop(0);
        httpServer = null;
    }

    /**
     * A response handler that accepts gzip-encoded data and replies request and response encoding values
     * followed by the request body. The response is compressed if "Accept-Encoding" is "gzip".
     */
    private static class GzipResponseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            // Decode body (if any)
            String contentEncoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
            InputStream body = exchange.getRequestBody();
            if ("gzip".equals(contentEncoding)) {
                body = new GZIPInputStream(body);
            }
            byte[] bytes = readAll(body);

            boolean compress = "gzip".equals(exchange.getRequestHeaders().getFirst("Accept-Encoding"));
            if (compress) {
                exchange.getResponseHeaders().add("Content-Encoding", "gzip");
            }

            exchange.sendResponseHeaders(200, 0);

            // Encode response if needed
            OutputStream out = exchange.getResponseBody();
            if (compress) {
                out = new GZIPOutputStream(out);
            }

            // Outputs <request-encoding|null>#<response-encoding|null>#<request-body>
            out.write(String.valueOf(contentEncoding).getBytes(StandardCharsets.UTF_8));
            out.write('#');
            out.write((compress ? "gzip" : "null").getBytes(StandardCharsets.UTF_8));
            out.write('#');
            out.write(bytes);
            out.close();

            exchange.close();
        }
    }

    /** Read all bytes of an input stream and close it. */
    private static byte[] readAll(InputStream in) throws IOException {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int len = 0;
        while ((len = in.read(buffer)) > 0) {
            bos.write(buffer, 0, len);
        }
        in.close();
        return bos.toByteArray();
    }

    private RestClient createClient(boolean enableCompression) {
        InetSocketAddress address = httpServer.getAddress();
        return RestClient.builder(new HttpHost(address.getHostString(), address.getPort(), "http"))
            .setCompressionEnabled(enableCompression)
            .build();
    }

    public void testGzipHeaderSync() throws Exception {
        RestClient restClient = createClient(false);

        // Send non-compressed request, expect compressed response
        Request request = new Request("POST", "/");
        request.setEntity(new StringEntity("plain request, gzip response", ContentType.TEXT_PLAIN));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Accept-Encoding", "gzip").build());

        Response response = restClient.performRequest(request);

        HttpEntity entity = response.getEntity();
        String content = new String(readAll(entity.getContent()), StandardCharsets.UTF_8);
        Assert.assertEquals("null#gzip#plain request, gzip response", content);

        restClient.close();
    }

    public void testGzipHeaderAsync() throws Exception {
        RestClient restClient = createClient(false);

        // Send non-compressed request, expect compressed response
        Request request = new Request("POST", "/");
        request.setEntity(new StringEntity("plain request, gzip response", ContentType.TEXT_PLAIN));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Accept-Encoding", "gzip").build());

        FutureResponse futureResponse = new FutureResponse();
        restClient.performRequestAsync(request, futureResponse);
        Response response = futureResponse.get();

        HttpEntity entity = response.getEntity();
        String content = new String(readAll(entity.getContent()), StandardCharsets.UTF_8);
        Assert.assertEquals("null#gzip#plain request, gzip response", content);

        restClient.close();
    }

    public void testCompressingClientSync() throws Exception {
        RestClient restClient = createClient(true);

        Request request = new Request("POST", "/");
        request.setEntity(new StringEntity("compressing client", ContentType.TEXT_PLAIN));

        Response response = restClient.performRequest(request);

        HttpEntity entity = response.getEntity();
        String content = new String(readAll(entity.getContent()), StandardCharsets.UTF_8);
        Assert.assertEquals("gzip#gzip#compressing client", content);

        restClient.close();
    }

    public void testCompressingClientAsync() throws Exception {
        InetSocketAddress address = httpServer.getAddress();
        RestClient restClient = RestClient.builder(new HttpHost(address.getHostString(), address.getPort(), "http"))
            .setCompressionEnabled(true)
            .build();

        Request request = new Request("POST", "/");
        request.setEntity(new StringEntity("compressing client", ContentType.TEXT_PLAIN));

        FutureResponse futureResponse = new FutureResponse();
        restClient.performRequestAsync(request, futureResponse);
        Response response = futureResponse.get();

        // Server should report it had a compressed request and sent back a compressed response
        HttpEntity entity = response.getEntity();
        String content = new String(readAll(entity.getContent()), StandardCharsets.UTF_8);
        Assert.assertEquals("gzip#gzip#compressing client", content);

        restClient.close();
    }

    public static class FutureResponse extends CompletableFuture<Response> implements ResponseListener {
        @Override
        public void onSuccess(Response response) {
            this.complete(response);
        }

        @Override
        public void onFailure(Exception exception) {
            this.completeExceptionally(exception);
        }
    }
}
