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

package org.havenask.engine;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;
import org.havenask.common.SuppressForbidden;
import org.havenask.test.HavenaskIntegTestCase;
import org.junit.After;
import org.junit.Before;

@SuppressForbidden(reason = "use a http server")
public abstract class HavenaskITTestCase extends HavenaskIntegTestCase {
    protected HttpServer server;

    @Before
    public void setup() throws IOException {
        server = HttpServer.create(new InetSocketAddress(49200), 0);
        server.createContext("/sql", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            String response = "sql result";
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        });
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        server.removeContext("/sql");
        server.stop(0);
    }

}
