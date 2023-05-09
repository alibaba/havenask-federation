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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.http;

import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.util.EntityUtils;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.test.rest.HavenaskRestTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

public class HttpCompressionIT extends HavenaskRestTestCase {

    private static final String GZIP_ENCODING = "gzip";
    private static final String SAMPLE_DOCUMENT = "{\n" +
        "   \"name\": {\n" +
        "      \"first name\": \"Steve\",\n" +
        "      \"last name\": \"Jobs\"\n" +
        "   }\n" +
        "}";

    public void testCompressesResponseIfRequested() throws IOException {
        Request request = new Request("POST", "/company/_doc/2");
        request.setJsonEntity(SAMPLE_DOCUMENT);
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), is(not(instanceOf(GzipDecompressingEntity.class))));

        request = new Request("GET", "/company/_doc/2");
        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING)
            .build();

        request.setOptions(requestOptions);
        response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(GZIP_ENCODING, response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), instanceOf(GzipDecompressingEntity.class));

        String body = EntityUtils.toString(response.getEntity());
        assertThat(body, containsString(SAMPLE_DOCUMENT));
    }

    public void testUncompressedResponseByDefault() throws IOException {
        Response response = client().performRequest(new Request("GET", "/"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));

        Request request = new Request("POST", "/company/_doc/1");
        request.setJsonEntity(SAMPLE_DOCUMENT);
        response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), is(not(instanceOf(GzipDecompressingEntity.class))));
    }

}
