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

package org.havenask.client.documentation;

import org.apache.http.HttpHost;
import org.havenask.client.HavenaskRestHighLevelClientTestCase;
import org.havenask.client.RequestOptions;
import org.havenask.client.RestClient;
import org.havenask.client.RestHighLevelClient;
import org.havenask.client.core.MainResponse;

import java.io.IOException;

/**
 * Documentation for miscellaneous APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
public class MiscellaneousDocumentationIT extends HavenaskRestHighLevelClientTestCase {

    public void testMain() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::main-execute
            MainResponse response = client.info(RequestOptions.DEFAULT);
            //end::main-execute
            //tag::main-response
            String clusterName = response.getClusterName();
            String clusterUuid = response.getClusterUuid();
            String nodeName = response.getNodeName();
            MainResponse.Version version = response.getVersion();
            String buildDate = version.getBuildDate();
            String buildHash = version.getBuildHash();
            String buildType = version.getBuildType();
            String luceneVersion = version.getLuceneVersion();
            String minimumIndexCompatibilityVersion= version.getMinimumIndexCompatibilityVersion();
            String minimumWireCompatibilityVersion = version.getMinimumWireCompatibilityVersion();
            String number = version.getNumber();
            //end::main-response
            assertNotNull(clusterName);
            assertNotNull(clusterUuid);
            assertNotNull(nodeName);
            assertNotNull(version);
            assertNotNull(buildDate);
            assertNotNull(buildHash);
            assertNotNull(buildType);
            assertNotNull(luceneVersion);
            assertNotNull(minimumIndexCompatibilityVersion);
            assertNotNull(minimumWireCompatibilityVersion);
            assertNotNull(number);
        }
    }

    public void testPing() throws IOException {
        RestHighLevelClient client = highLevelClient();
        //tag::ping-execute
        boolean response = client.ping(RequestOptions.DEFAULT);
        //end::ping-execute
        assertTrue(response);
    }

    public void testInitializationFromClientBuilder() throws IOException {
        //tag::rest-high-level-client-init
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
        //end::rest-high-level-client-init

        //tag::rest-high-level-client-close
        client.close();
        //end::rest-high-level-client-close
    }
}
