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

package org.havenask.cluster.remote.test;

import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.Before;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.client.RequestOptions;
import org.havenask.client.RestClient;
import org.havenask.client.RestHighLevelClient;
import org.havenask.common.settings.Settings;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.test.rest.HavenaskRestTestCase;

import java.io.IOException;
import java.util.Collections;

public abstract class AbstractMultiClusterRemoteTestCase extends HavenaskRestTestCase {

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private static RestHighLevelClient cluster1Client;
    private static RestHighLevelClient cluster2Client;
    private static boolean initialized = false;


    @Override
    protected String getTestRestCluster() {
        return "localhost:" + getProperty("test.fixtures.havenask-1.tcp.9200");
    }

    @Before
    public void initClientsAndConfigureClusters() throws Exception {
        if (initialized) {
            return;
        }

        cluster1Client = buildClient("localhost:" + getProperty("test.fixtures.havenask-1.tcp.9200"));
        cluster2Client = buildClient("localhost:" + getProperty("test.fixtures.havenask-2.tcp.9200"));

        cluster1Client().cluster().health(new ClusterHealthRequest().waitForNodes("1").waitForYellowStatus(), RequestOptions.DEFAULT);
        cluster2Client().cluster().health(new ClusterHealthRequest().waitForNodes("1").waitForYellowStatus(), RequestOptions.DEFAULT);

        initialized = true;
    }


    @AfterClass
    public static void destroyClients() throws IOException {
        try {
            IOUtils.close(cluster1Client, cluster2Client);
        } finally {
            cluster1Client = null;
            cluster2Client = null;
        }
    }

    protected static RestHighLevelClient cluster1Client() {
        return cluster1Client;
    }

    protected static RestHighLevelClient cluster2Client() {
        return cluster2Client;
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, RestClient::close, Collections.emptyList());
        }
    }

    private RestHighLevelClient buildClient(final String url) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)), getProtocol());
        return new HighLevelClient(buildClient(restAdminSettings(), new HttpHost[]{httpHost}));
    }

    @Override
    protected Settings restClientSettings() {
        return super.restClientSettings();
    }

    @Override
    protected String getProtocol() {
        return "http";
    }

    private String getProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("Could not find system properties from test.fixtures. " +
                "This test expects to run with the havenask.test.fixtures Gradle plugin");
        }
        return value;
    }
}
