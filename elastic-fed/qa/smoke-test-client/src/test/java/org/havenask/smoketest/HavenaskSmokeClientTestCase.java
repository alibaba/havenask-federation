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

package org.havenask.smoketest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.LuceneTestCase;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.client.Client;
import org.havenask.client.transport.TransportClient;
import org.havenask.common.network.NetworkModule;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.env.Environment;
import org.havenask.plugins.Plugin;
import org.havenask.transport.client.PreBuiltTransportClient;
import org.havenask.transport.nio.MockNioTransportPlugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;
import static org.hamcrest.Matchers.notNullValue;

/**
 * An abstract base class to run integration tests against an Havenask cluster running outside of the test process.
 * <p>
 * You can define a list of transport addresses from where you can reach your cluster by setting "tests.cluster" system
 * property. It defaults to "localhost:9300". If you run this from `gradle integTest` then it will start the clsuter for
 * you and set up the property.
 * <p>
 * If you want to debug this module from your IDE, then start an external cluster by yourself, maybe with `gradle run`,
 * then run JUnit. If you changed the default port, set "-Dtests.cluster=localhost:PORT" when running your test.
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public abstract class HavenaskSmokeClientTestCase extends LuceneTestCase {

    /**
     * Key used to eventually switch to using an external cluster and provide its transport addresses
     */
    public static final String TESTS_CLUSTER = "tests.cluster";

    protected static final Logger logger = LogManager.getLogger(HavenaskSmokeClientTestCase.class);

    private static final AtomicInteger counter = new AtomicInteger();
    private static Client client;
    private static String clusterAddresses;
    protected String index;

    private static Client startClient(Path tempDir, TransportAddress... transportAddresses) {
        Settings.Builder builder = Settings.builder()
            .put("node.name", "qa_smoke_client_" + counter.getAndIncrement())
            .put("client.transport.ignore_cluster_name", true)
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir);
        final Collection<Class<? extends Plugin>> plugins;
        String transportKey = MockNioTransportPlugin.MOCK_NIO_TRANSPORT_NAME;
        Class<? extends Plugin> transportPlugin = MockNioTransportPlugin.class;
        if (random().nextBoolean()) {
            builder.put(NetworkModule.TRANSPORT_TYPE_KEY, transportKey);
            plugins = Collections.singleton(transportPlugin);
        } else {
            plugins = Collections.emptyList();
        }
        TransportClient client = new PreBuiltTransportClient(builder.build(), plugins).addTransportAddresses(transportAddresses);

        logger.info("--> Havenask Java TransportClient started");

        Exception clientException = null;
        try {
            ClusterHealthResponse health = client.admin().cluster().prepareHealth().get();
            logger.info("--> connected to [{}] cluster which is running [{}] node(s).",
                    health.getClusterName(), health.getNumberOfNodes());
        } catch (Exception e) {
            clientException = e;
        }

        assumeNoException("Sounds like your cluster is not running at " + clusterAddresses, clientException);

        return client;
    }

    private static Client startClient() throws IOException {
        String[] stringAddresses = clusterAddresses.split(",");
        TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            URL url = new URL("http://" + stringAddress);
            InetAddress inetAddress = InetAddress.getByName(url.getHost());
            transportAddresses[i++] = new TransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
        }
        return startClient(createTempDir(), transportAddresses);
    }

    public static Client getClient() {
        if (client == null) {
            try {
                client = startClient();
            } catch (IOException e) {
                logger.error("can not start the client", e);
            }
            assertThat(client, notNullValue());
        }
        return client;
    }

    @BeforeClass
    public static void initializeSettings() {
        clusterAddresses = System.getProperty(TESTS_CLUSTER);
        if (clusterAddresses == null || clusterAddresses.isEmpty()) {
            fail("Must specify " + TESTS_CLUSTER + " for smoke client test");
        }
    }

    @AfterClass
    public static void stopTransportClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Before
    public void defineIndexName() {
        doClean();
        index = "qa-smoke-test-client-" + randomAsciiOfLength(10).toLowerCase(Locale.getDefault());
    }

    @After
    public void cleanIndex() {
        doClean();
    }

    private void doClean() {
        if (client != null && index != null) {
            try {
                client.admin().indices().prepareDelete(index).get();
            } catch (Exception e) {
                // We ignore this cleanup exception
            }
        }
    }
}
