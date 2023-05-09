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

package org.havenask.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.havenask.client.Request;
import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.support.master.AcknowledgedRequest;
import org.havenask.client.cluster.RemoteInfoRequest;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.Priority;
import org.havenask.common.util.CollectionUtils;
import org.havenask.test.HavenaskTestCase;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ClusterRequestConvertersTests extends HavenaskTestCase {

    public void testClusterPutSettings() throws IOException {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(request, expectedParams);
        RequestConvertersTests.setRandomTimeout(request::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request expectedRequest = ClusterRequestConverters.clusterPutSettings(request);
        Assert.assertEquals("/_cluster/settings", expectedRequest.getEndpoint());
        Assert.assertEquals(HttpPut.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testClusterGetSettings() throws IOException {
        ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(request, expectedParams);
        request.includeDefaults(HavenaskTestCase.randomBoolean());
        if (request.includeDefaults()) {
            expectedParams.put("include_defaults", String.valueOf(true));
        }

        Request expectedRequest = ClusterRequestConverters.clusterGetSettings(request);
        Assert.assertEquals("/_cluster/settings", expectedRequest.getEndpoint());
        Assert.assertEquals(HttpGet.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testClusterHealth() {
        ClusterHealthRequest healthRequest = new ClusterHealthRequest();
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomLocal(healthRequest::local, expectedParams);
        String timeoutType = HavenaskTestCase.randomFrom("timeout", "masterTimeout", "both", "none");
        String timeout = HavenaskTestCase.randomTimeValue();
        String masterTimeout = HavenaskTestCase.randomTimeValue();
        switch (timeoutType) {
            case "timeout":
                healthRequest.timeout(timeout);
                expectedParams.put("timeout", timeout);
                // If Master Timeout wasn't set it uses the same value as Timeout
                expectedParams.put("master_timeout", timeout);
                break;
            case "masterTimeout":
                expectedParams.put("timeout", "30s");
                healthRequest.masterNodeTimeout(masterTimeout);
                expectedParams.put("master_timeout", masterTimeout);
                break;
            case "both":
                healthRequest.timeout(timeout);
                expectedParams.put("timeout", timeout);
                healthRequest.masterNodeTimeout(timeout);
                expectedParams.put("master_timeout", timeout);
                break;
            case "none":
                expectedParams.put("timeout", "30s");
                expectedParams.put("master_timeout", "30s");
                break;
            default:
                throw new UnsupportedOperationException();
        }
        RequestConvertersTests.setRandomWaitForActiveShards(healthRequest::waitForActiveShards, ActiveShardCount.NONE, expectedParams);
        if (HavenaskTestCase.randomBoolean()) {
            ClusterHealthRequest.Level level = HavenaskTestCase.randomFrom(ClusterHealthRequest.Level.values());
            healthRequest.level(level);
            expectedParams.put("level", level.name().toLowerCase(Locale.ROOT));
        } else {
            expectedParams.put("level", "cluster");
        }
        if (HavenaskTestCase.randomBoolean()) {
            Priority priority = HavenaskTestCase.randomFrom(Priority.values());
            healthRequest.waitForEvents(priority);
            expectedParams.put("wait_for_events", priority.name().toLowerCase(Locale.ROOT));
        }
        if (HavenaskTestCase.randomBoolean()) {
            ClusterHealthStatus status = HavenaskTestCase.randomFrom(ClusterHealthStatus.values());
            healthRequest.waitForStatus(status);
            expectedParams.put("wait_for_status", status.name().toLowerCase(Locale.ROOT));
        }
        if (HavenaskTestCase.randomBoolean()) {
            boolean waitForNoInitializingShards = HavenaskTestCase.randomBoolean();
            healthRequest.waitForNoInitializingShards(waitForNoInitializingShards);
            if (waitForNoInitializingShards) {
                expectedParams.put("wait_for_no_initializing_shards", Boolean.TRUE.toString());
            }
        }
        if (HavenaskTestCase.randomBoolean()) {
            boolean waitForNoRelocatingShards = HavenaskTestCase.randomBoolean();
            healthRequest.waitForNoRelocatingShards(waitForNoRelocatingShards);
            if (waitForNoRelocatingShards) {
                expectedParams.put("wait_for_no_relocating_shards", Boolean.TRUE.toString());
            }
        }
        String[] indices = HavenaskTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        healthRequest.indices(indices);

        Request request = ClusterRequestConverters.clusterHealth(healthRequest);
        Assert.assertThat(request, CoreMatchers.notNullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
        if (CollectionUtils.isEmpty(indices) == false) {
            Assert.assertThat(request.getEndpoint(), equalTo("/_cluster/health/" + String.join(",", indices)));
        } else {
            Assert.assertThat(request.getEndpoint(), equalTo("/_cluster/health"));
        }
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
    }

    public void testRemoteInfo() {
        RemoteInfoRequest request = new RemoteInfoRequest();
        Request expectedRequest = ClusterRequestConverters.remoteInfo(request);
        assertEquals("/_remote/info", expectedRequest.getEndpoint());
        assertEquals(HttpGet.METHOD_NAME, expectedRequest.getMethod());
        assertEquals(emptyMap(), expectedRequest.getParameters());
    }
}
