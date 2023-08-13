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
import java.util.Arrays;
import java.util.Collection;

import org.havenask.ArpcThreadLeakFilter;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.cluster.ClusterState;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.engine.rpc.http.SearcherHttpClient;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.Scope;
import org.havenask.transport.nio.MockNioTransportPlugin;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class, ArpcThreadLeakFilter.class })
@HavenaskIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, scope = Scope.SUITE)
public class CheckTargetServiceIT extends HavenaskITTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class, MockNioTransportPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testCheckDataNode() throws IOException {
        ClusterState clusterState = clusterService().state();
        SearcherClient client = new SearcherHttpClient(39200);
        boolean result = CheckTargetService.checkDataNode(clusterState, client);
        assertFalse(result);
    }

    public void testCheckIngestNode() throws IOException {
        ClusterState clusterState = clusterService().state();
        try {
            boolean result = CheckTargetService.checkIngestNode(clusterState, client());
            assertFalse(result);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("no ingest node"));
        }
    }

}
