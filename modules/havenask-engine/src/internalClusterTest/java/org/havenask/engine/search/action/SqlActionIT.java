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

package org.havenask.engine.search.action;

import java.util.Arrays;
import java.util.Collection;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.hamcrest.Matchers;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.common.SuppressForbidden;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.HavenaskITTestCase;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.transport.nio.MockNioTransportPlugin;

@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class })
@HavenaskIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, scope = HavenaskIntegTestCase.Scope.TEST)
public class SqlActionIT extends HavenaskITTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .putList("node.roles", Arrays.asList("master", "data", "ingest"));
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class, MockNioTransportPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testSqlAction() {
        HavenaskSqlRequest request = new HavenaskSqlRequest("select * from test", null);
        HavenaskSqlResponse response = client().execute(HavenaskSqlAction.INSTANCE, request).actionGet();
        assertEquals(200, response.getResultCode());
        assertEquals("sql result", response.getResult());
    }

    public void testSqlActionFailed() {
        // stop mock server
        server.stop(0);
        HavenaskSqlRequest request = new HavenaskSqlRequest("select * from test", null);
        HavenaskSqlResponse response = client().execute(HavenaskSqlAction.INSTANCE, request).actionGet();
        assertEquals(500, response.getResultCode());
        assertThat(response.getResult(), Matchers.containsString("execute sql failed"));
    }
}
