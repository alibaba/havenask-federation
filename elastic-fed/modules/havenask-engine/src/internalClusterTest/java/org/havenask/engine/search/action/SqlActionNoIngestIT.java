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
import org.havenask.ArpcThreadLeakFilter;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction.Request;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.transport.nio.MockNioTransportPlugin;

@ThreadLeakFilters(filters = { ArpcThreadLeakFilter.class })
public class SqlActionNoIngestIT extends HavenaskIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .putList("node.roles", Arrays.asList("master", "data"));
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

    public void testSqlActionFailed() {
        HavenaskSqlRequest request = new HavenaskSqlRequest("select * from test", null);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> client().execute(HavenaskSqlAction.INSTANCE, request).actionGet()
        );
        assertThat(
            e.getMessage(),
            Matchers.equalTo("There are no ingest nodes in this cluster, unable to forward request to an ingest node.")
        );
    }

    // test SqlClientInfoAction failed
    public void testSqlClientInfoActionFailed() {
        HavenaskSqlClientInfoAction.Request request = new Request();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> client().execute(HavenaskSqlClientInfoAction.INSTANCE, request).actionGet()
        );
        assertThat(
            e.getMessage(),
            Matchers.equalTo("There are no ingest nodes in this cluster, unable to forward request to an ingest node.")
        );
    }

}
