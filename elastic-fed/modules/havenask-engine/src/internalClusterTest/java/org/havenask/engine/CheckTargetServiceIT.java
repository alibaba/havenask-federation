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
import java.util.HashSet;
import java.util.Set;

import org.havenask.ArpcThreadLeakFilter;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.settings.Settings;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction;
import org.havenask.plugins.Plugin;
import org.havenask.transport.nio.MockNioTransportPlugin;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class, ArpcThreadLeakFilter.class })
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
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.engine", "havenask")
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                true
            )
            .put(
                IndexMetadata.builder("test2")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                true
            )
            .build();

        Set<String> nodeIndices = new HashSet<>();
        nodeIndices.add("test1");
        nodeIndices.add("test2");

        Set<String> searcherTables = new HashSet<>();
        searcherTables.add("test1");
        boolean result = CheckTargetService.checkDataNodeEquals(metadata, nodeIndices, searcherTables);
        assertTrue(result);
    }

    public void testCheckIngestNode() {
        try {
            HavenaskSqlClientInfoAction.Response sqlInfoResponse = client().execute(
                HavenaskSqlClientInfoAction.INSTANCE,
                new HavenaskSqlClientInfoAction.Request()
            ).actionGet();
            Set<String> havenaskIndices = new HashSet<>();
            if (sqlInfoResponse.getResult() != null && sqlInfoResponse.getResult().isEmpty() == false) {
                havenaskIndices.add("test2_0");
            }

            boolean result = CheckTargetService.checkIngestNodeEquals(sqlInfoResponse, havenaskIndices);
            assertTrue(result);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("no ingest node"));
        }
    }

}
