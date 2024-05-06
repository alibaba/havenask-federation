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

import org.havenask.common.settings.Settings;
import org.havenask.node.NodeRoleSettings;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.index.reindex.ReindexPlugin;
import java.util.Arrays;
import java.util.Collection;

public abstract class HavenaskInternalClusterTestCase extends HavenaskIntegTestCase {
    private static final String RELATIVE_BIN_PATH = "/distribution/src/bin";
    private static final String ROOT_DIR_PATTERN = "/havenask-federation/elastic-fed";
    private static final int DEFAULT_SEARCHER_HTTP_PORT = NativeProcessControlService.HAVENASK_SEARCHER_HTTP_PORT_SETTING.getDefault(
        Settings.EMPTY
    );
    private static final int DEFAULT_SEARCHER_TCP_PORT = NativeProcessControlService.HAVENASK_SEARCHER_TCP_PORT_SETTING.getDefault(
        Settings.EMPTY
    );
    private static final int DEFAULT_SEARCHER_GRPC_PORT = NativeProcessControlService.HAVENASK_SEARCHER_GRPC_PORT_SETTING.getDefault(
        Settings.EMPTY
    );
    private static final int DEFAULT_QRS_HTTP_PORT = NativeProcessControlService.HAVENASK_QRS_HTTP_PORT_SETTING.getDefault(Settings.EMPTY);
    private static final int DEFAULT_QRS_TCP_PORT = NativeProcessControlService.HAVENASK_QRS_TCP_PORT_SETTING.getDefault(Settings.EMPTY);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class, ReindexPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    // TODO: Supports customization of each node‘s node roles
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return havenaskNodeSettings(super.nodeSettings(nodeOrdinal), nodeOrdinal);
    }

    public static Settings havenaskNodeSettings(Settings base, int nodeOrdinal) {
        String usrDir = System.getProperty("user.dir");
        String binDir = "";

        int lastIndex = usrDir.lastIndexOf(ROOT_DIR_PATTERN);
        if (lastIndex != -1) {
            String projectDir = usrDir.substring(0, lastIndex + ROOT_DIR_PATTERN.length());
            binDir = projectDir + RELATIVE_BIN_PATH;
        }

        int searcherHttpPort = DEFAULT_SEARCHER_HTTP_PORT + nodeOrdinal;
        int searcherTcpPort = DEFAULT_SEARCHER_TCP_PORT + nodeOrdinal;
        int searcherGrpcPort = DEFAULT_SEARCHER_GRPC_PORT + nodeOrdinal;
        int qrsHttpPort = DEFAULT_QRS_HTTP_PORT + nodeOrdinal;
        int qrsTcpPort = DEFAULT_QRS_TCP_PORT + nodeOrdinal;

        return Settings.builder()
            .put(base)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), Arrays.asList("master", "data", "ingest"))
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(HavenaskEngineEnvironment.HAVENASK_ENVIRONMENT_BINFILE_PATH_SETTING.getKey(), binDir)
            .put(NativeProcessControlService.HAVENASK_SEARCHER_HTTP_PORT_SETTING.getKey(), String.valueOf(searcherHttpPort))
            .put(NativeProcessControlService.HAVENASK_SEARCHER_TCP_PORT_SETTING.getKey(), String.valueOf(searcherTcpPort))
            .put(NativeProcessControlService.HAVENASK_SEARCHER_GRPC_PORT_SETTING.getKey(), String.valueOf(searcherGrpcPort))
            .put(NativeProcessControlService.HAVENASK_QRS_HTTP_PORT_SETTING.getKey(), String.valueOf(qrsHttpPort))
            .put(NativeProcessControlService.HAVENASK_QRS_TCP_PORT_SETTING.getKey(), String.valueOf(qrsTcpPort))
            .build();
    }
}
