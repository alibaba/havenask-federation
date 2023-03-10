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

import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.env.TestEnvironment;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.Before;

public class NativeProcessControlServiceTests extends HavenaskTestCase {
    private NativeProcessControlService nativeProcessControlService;

    @Before
    public void setup() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ThreadPool threadpool = new TestThreadPool("");
        ClusterService clusterService = new ClusterService(settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadpool);
        Environment environment = TestEnvironment.newEnvironment(settings);

        nativeProcessControlService = new MockNativeProcessControlService(clusterService, threadpool,
            environment, new NodeEnvironment(settings, environment),
            new HavenaskEngineEnvironment(settings));
    }

    public void testCheckProcessAlive() {
        boolean alive = nativeProcessControlService.checkProcessAlive("searcher");
        assertFalse(alive);
    }
}
