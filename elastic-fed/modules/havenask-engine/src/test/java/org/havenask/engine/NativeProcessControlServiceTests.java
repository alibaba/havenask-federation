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

import static org.havenask.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.PathUtils;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.discovery.DiscoveryModule;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.env.TestEnvironment;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

public class NativeProcessControlServiceTests extends HavenaskTestCase {
    private NativeProcessControlService nativeProcessControlService;
    private ThreadPool threadPool;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();
        Set<Setting<?>> buildInSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        buildInSettings.add(NativeProcessControlService.HAVENASK_COMMAND_TIMEOUT_SETTING);
        ClusterService clusterService = new ClusterService(settings, new ClusterSettings(settings, buildInSettings), threadPool);

        Environment environment = TestEnvironment.newEnvironment(settings);
        NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment);
        nativeProcessControlService = new MockNativeProcessControlService(
            clusterService,
            threadPool,
            environment,
            nodeEnvironment,
            new HavenaskEngineEnvironment(environment, settings)
        );
        nodeEnvironment.close();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        nativeProcessControlService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        threadPool = null;
    }

    @AwaitsFix(bugUrl = "https://github.com/alibaba/havenask-federation/issues/78")
    public void testStartStopSearcher() throws Exception {
        // 启动searcher
        nativeProcessControlService.start();
        assertBusy(() -> {
            boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.SEARCHER_ROLE);
            assertTrue(alive);
        });

        // 关闭启动searcher
        nativeProcessControlService.stop();
        assertBusy(() -> {
            boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.SEARCHER_ROLE);
            assertFalse(alive);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/alibaba/havenask-federation/issues/78")
    public void testStartStopQrs() throws Exception {
        // 启动qrs
        nativeProcessControlService.start();
        assertBusy(() -> {
            boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.QRS_ROLE);
            assertTrue(alive);
        });

        // 关闭启动qrs
        nativeProcessControlService.stop();
        assertBusy(() -> {
            boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.QRS_ROLE);
            assertFalse(alive);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/alibaba/havenask-federation/issues/78")
    public void testCheckProcessAlive() throws Exception {
        // 传递错误的searcher名称
        {
            boolean alive = nativeProcessControlService.checkProcessAlive("wrong_searcher");
            assertFalse(alive);
        }

        // searcher进程不存在
        {
            boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.SEARCHER_ROLE);
            assertFalse(alive);
        }

        // 启动searcher,并检测
        {
            AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[] { "sh", "-c", nativeProcessControlService.startSearcherCommand });
                } catch (IOException e) {
                    return null;
                }
            });
            boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.SEARCHER_ROLE);
            assertTrue(alive);
        }

        // 再启动一个searcher,检测出2个进程会出错
        {
            AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[] { "sh", "-c", nativeProcessControlService.startSearcherCommand });
                } catch (IOException e) {
                    return null;
                }
            });
            boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.SEARCHER_ROLE);
            assertFalse(alive);
        }

        // 关闭全部searcher后,检测出进程不存在
        {
            AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[] { "sh", "-c", nativeProcessControlService.stopHavenaskCommand });
                } catch (IOException e) {
                    return null;
                }
            });
            assertBusy(() -> {
                boolean alive = nativeProcessControlService.checkProcessAlive(NativeProcessControlService.SEARCHER_ROLE);
                assertFalse(alive);
            });
        }
    }

    // check table size
    public void testGetTableSize() throws Exception {
        // 传递正确的table path
        {
            long tableSize = nativeProcessControlService.getTableSize(PathUtils.get("./"));
            assertTrue(tableSize >= 0);
        }

        // 传递错误的table path
        {
            long tableSize = nativeProcessControlService.getTableSize(PathUtils.get("/exception"));
            assertEquals(-1L, tableSize);
        }
    }
}
