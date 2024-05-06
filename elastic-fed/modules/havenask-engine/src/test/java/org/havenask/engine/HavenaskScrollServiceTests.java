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

import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.discovery.DiscoveryModule;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.search.internal.HavenaskScrollContext;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.havenask.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskScrollServiceTests extends HavenaskAllocationTestCase {
    private HavenaskScrollService havenaskScrollService;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        // generate threadPool
        threadPool = new TestThreadPool(getTestName());

        // generate clusterService
        Settings settings = Settings.builder()
            .put(HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.getKey(), true)
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .build();

        Set<Setting<?>> buildInSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        buildInSettings.add(NativeProcessControlService.HAVENASK_COMMAND_TIMEOUT_SETTING);
        clusterService = new ClusterService(settings, new ClusterSettings(settings, buildInSettings), threadPool);

        // generate havenaskScrollService
        havenaskScrollService = new HavenaskScrollService(clusterService, threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        havenaskScrollService.close();
    }

    public void testPutAndRemoveScrollContext() {
        String sessionID = "testSession";

        HavenaskScrollContext context = mock(HavenaskScrollContext.class);
        when(context.getScrollSessionId()).thenReturn(sessionID);

        assertEquals(0, havenaskScrollService.getActiveContextSize());

        havenaskScrollService.putScrollContext(context);
        assertEquals(1, havenaskScrollService.getActiveContextSize());

        HavenaskScrollContext retrievedContex = havenaskScrollService.getScrollContext(sessionID);
        assertNotNull(retrievedContex);

        havenaskScrollService.removeScrollContext(sessionID);
        assertEquals(0, havenaskScrollService.getActiveContextSize());
    }

    public void testReaperRemovesExpiredContexts() {
        String sessionID = "expiredSession";
        HavenaskScrollContext expiredContext = mock(HavenaskScrollContext.class);
        when(expiredContext.getScrollSessionId()).thenReturn(sessionID);
        when(expiredContext.isExpired()).thenReturn(true);

        havenaskScrollService.putScrollContext(expiredContext);
        assertEquals(1, havenaskScrollService.getActiveContextSize());
        havenaskScrollService.new Reaper().run();   // 手动触发 Reaper 运行
        assertEquals(0, havenaskScrollService.getActiveContextSize());
    }

    public void testRemoveAllHavenaskScrollContext() {
        int contextSize = 5;
        for (int i = 0; i < contextSize; i++) {
            String sessionId = "expiredSession" + i;
            HavenaskScrollContext expiredContext = mock(HavenaskScrollContext.class);
            when(expiredContext.getScrollSessionId()).thenReturn(sessionId);
            when(expiredContext.isExpired()).thenReturn(true);
            havenaskScrollService.putScrollContext(expiredContext);
        }

        assertEquals(contextSize, havenaskScrollService.getActiveContextSize());
        havenaskScrollService.removeAllHavenaskScrollContext();
        assertEquals(0, havenaskScrollService.getActiveContextSize());
    }

}
