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

package org.havenask.repositories;

import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.Environment;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.plugins.RepositoryPlugin;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoriesModuleTests extends HavenaskTestCase {

    private Environment environment;
    private NamedXContentRegistry contentRegistry;
    private List<RepositoryPlugin> repoPlugins = new ArrayList<>();
    private RepositoryPlugin plugin1;
    private RepositoryPlugin plugin2;
    private Repository.Factory factory;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private RecoverySettings recoverySettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        environment = mock(Environment.class);
        contentRegistry = mock(NamedXContentRegistry.class);
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        recoverySettings = mock(RecoverySettings.class);
        plugin1 = mock(RepositoryPlugin.class);
        plugin2 = mock(RepositoryPlugin.class);
        factory = mock(Repository.Factory.class);
        repoPlugins.add(plugin1);
        repoPlugins.add(plugin2);
        when(environment.settings()).thenReturn(Settings.EMPTY);
    }

    public void testCanRegisterTwoRepositoriesWithDifferentTypes() {
        when(plugin1.getRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type1", factory));
        when(plugin2.getRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type2", factory));

        // Would throw
        new RepositoriesModule(
            environment, repoPlugins, mock(TransportService.class), mock(ClusterService.class), threadPool, contentRegistry,
            recoverySettings);
    }

    public void testCannotRegisterTwoRepositoriesWithSameTypes() {
        when(plugin1.getRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type1", factory));
        when(plugin2.getRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type1", factory));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new RepositoriesModule(environment, repoPlugins, mock(TransportService.class), clusterService,
                threadPool, contentRegistry, recoverySettings));

        assertEquals("Repository type [type1] is already registered", ex.getMessage());
    }

    public void testCannotRegisterTwoInternalRepositoriesWithSameTypes() {
        when(plugin1.getInternalRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type1", factory));
        when(plugin2.getInternalRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type1", factory));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new RepositoriesModule(environment, repoPlugins, mock(TransportService.class), clusterService,
                threadPool, contentRegistry, recoverySettings));

        assertEquals("Internal repository type [type1] is already registered", ex.getMessage());
    }

    public void testCannotRegisterNormalAndInternalRepositoriesWithSameTypes() {
        when(plugin1.getRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type1", factory));
        when(plugin2.getInternalRepositories(environment, contentRegistry, clusterService, recoverySettings))
            .thenReturn(Collections.singletonMap("type1", factory));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> new RepositoriesModule(environment, repoPlugins, mock(TransportService.class), clusterService, threadPool,
                contentRegistry, recoverySettings));

        assertEquals("Internal repository type [type1] is already registered as a non-internal repository", ex.getMessage());
    }
}
