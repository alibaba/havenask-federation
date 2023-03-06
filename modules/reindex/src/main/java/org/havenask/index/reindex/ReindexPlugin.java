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

package org.havenask.index.reindex;

import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.index.reindex.spi.RemoteReindexExtension;
import org.havenask.plugins.ExtensiblePlugin;
import org.havenask.plugins.ExtensiblePlugin.ExtensionLoader;
import org.havenask.watcher.ResourceWatcherService;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionResponse;
import org.havenask.client.Client;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.SettingsFilter;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.RepositoriesService;
import org.havenask.rest.RestController;
import org.havenask.rest.RestHandler;
import org.havenask.script.ScriptService;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class ReindexPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin {
    public static final String NAME = "reindex";
    private static final Logger logger = LogManager.getLogger(ReindexPlugin.class);

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(ReindexAction.INSTANCE, TransportReindexAction.class),
                new ActionHandler<>(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class),
                new ActionHandler<>(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class),
                new ActionHandler<>(RethrottleAction.INSTANCE, TransportRethrottleAction.class));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return singletonList(
                new NamedWriteableRegistry.Entry(Task.Status.class, BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestReindexAction(),
                new RestUpdateByQueryAction(),
                new RestDeleteByQueryAction(),
                new RestRethrottleAction(nodesInCluster));
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver expressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        return Collections.singletonList(new ReindexSslConfig(environment.settings(), environment, resourceWatcherService));
    }

    @Override
    public List<Setting<?>> getSettings() {
        final List<Setting<?>> settings = new ArrayList<>();
        settings.add(TransportReindexAction.REMOTE_CLUSTER_WHITELIST);
        settings.addAll(ReindexSslConfig.getSettings());
        return settings;
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        logger.info("ReindexPlugin reloadSPI called");
        Iterable<RemoteReindexExtension> iterable = loader.loadExtensions(RemoteReindexExtension.class);
        List<RemoteReindexExtension> remoteReindexExtensionList = new ArrayList<>();
        iterable.forEach(remoteReindexExtensionList::add);
        if (remoteReindexExtensionList.isEmpty()) {
            logger.info("Unable to find any implementation for RemoteReindexExtension");
        } else {
            if (remoteReindexExtensionList.size() > 1) {
                logger.warn("More than one implementation found: " + remoteReindexExtensionList);
            }
            // We shouldn't have more than one extension. Incase there is, we simply pick the first one.
            TransportReindexAction.remoteExtension = Optional.ofNullable(remoteReindexExtensionList.get(0));
            logger.info("Loaded extension " + TransportReindexAction.remoteExtension);
        }
    }
}
