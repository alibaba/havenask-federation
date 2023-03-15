/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Havenask Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.havenask.engine;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.havenask.client.Client;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.engine.HavenaskEngine;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.index.IndexSettings;
import org.havenask.index.engine.EngineFactory;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.AnalysisPlugin;
import org.havenask.plugins.EnginePlugin;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.SearchPlugin;
import org.havenask.repositories.RepositoriesService;
import org.havenask.script.ScriptService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.watcher.ResourceWatcherService;

public class HavenaskEnginePlugin extends Plugin implements EnginePlugin, AnalysisPlugin, ActionPlugin, SearchPlugin {
    private static Logger logger = LogManager.getLogger(HavenaskEnginePlugin.class);
    private final SetOnce<HavenaskEngineEnvironment> havenaskEngineEnvironmentSetOnce = new SetOnce<>();
    private final SetOnce<NativeProcessControlService> nativeProcessControlServiceSetOnce = new SetOnce<>();

    @Override
    public Optional<EngineFactory> getEngineFactory(
        IndexSettings indexSettings) {
        if (EngineSettings.isHavenaskEngine(indexSettings.getSettings())) {
            return Optional.of(engineConfig -> new HavenaskEngine(engineConfig));
        }

        return Optional.empty();
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService, ScriptService scriptService,
        NamedXContentRegistry xContentRegistry, Environment environment,
        NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier) {
        HavenaskEngineEnvironment havenaskEngineEnvironment = new HavenaskEngineEnvironment(clusterService.getSettings());
        havenaskEngineEnvironmentSetOnce.set(havenaskEngineEnvironment);

        NativeProcessControlService nativeProcessControlService = new NativeProcessControlService(clusterService, threadPool, environment, nodeEnvironment, havenaskEngineEnvironment);
        nativeProcessControlServiceSetOnce.set(nativeProcessControlService);
        return Arrays.asList(nativeProcessControlServiceSetOnce.get(), havenaskEngineEnvironmentSetOnce.get());
    }
}
