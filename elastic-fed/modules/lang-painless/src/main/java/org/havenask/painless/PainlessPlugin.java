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

package org.havenask.painless;


import org.apache.lucene.util.SetOnce;
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
import org.havenask.painless.action.PainlessContextAction;
import org.havenask.painless.action.PainlessExecuteAction;
import org.havenask.painless.spi.PainlessExtension;
import org.havenask.painless.spi.Whitelist;
import org.havenask.painless.spi.WhitelistLoader;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.ExtensiblePlugin;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.ScriptPlugin;
import org.havenask.repositories.RepositoriesService;
import org.havenask.rest.RestController;
import org.havenask.rest.RestHandler;
import org.havenask.script.IngestScript;
import org.havenask.script.ScoreScript;
import org.havenask.script.ScriptContext;
import org.havenask.script.ScriptEngine;
import org.havenask.script.ScriptService;
import org.havenask.search.aggregations.pipeline.MovingFunctionScript;
import org.havenask.threadpool.ThreadPool;
import org.havenask.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Registers Painless as a plugin.
 */
public final class PainlessPlugin extends Plugin implements ScriptPlugin, ExtensiblePlugin, ActionPlugin {

    private static final Map<ScriptContext<?>, List<Whitelist>> whitelists;

    /*
     * Contexts from Core that need custom whitelists can add them to the map below.
     * Whitelist resources should be added as appropriately named, separate files
     * under Painless' resources
     */
    static {
        Map<ScriptContext<?>, List<Whitelist>> map = new HashMap<>();

        // Moving Function Pipeline Agg
        List<Whitelist> movFn = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        movFn.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.havenask.aggs.movfn.txt"));
        map.put(MovingFunctionScript.CONTEXT, movFn);

        // Functions used for scoring docs
        List<Whitelist> scoreFn = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        scoreFn.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.havenask.score.txt"));
        map.put(ScoreScript.CONTEXT, scoreFn);

        // Functions available to ingest pipelines
        List<Whitelist> ingest = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        ingest.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.havenask.ingest.txt"));
        map.put(IngestScript.CONTEXT, ingest);

        whitelists = map;
    }

    private final SetOnce<PainlessScriptEngine> painlessScriptEngine = new SetOnce<>();

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        Map<ScriptContext<?>, List<Whitelist>> contextsWithWhitelists = new HashMap<>();
        for (ScriptContext<?> context : contexts) {
            // we might have a context that only uses the base whitelists, so would not have been filled in by reloadSPI
            List<Whitelist> contextWhitelists = whitelists.get(context);
            if (contextWhitelists == null) {
                contextWhitelists = new ArrayList<>(Whitelist.BASE_WHITELISTS);
            }
            contextsWithWhitelists.put(context, contextWhitelists);
        }
        painlessScriptEngine.set(new PainlessScriptEngine(settings, contextsWithWhitelists));
        return painlessScriptEngine.get();
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver expressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        // this is a hack to bind the painless script engine in guice (all components are added to guice), so that
        // the painless context api. this is a temporary measure until transport actions do no require guice
        return Collections.singletonList(painlessScriptEngine.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CompilerSettings.REGEX_ENABLED, CompilerSettings.REGEX_LIMIT_FACTOR);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        loader.loadExtensions(PainlessExtension.class).stream()
            .flatMap(extension -> extension.getContextWhitelists().entrySet().stream())
            .forEach(entry -> {
                List<Whitelist> existing = whitelists.computeIfAbsent(entry.getKey(),
                    c -> new ArrayList<>(Whitelist.BASE_WHITELISTS));
                existing.addAll(entry.getValue());
            });
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Collections.singletonList(PainlessExecuteAction.PainlessTestScript.CONTEXT);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(PainlessExecuteAction.INSTANCE, PainlessExecuteAction.TransportAction.class));
        actions.add(new ActionHandler<>(PainlessContextAction.INSTANCE, PainlessContextAction.TransportAction.class));
        return actions;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new PainlessExecuteAction.RestAction());
        handlers.add(new PainlessContextAction.RestAction());
        return handlers;
    }
}
