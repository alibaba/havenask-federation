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

package org.havenask.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;
import org.havenask.watcher.ResourceWatcherService;
import org.havenask.Assertions;
import org.havenask.Build;
import org.havenask.HavenaskException;
import org.havenask.HavenaskTimeoutException;
import org.havenask.Version;
import org.havenask.action.ActionModule;
import org.havenask.action.ActionType;
import org.havenask.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.havenask.action.search.SearchExecutionStatsCollector;
import org.havenask.action.search.SearchPhaseController;
import org.havenask.action.search.SearchTransportService;
import org.havenask.action.support.TransportAction;
import org.havenask.action.update.UpdateHelper;
import org.havenask.bootstrap.BootstrapCheck;
import org.havenask.bootstrap.BootstrapContext;
import org.havenask.client.Client;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.ClusterInfoService;
import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateObserver;
import org.havenask.cluster.InternalClusterInfoService;
import org.havenask.cluster.NodeConnectionsService;
import org.havenask.cluster.action.index.MappingUpdatedAction;
import org.havenask.cluster.metadata.AliasValidator;
import org.havenask.cluster.metadata.IndexTemplateMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataCreateDataStreamService;
import org.havenask.cluster.metadata.MetadataCreateIndexService;
import org.havenask.cluster.metadata.MetadataIndexUpgradeService;
import org.havenask.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.havenask.cluster.metadata.TemplateUpgradeService;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.routing.BatchedRerouteService;
import org.havenask.cluster.routing.RerouteService;
import org.havenask.cluster.routing.allocation.DiskThresholdMonitor;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.StopWatch;
import org.havenask.common.breaker.CircuitBreaker;
import org.havenask.common.component.Lifecycle;
import org.havenask.common.component.LifecycleComponent;
import org.havenask.common.inject.Injector;
import org.havenask.common.inject.Key;
import org.havenask.common.inject.Module;
import org.havenask.common.inject.ModulesBuilder;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.lease.Releasables;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.logging.HeaderWarning;
import org.havenask.common.logging.NodeAndClusterIdStateListener;
import org.havenask.common.network.NetworkAddress;
import org.havenask.common.network.NetworkModule;
import org.havenask.common.network.NetworkService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.ConsistentSettingsService;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.SettingUpgrader;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.SettingsModule;
import org.havenask.common.transport.BoundTransportAddress;
import org.havenask.common.transport.TransportAddress;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.BigArrays;
import org.havenask.common.util.PageCacheRecycler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.discovery.Discovery;
import org.havenask.discovery.DiscoveryModule;
import org.havenask.discovery.DiscoverySettings;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.env.NodeMetadata;
import org.havenask.gateway.GatewayAllocator;
import org.havenask.gateway.GatewayMetaState;
import org.havenask.gateway.GatewayModule;
import org.havenask.gateway.GatewayService;
import org.havenask.gateway.MetaStateService;
import org.havenask.gateway.PersistedClusterStateService;
import org.havenask.http.HttpServerTransport;
import org.havenask.index.IndexSettings;
import org.havenask.index.IndexingPressure;
import org.havenask.index.analysis.AnalysisRegistry;
import org.havenask.index.engine.EngineFactory;
import org.havenask.indices.IndicesModule;
import org.havenask.indices.IndicesService;
import org.havenask.indices.ShardLimitValidator;
import org.havenask.indices.SystemIndexDescriptor;
import org.havenask.indices.SystemIndices;
import org.havenask.indices.analysis.AnalysisModule;
import org.havenask.indices.breaker.BreakerSettings;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.indices.breaker.HierarchyCircuitBreakerService;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.indices.cluster.IndicesClusterStateService;
import org.havenask.indices.recovery.PeerRecoverySourceService;
import org.havenask.indices.recovery.PeerRecoveryTargetService;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.indices.store.IndicesStore;
import org.havenask.ingest.IngestService;
import org.havenask.monitor.MonitorService;
import org.havenask.monitor.fs.FsHealthService;
import org.havenask.monitor.jvm.JvmInfo;
import org.havenask.persistent.PersistentTasksClusterService;
import org.havenask.persistent.PersistentTasksExecutor;
import org.havenask.persistent.PersistentTasksExecutorRegistry;
import org.havenask.persistent.PersistentTasksService;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.AnalysisPlugin;
import org.havenask.plugins.CircuitBreakerPlugin;
import org.havenask.plugins.ClusterPlugin;
import org.havenask.plugins.DiscoveryPlugin;
import org.havenask.plugins.EnginePlugin;
import org.havenask.plugins.IndexStorePlugin;
import org.havenask.plugins.IngestPlugin;
import org.havenask.plugins.MapperPlugin;
import org.havenask.plugins.MetadataUpgrader;
import org.havenask.plugins.NetworkPlugin;
import org.havenask.plugins.PersistentTaskPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.PluginsService;
import org.havenask.plugins.RepositoryPlugin;
import org.havenask.plugins.ScriptPlugin;
import org.havenask.plugins.SearchPlugin;
import org.havenask.plugins.SystemIndexPlugin;
import org.havenask.repositories.RepositoriesModule;
import org.havenask.repositories.RepositoriesService;
import org.havenask.rest.RestController;
import org.havenask.script.ScriptContext;
import org.havenask.script.ScriptEngine;
import org.havenask.script.ScriptModule;
import org.havenask.script.ScriptService;
import org.havenask.search.SearchModule;
import org.havenask.search.SearchService;
import org.havenask.search.aggregations.support.AggregationUsageService;
import org.havenask.search.fetch.FetchPhase;
import org.havenask.snapshots.InternalSnapshotsInfoService;
import org.havenask.snapshots.RestoreService;
import org.havenask.snapshots.SnapshotShardsService;
import org.havenask.snapshots.SnapshotsInfoService;
import org.havenask.snapshots.SnapshotsService;
import org.havenask.tasks.Task;
import org.havenask.tasks.TaskCancellationService;
import org.havenask.tasks.TaskResultsService;
import org.havenask.threadpool.ExecutorBuilder;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.RemoteClusterService;
import org.havenask.transport.Transport;
import org.havenask.transport.TransportInterceptor;
import org.havenask.transport.TransportService;
import org.havenask.usage.UsageService;

import javax.net.ssl.SNIHostName;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A node represent a node within a cluster ({@code cluster.name}). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 */
public class Node implements Closeable {
    public static final Setting<Boolean> WRITE_PORTS_FILE_SETTING =
        Setting.boolSetting("node.portsfile", false, Property.NodeScope);
    private static final Setting<Boolean> NODE_DATA_SETTING =
        Setting.boolSetting("node.data", true, Property.Deprecated, Property.NodeScope);
    private static final Setting<Boolean> NODE_MASTER_SETTING =
        Setting.boolSetting("node.master", true, Property.Deprecated, Property.NodeScope);
    private static final Setting<Boolean> NODE_INGEST_SETTING =
        Setting.boolSetting("node.ingest", true, Property.Deprecated, Property.NodeScope);
    private static final Setting<Boolean> NODE_REMOTE_CLUSTER_CLIENT = Setting.boolSetting(
        "node.remote_cluster_client",
        RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
        Property.Deprecated,
        Property.NodeScope);

    /**
    * controls whether the node is allowed to persist things like metadata to disk
    * Note that this does not control whether the node stores actual indices (see
    * {@link #NODE_DATA_SETTING}). However, if this is false, {@link #NODE_DATA_SETTING}
    * and {@link #NODE_MASTER_SETTING} must also be false.
    *
    */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING =
        Setting.boolSetting("node.local_storage", true, Property.Deprecated, Property.NodeScope);
    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);
    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting("node.attr.", (key) ->
        new Setting<>(key, "", (value) -> {
            if (value.length() > 0
                && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace " +
                    "[" + value + "]");
            }
            if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                try {
                    new SNIHostName(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e );
                }
            }
            return value;
        }, Property.NodeScope));
    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> {
        switch (s) {
            case "hierarchy":
            case "none":
                return s;
            default:
                throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
        }
    }, Setting.Property.NodeScope);

    private static final String CLIENT_TYPE = "node";

    private final Lifecycle lifecycle = new Lifecycle();

    /**
     * This logger instance is an instance field as opposed to a static field. This ensures that the field is not
     * initialized until an instance of Node is constructed, which is sure to happen after the logging infrastructure
     * has been initialized to include the hostname. If this field were static, then it would be initialized when the
     * class initializer runs. Alas, this happens too early, before logging is initialized as this class is referred to
     * in InternalSettingsPreparer#finalizeSettings, which runs when creating the Environment, before logging is
     * initialized.
     */
    private final Logger logger = LogManager.getLogger(Node.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Node.class);
    private final Injector injector;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final PluginsService pluginsService;
    private final NodeClient client;
    private final Collection<LifecycleComponent> pluginLifecycleComponents;
    private final LocalNodeFactory localNodeFactory;
    private final NodeService nodeService;

    public Node(Environment environment) {
        this(environment, Collections.emptyList(), true);
    }

    /**
     * Constructs a node
     *
     * @param initialEnvironment         the initial environment for this node, which will be added to by plugins
     * @param classpathPlugins           the plugins to be loaded from the classpath
     * @param forbidPrivateIndexSettings whether or not private index settings are forbidden when creating an index; this is used in the
     *                                   test framework for tests that rely on being able to set private settings
     */
    protected Node(final Environment initialEnvironment,
                   Collection<Class<? extends Plugin>> classpathPlugins, boolean forbidPrivateIndexSettings) {
        final List<Closeable> resourcesToClose = new ArrayList<>(); // register everything we need to release in the case of an error
        boolean success = false;
        try {
            Settings tmpSettings = Settings.builder().put(initialEnvironment.settings())
                .put(Client.CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE).build();

            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info(
                "version[{}], pid[{}], build[{}/{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
                Build.CURRENT.getQualifiedVersion(),
                jvmInfo.pid(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
                Constants.OS_NAME,
                Constants.OS_VERSION,
                Constants.OS_ARCH,
                Constants.JVM_VENDOR,
                Constants.JVM_NAME,
                Constants.JAVA_VERSION,
                Constants.JVM_VERSION);
            if (jvmInfo.getBundledJdk()) {
                logger.info("JVM home [{}], using bundled JDK [{}]", System.getProperty("java.home"), jvmInfo.getUsingBundledJdk());
            } else {
                logger.info("JVM home [{}]", System.getProperty("java.home"));
                deprecationLogger.deprecate(
                    "no-jdk",
                    "no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release");
            }
            logger.info("JVM arguments {}", Arrays.toString(jvmInfo.getInputArguments()));
            if (Build.CURRENT.isProductionRelease() == false) {
                logger.warn(
                    "version [{}] is a pre-release version of Havenask and is not suitable for production",
                    Build.CURRENT.getQualifiedVersion());
            }

            if (logger.isDebugEnabled()) {
                logger.debug("using config [{}], data [{}], logs [{}], plugins [{}]",
                    initialEnvironment.configFile(), Arrays.toString(initialEnvironment.dataFiles()),
                    initialEnvironment.logsFile(), initialEnvironment.pluginsFile());
            }

            this.pluginsService = new PluginsService(tmpSettings, initialEnvironment.configFile(), initialEnvironment.modulesFile(),
                initialEnvironment.pluginsFile(), classpathPlugins);
            final Settings settings = pluginsService.updatedSettings();

            final Set<DiscoveryNodeRole> additionalRoles = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(Plugin::getRoles)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
            DiscoveryNode.setAdditionalRoles(additionalRoles);

            /*
             * Create the environment based on the finalized view of the settings. This is to ensure that components get the same setting
             * values, no matter they ask for them from.
             */
            this.environment = new Environment(settings, initialEnvironment.configFile(), Node.NODE_LOCAL_STORAGE_SETTING.get(settings));
            Environment.assertEquivalent(initialEnvironment, this.environment);
            nodeEnvironment = new NodeEnvironment(tmpSettings, environment);
            logger.info("node name [{}], node ID [{}], cluster name [{}], roles {}",
                NODE_NAME_SETTING.get(tmpSettings), nodeEnvironment.nodeId(), ClusterName.CLUSTER_NAME_SETTING.get(tmpSettings).value(),
                DiscoveryNode.getRolesFromSettings(settings).stream()
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.toCollection(LinkedHashSet::new)));
            resourcesToClose.add(nodeEnvironment);
            localNodeFactory = new LocalNodeFactory(settings, nodeEnvironment.nodeId());

            final List<ExecutorBuilder<?>> executorBuilders = pluginsService.getExecutorBuilders(settings);

            final ThreadPool threadPool = new ThreadPool(settings, executorBuilders.toArray(new ExecutorBuilder[0]));
            resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
            resourcesToClose.add(resourceWatcherService);
            // adds the context to the DeprecationLogger so that it does not need to be injected everywhere
            HeaderWarning.setThreadContext(threadPool.getThreadContext());
            resourcesToClose.add(() -> HeaderWarning.removeThreadContext(threadPool.getThreadContext()));

            final List<Setting<?>> additionalSettings = new ArrayList<>();
            // register the node.data, node.ingest, node.master, node.remote_cluster_client settings here so we can mark them private
            additionalSettings.add(NODE_DATA_SETTING);
            additionalSettings.add(NODE_INGEST_SETTING);
            additionalSettings.add(NODE_MASTER_SETTING);
            additionalSettings.add(NODE_REMOTE_CLUSTER_CLIENT);
            additionalSettings.addAll(pluginsService.getPluginSettings());
            final List<String> additionalSettingsFilter = new ArrayList<>(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            client = new NodeClient(settings, threadPool);

            final ScriptModule scriptModule = new ScriptModule(settings, pluginsService.filterPlugins(ScriptPlugin.class));
            final ScriptService scriptService = newScriptService(settings, scriptModule.engines, scriptModule.contexts);
            AnalysisModule analysisModule = new AnalysisModule(this.environment, pluginsService.filterPlugins(AnalysisPlugin.class));
            // this is as early as we can validate settings at this point. we already pass them to ScriptModule as well as ThreadPool
            // so we might be late here already

            final Set<SettingUpgrader<?>> settingsUpgraders = pluginsService.filterPlugins(Plugin.class)
                    .stream()
                    .map(Plugin::getSettingUpgraders)
                    .flatMap(List::stream)
                    .collect(Collectors.toSet());

            final SettingsModule settingsModule =
                    new SettingsModule(settings, additionalSettings, additionalSettingsFilter, settingsUpgraders);
            scriptModule.registerClusterSettingsListeners(scriptService, settingsModule.getClusterSettings());
            final NetworkService networkService = new NetworkService(
                getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class)));

            List<ClusterPlugin> clusterPlugins = pluginsService.filterPlugins(ClusterPlugin.class);
            final ClusterService clusterService = new ClusterService(settings, settingsModule.getClusterSettings(), threadPool);
            clusterService.addStateApplier(scriptService);
            resourcesToClose.add(clusterService);
            final Set<Setting<?>> consistentSettings = settingsModule.getConsistentSettings();
            if (consistentSettings.isEmpty() == false) {
                clusterService.addLocalNodeMasterListener(
                        new ConsistentSettingsService(settings, clusterService, consistentSettings).newHashPublisher());
            }
            final IngestService ingestService = new IngestService(clusterService, threadPool, this.environment,
                scriptService, analysisModule.getAnalysisRegistry(),
                pluginsService.filterPlugins(IngestPlugin.class), client);
            final SetOnce<RepositoriesService> repositoriesServiceReference = new SetOnce<>();
            final ClusterInfoService clusterInfoService = newClusterInfoService(settings, clusterService, threadPool, client);
            final UsageService usageService = new UsageService();

            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors...
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            final MonitorService monitorService = new MonitorService(settings, nodeEnvironment, threadPool);
            final FsHealthService fsHealthService = new FsHealthService(settings, clusterService.getClusterSettings(), threadPool,
                nodeEnvironment);
            final SetOnce<RerouteService> rerouteServiceReference = new SetOnce<>();
            final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(settings, clusterService,
                repositoriesServiceReference::get, rerouteServiceReference::get);
            final ClusterModule clusterModule = new ClusterModule(settings, clusterService, clusterPlugins, clusterInfoService,
                snapshotsInfoService, threadPool.getThreadContext());
            modules.add(clusterModule);
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            modules.add(indicesModule);

            SearchModule searchModule = new SearchModule(settings, false, pluginsService.filterPlugins(SearchPlugin.class));
            List<BreakerSettings> pluginCircuitBreakers = pluginsService.filterPlugins(CircuitBreakerPlugin.class)
                .stream()
                .map(plugin -> plugin.getCircuitBreaker(settings))
                .collect(Collectors.toList());
            final CircuitBreakerService circuitBreakerService = createCircuitBreakerService(settingsModule.getSettings(),
                pluginCircuitBreakers,
                settingsModule.getClusterSettings());
            pluginsService.filterPlugins(CircuitBreakerPlugin.class)
                .forEach(plugin -> {
                    CircuitBreaker breaker = circuitBreakerService.getBreaker(plugin.getCircuitBreaker(settings).getName());
                    plugin.setCircuitBreaker(breaker);
                });
            resourcesToClose.add(circuitBreakerService);
            modules.add(new GatewayModule());


            PageCacheRecycler pageCacheRecycler = createPageCacheRecycler(settings);
            BigArrays bigArrays = createBigArrays(pageCacheRecycler, circuitBreakerService);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                indicesModule.getNamedWriteables().stream(),
                searchModule.getNamedWriteables().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedWriteables().stream()),
                ClusterModule.getNamedWriteables().stream())
                .flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                searchModule.getNamedXContents().stream(),
                pluginsService.filterPlugins(Plugin.class).stream()
                    .flatMap(p -> p.getNamedXContent().stream()),
                ClusterModule.getNamedXWriteables().stream())
                .flatMap(Function.identity()).collect(toList()));
            final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);
            final PersistedClusterStateService lucenePersistedStateFactory
                = new PersistedClusterStateService(nodeEnvironment, xContentRegistry, bigArrays, clusterService.getClusterSettings(),
                threadPool::relativeTimeInMillis);

            // collect engine factory providers from plugins
            final Collection<EnginePlugin> enginePlugins = pluginsService.filterPlugins(EnginePlugin.class);
            final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders =
                    enginePlugins.stream().map(plugin -> (Function<IndexSettings, Optional<EngineFactory>>)plugin::getEngineFactory)
                            .collect(Collectors.toList());


            final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories =
                    pluginsService.filterPlugins(IndexStorePlugin.class)
                            .stream()
                            .map(IndexStorePlugin::getDirectoryFactories)
                            .flatMap(m -> m.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories =
                pluginsService.filterPlugins(IndexStorePlugin.class)
                    .stream()
                    .map(IndexStorePlugin::getRecoveryStateFactories)
                    .flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final Map<String, Collection<SystemIndexDescriptor>> systemIndexDescriptorMap = Collections.unmodifiableMap(pluginsService
                .filterPlugins(SystemIndexPlugin.class)
                .stream()
                .collect(Collectors.toMap(
                    plugin -> plugin.getClass().getSimpleName(),
                    plugin -> plugin.getSystemIndexDescriptors(settings))));
            final SystemIndices systemIndices = new SystemIndices(systemIndexDescriptorMap);

            final RerouteService rerouteService
                = new BatchedRerouteService(clusterService, clusterModule.getAllocationService()::reroute);
            rerouteServiceReference.set(rerouteService);
            clusterService.setRerouteService(rerouteService);

            final IndicesService indicesService =
                new IndicesService(settings, pluginsService, nodeEnvironment, xContentRegistry, analysisModule.getAnalysisRegistry(),
                    clusterModule.getIndexNameExpressionResolver(), indicesModule.getMapperRegistry(), namedWriteableRegistry,
                    threadPool, settingsModule.getIndexScopedSettings(), circuitBreakerService, bigArrays, scriptService,
                    clusterService, client, metaStateService, engineFactoryProviders, indexStoreFactories,
                    searchModule.getValuesSourceRegistry(), recoveryStateFactories);

            final AliasValidator aliasValidator = new AliasValidator();

            final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService);
            final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                    settings,
                    clusterService,
                    indicesService,
                    clusterModule.getAllocationService(),
                    aliasValidator,
                shardLimitValidator,
                    environment,
                    settingsModule.getIndexScopedSettings(),
                    threadPool,
                    xContentRegistry,
                    systemIndices,
                    forbidPrivateIndexSettings
            );
            pluginsService.filterPlugins(Plugin.class)
                .forEach(p -> p.getAdditionalIndexSettingProviders()
                    .forEach(metadataCreateIndexService::addAdditionalIndexSettingProvider));

            final MetadataCreateDataStreamService metadataCreateDataStreamService =
                new MetadataCreateDataStreamService(threadPool, clusterService, metadataCreateIndexService);

            Collection<Object> pluginComponents = pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.createComponents(client, clusterService, threadPool, resourceWatcherService,
                                                 scriptService, xContentRegistry, environment, nodeEnvironment,
                                                 namedWriteableRegistry, clusterModule.getIndexNameExpressionResolver(),
                                                 repositoriesServiceReference::get).stream())
                .collect(Collectors.toList());

            ActionModule actionModule = new ActionModule(false, settings, clusterModule.getIndexNameExpressionResolver(),
                settingsModule.getIndexScopedSettings(), settingsModule.getClusterSettings(), settingsModule.getSettingsFilter(),
                threadPool, pluginsService.filterPlugins(ActionPlugin.class), client, circuitBreakerService, usageService, systemIndices);
            modules.add(actionModule);

            final RestController restController = actionModule.getRestController();
            final NetworkModule networkModule = new NetworkModule(settings, false, pluginsService.filterPlugins(NetworkPlugin.class),
                threadPool, bigArrays, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, xContentRegistry,
                networkService, restController, clusterService.getClusterSettings());
            Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders =
                pluginsService.filterPlugins(Plugin.class).stream()
                    .map(Plugin::getIndexTemplateMetadataUpgrader)
                    .collect(Collectors.toList());
            final MetadataUpgrader metadataUpgrader = new MetadataUpgrader(indexTemplateMetadataUpgraders);
            final MetadataIndexUpgradeService metadataIndexUpgradeService = new MetadataIndexUpgradeService(settings, xContentRegistry,
                indicesModule.getMapperRegistry(), settingsModule.getIndexScopedSettings(), systemIndices, scriptService);
            if (DiscoveryNode.isMasterNode(settings)) {
                clusterService.addListener(new SystemIndexMetadataUpgradeService(systemIndices, clusterService));
            }
            new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders);
            final Transport transport = networkModule.getTransportSupplier().get();
            Set<String> taskHeaders = Stream.concat(
                pluginsService.filterPlugins(ActionPlugin.class).stream().flatMap(p -> p.getTaskHeaders().stream()),
                Stream.of(Task.X_OPAQUE_ID)
            ).collect(Collectors.toSet());
            final TransportService transportService = newTransportService(settings, transport, threadPool,
                networkModule.getTransportInterceptor(), localNodeFactory, settingsModule.getClusterSettings(), taskHeaders);
            final GatewayMetaState gatewayMetaState = new GatewayMetaState();
            final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
            final SearchTransportService searchTransportService =  new SearchTransportService(transportService,
                SearchExecutionStatsCollector.makeWrapper(responseCollectorService));
            final HttpServerTransport httpServerTransport = newHttpTransport(networkModule);
            final IndexingPressure indexingLimits = new IndexingPressure(settings);

            final RecoverySettings recoverySettings = new RecoverySettings(settings, settingsModule.getClusterSettings());
            RepositoriesModule repositoriesModule = new RepositoriesModule(this.environment,
                pluginsService.filterPlugins(RepositoryPlugin.class), transportService, clusterService, threadPool, xContentRegistry,
                recoverySettings);
            RepositoriesService repositoryService = repositoriesModule.getRepositoryService();
            repositoriesServiceReference.set(repositoryService);
            SnapshotsService snapshotsService = new SnapshotsService(settings, clusterService,
                    clusterModule.getIndexNameExpressionResolver(), repositoryService, transportService, actionModule.getActionFilters());
            SnapshotShardsService snapshotShardsService = new SnapshotShardsService(settings, clusterService, repositoryService,
                    transportService, indicesService);
            TransportNodesSnapshotsStatus nodesSnapshotsStatus = new TransportNodesSnapshotsStatus(threadPool, clusterService,
                transportService, snapshotShardsService, actionModule.getActionFilters());
            RestoreService restoreService = new RestoreService(clusterService, repositoryService, clusterModule.getAllocationService(),
                metadataCreateIndexService, metadataIndexUpgradeService, clusterService.getClusterSettings(), shardLimitValidator);

            final DiskThresholdMonitor diskThresholdMonitor = new DiskThresholdMonitor(settings, clusterService::state,
                clusterService.getClusterSettings(), client, threadPool::relativeTimeInMillis, rerouteService);
            clusterInfoService.addListener(diskThresholdMonitor::onNewInfo);

            final DiscoveryModule discoveryModule = new DiscoveryModule(settings, threadPool, transportService, namedWriteableRegistry,
                networkService, clusterService.getMasterService(), clusterService.getClusterApplierService(),
                clusterService.getClusterSettings(), pluginsService.filterPlugins(DiscoveryPlugin.class),
                clusterModule.getAllocationService(), environment.configFile(), gatewayMetaState, rerouteService,
                fsHealthService);
            this.nodeService = new NodeService(settings, threadPool, monitorService, discoveryModule.getDiscovery(),
                transportService, indicesService, pluginsService, circuitBreakerService, scriptService,
                httpServerTransport, ingestService, clusterService, settingsModule.getSettingsFilter(), responseCollectorService,
                searchTransportService, indexingLimits, searchModule.getValuesSourceRegistry().getUsageService());

            final SearchService searchService = newSearchService(clusterService, indicesService,
                threadPool, scriptService, bigArrays, searchModule.getFetchPhase(),
                responseCollectorService, circuitBreakerService);

            final List<PersistentTasksExecutor<?>> tasksExecutors = pluginsService
                .filterPlugins(PersistentTaskPlugin.class).stream()
                .map(p -> p.getPersistentTasksExecutor(clusterService, threadPool, client, settingsModule,
                    clusterModule.getIndexNameExpressionResolver()))
                .flatMap(List::stream)
                .collect(toList());

            final PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(tasksExecutors);
            final PersistentTasksClusterService persistentTasksClusterService =
                new PersistentTasksClusterService(settings, registry, clusterService, threadPool);
            resourcesToClose.add(persistentTasksClusterService);
            final PersistentTasksService persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);

            modules.add(b -> {
                    b.bind(Node.class).toInstance(this);
                    b.bind(NodeService.class).toInstance(nodeService);
                    b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
                    b.bind(PluginsService.class).toInstance(pluginsService);
                    b.bind(Client.class).toInstance(client);
                    b.bind(NodeClient.class).toInstance(client);
                    b.bind(Environment.class).toInstance(this.environment);
                    b.bind(ThreadPool.class).toInstance(threadPool);
                    b.bind(NodeEnvironment.class).toInstance(nodeEnvironment);
                    b.bind(ResourceWatcherService.class).toInstance(resourceWatcherService);
                    b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                    b.bind(BigArrays.class).toInstance(bigArrays);
                    b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
                    b.bind(ScriptService.class).toInstance(scriptService);
                    b.bind(AnalysisRegistry.class).toInstance(analysisModule.getAnalysisRegistry());
                    b.bind(IngestService.class).toInstance(ingestService);
                    b.bind(IndexingPressure.class).toInstance(indexingLimits);
                    b.bind(UsageService.class).toInstance(usageService);
                    b.bind(AggregationUsageService.class).toInstance(searchModule.getValuesSourceRegistry().getUsageService());
                    b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                    b.bind(MetadataUpgrader.class).toInstance(metadataUpgrader);
                    b.bind(MetaStateService.class).toInstance(metaStateService);
                    b.bind(PersistedClusterStateService.class).toInstance(lucenePersistedStateFactory);
                    b.bind(IndicesService.class).toInstance(indicesService);
                    b.bind(AliasValidator.class).toInstance(aliasValidator);
                    b.bind(MetadataCreateIndexService.class).toInstance(metadataCreateIndexService);
                    b.bind(MetadataCreateDataStreamService.class).toInstance(metadataCreateDataStreamService);
                    b.bind(SearchService.class).toInstance(searchService);
                    b.bind(SearchTransportService.class).toInstance(searchTransportService);
                    b.bind(SearchPhaseController.class).toInstance(new SearchPhaseController(
                        namedWriteableRegistry, searchService::aggReduceContextBuilder));
                    b.bind(Transport.class).toInstance(transport);
                    b.bind(TransportService.class).toInstance(transportService);
                    b.bind(NetworkService.class).toInstance(networkService);
                    b.bind(UpdateHelper.class).toInstance(new UpdateHelper(scriptService));
                    b.bind(MetadataIndexUpgradeService.class).toInstance(metadataIndexUpgradeService);
                    b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
                    b.bind(SnapshotsInfoService.class).toInstance(snapshotsInfoService);
                    b.bind(GatewayMetaState.class).toInstance(gatewayMetaState);
                    b.bind(Discovery.class).toInstance(discoveryModule.getDiscovery());
                    {
                        processRecoverySettings(settingsModule.getClusterSettings(), recoverySettings);
                        b.bind(PeerRecoverySourceService.class).toInstance(new PeerRecoverySourceService(transportService,
                                indicesService, recoverySettings));
                        b.bind(PeerRecoveryTargetService.class).toInstance(new PeerRecoveryTargetService(threadPool,
                                transportService, recoverySettings, clusterService));
                    }
                    b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
                    pluginComponents.stream().forEach(p -> b.bind((Class) p.getClass()).toInstance(p));
                    b.bind(PersistentTasksService.class).toInstance(persistentTasksService);
                    b.bind(PersistentTasksClusterService.class).toInstance(persistentTasksClusterService);
                    b.bind(PersistentTasksExecutorRegistry.class).toInstance(registry);
                    b.bind(RepositoriesService.class).toInstance(repositoryService);
                    b.bind(SnapshotsService.class).toInstance(snapshotsService);
                    b.bind(SnapshotShardsService.class).toInstance(snapshotShardsService);
                    b.bind(TransportNodesSnapshotsStatus.class).toInstance(nodesSnapshotsStatus);
                    b.bind(RestoreService.class).toInstance(restoreService);
                    b.bind(RerouteService.class).toInstance(rerouteService);
                    b.bind(ShardLimitValidator.class).toInstance(shardLimitValidator);
                    b.bind(FsHealthService.class).toInstance(fsHealthService);
                    b.bind(SystemIndices.class).toInstance(systemIndices);
                }
            );
            injector = modules.createInjector();

            // We allocate copies of existing shards by looking for a viable copy of the shard in the cluster and assigning the shard there.
            // The search for viable copies is triggered by an allocation attempt (i.e. a reroute) and is performed asynchronously. When it
            // completes we trigger another reroute to try the allocation again. This means there is a circular dependency: the allocation
            // service needs access to the existing shards allocators (e.g. the GatewayAllocator) which need to be able to trigger a
            // reroute, which needs to call into the allocation service. We close the loop here:
            clusterModule.setExistingShardsAllocators(injector.getInstance(GatewayAllocator.class));

            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream()
                .filter(p -> p instanceof LifecycleComponent)
                .map(p -> (LifecycleComponent) p).collect(Collectors.toList());
            pluginLifecycleComponents.addAll(pluginsService.getGuiceServiceClasses().stream()
                .map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);
            resourcesToClose.add(injector.getInstance(PeerRecoverySourceService.class));
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            client.initialize(injector.getInstance(new Key<Map<ActionType, TransportAction>>() {}),
                    () -> clusterService.localNode().getId(), transportService.getRemoteClusterService(),
                    namedWriteableRegistry);
            logger.debug("initializing HTTP handlers ...");
            actionModule.initRestHandlers(() -> clusterService.state().nodes());
            logger.info("initialized");

            success = true;
        } catch (IOException ex) {
            throw new HavenaskException("failed to bind service", ex);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }

    protected TransportService newTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                                                   TransportInterceptor interceptor,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   ClusterSettings clusterSettings, Set<String> taskHeaders) {
        return new TransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
    }

    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        // Noop in production, overridden by tests
    }

    /**
     * The settings that are used by this node. Contains original settings as well as additional settings provided by plugins.
     */
    public Settings settings() {
        return this.environment.settings();
    }

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    public Client client() {
        return client;
    }

    /**
     * Returns the environment of the node
     */
    public Environment getEnvironment() {
        return environment;
    }

    /**
     * Returns the {@link NodeEnvironment} instance of this node
     */
    public NodeEnvironment getNodeEnvironment() {
        return nodeEnvironment;
    }


    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    public Node start() throws NodeValidationException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        logger.info("starting ...");
        pluginLifecycleComponents.forEach(LifecycleComponent::start);

        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        injector.getInstance(RepositoriesService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(FsHealthService.class).start();
        nodeService.getMonitorService().start();

        final ClusterService clusterService = injector.getInstance(ClusterService.class);

        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);

        injector.getInstance(GatewayService.class).start();
        Discovery discovery = injector.getInstance(Discovery.class);
        clusterService.getMasterService().setClusterStatePublisher(discovery::publish);

        // Start the transport service now so the publish address will be added to the local disco node in ClusterService
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.getTaskManager().setTaskResultsService(injector.getInstance(TaskResultsService.class));
        transportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(transportService));
        transportService.start();
        assert localNodeFactory.getNode() != null;
        assert transportService.getLocalNode().equals(localNodeFactory.getNode())
            : "transportService has a different local node than the factory provided";
        injector.getInstance(PeerRecoverySourceService.class).start();

        // Load (and maybe upgrade) the metadata stored on disk
        final GatewayMetaState gatewayMetaState = injector.getInstance(GatewayMetaState.class);
        gatewayMetaState.start(settings(), transportService, clusterService, injector.getInstance(MetaStateService.class),
            injector.getInstance(MetadataIndexUpgradeService.class), injector.getInstance(MetadataUpgrader.class),
            injector.getInstance(PersistedClusterStateService.class));
        if (Assertions.ENABLED) {
            try {
                if (DiscoveryModule.DISCOVERY_TYPE_SETTING.get(environment.settings()).equals(
                    DiscoveryModule.ZEN_DISCOVERY_TYPE) == false) {
                    assert injector.getInstance(MetaStateService.class).loadFullState().v1().isEmpty();
                }
                final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY,
                    nodeEnvironment.nodeDataPaths());
                assert nodeMetadata != null;
                assert nodeMetadata.nodeVersion().equals(Version.CURRENT);
                assert nodeMetadata.nodeId().equals(localNodeFactory.getNode().getId());
            } catch (IOException e) {
                assert false : e;
            }
        }
        // we load the global state here (the persistent part of the cluster state stored on disk) to
        // pass it to the bootstrap checks to allow plugins to enforce certain preconditions based on the recovered state.
        final Metadata onDiskMetadata = gatewayMetaState.getPersistedState().getLastAcceptedState().metadata();
        assert onDiskMetadata != null : "metadata is null but shouldn't"; // this is never null
        validateNodeBeforeAcceptingRequests(new BootstrapContext(environment, onDiskMetadata), transportService.boundAddress(),
            pluginsService.filterPlugins(Plugin.class).stream()
                .flatMap(p -> p.getBootstrapChecks().stream()).collect(Collectors.toList()));

        clusterService.addStateApplier(transportService.getTaskManager());
        // start after transport service so the local disco is known
        discovery.start(); // start before cluster service so that it can set initial state on ClusterApplierService
        clusterService.start();
        assert clusterService.localNode().equals(localNodeFactory.getNode())
            : "clusterService has a different local node than the factory provided";
        transportService.acceptIncomingRequests();
        discovery.startInitialJoin();
        final TimeValue initialStateTimeout = DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings());
        configureNodeAndClusterIdStateListener(clusterService);

        if (initialStateTimeout.millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterState clusterState = clusterService.state();
            ClusterStateObserver observer =
                new ClusterStateObserver(clusterState, clusterService, null, logger, thread.getThreadContext());

            if (clusterState.nodes().getMasterNodeId() == null) {
                logger.debug("waiting to join the cluster. timeout [{}]", initialStateTimeout);
                final CountDownLatch latch = new CountDownLatch(1);
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) { latch.countDown(); }

                    @Override
                    public void onClusterServiceClose() {
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.warn("timed out while waiting for initial discovery state - timeout: {}",
                            initialStateTimeout);
                        latch.countDown();
                    }
                }, state -> state.nodes().getMasterNodeId() != null, initialStateTimeout);

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new HavenaskTimeoutException("Interrupted while waiting for initial discovery state");
                }
            }
        }

        injector.getInstance(HttpServerTransport.class).start();

        if (WRITE_PORTS_FILE_SETTING.get(settings())) {
            TransportService transport = injector.getInstance(TransportService.class);
            writePortsFile("transport", transport.boundAddress());
            HttpServerTransport http = injector.getInstance(HttpServerTransport.class);
            writePortsFile("http", http.boundAddress());
        }

        logger.info("started");

        pluginsService.filterPlugins(ClusterPlugin.class).forEach(ClusterPlugin::onNodeStarted);

        return this;
    }

    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        NodeAndClusterIdStateListener.getAndSetNodeIdAndClusterId(clusterService,
            injector.getInstance(ThreadPool.class).getThreadContext());
    }

    private Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        logger.info("stopping ...");

        injector.getInstance(ResourceWatcherService.class).close();
        injector.getInstance(HttpServerTransport.class).stop();

        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        injector.getInstance(RepositoriesService.class).stop();
        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // close discovery early to not react to pings anymore.
        // This can confuse other nodes and delay things - mostly if we're the master and we're running tests.
        injector.getInstance(Discovery.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        injector.getInstance(FsHealthService.class).stop();
        nodeService.getMonitorService().stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(TransportService.class).stop();

        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless:
    // close() might not be executed, in case another (for example api) call to close() has already set some lifecycles to stopped.
    // In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
    public synchronized void close() throws IOException {
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                stop();
            }
            if (!lifecycle.moveToClosed()) {
                return;
            }
        }

        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("node_service"));
        toClose.add(nodeService);
        toClose.add(() -> stopWatch.stop().start("http"));
        toClose.add(injector.getInstance(HttpServerTransport.class));
        toClose.add(() -> stopWatch.stop().start("snapshot_service"));
        toClose.add(injector.getInstance(SnapshotsService.class));
        toClose.add(injector.getInstance(SnapshotShardsService.class));
        toClose.add(injector.getInstance(RepositoriesService.class));
        toClose.add(() -> stopWatch.stop().start("client"));
        Releasables.close(injector.getInstance(Client.class));
        toClose.add(() -> stopWatch.stop().start("indices_cluster"));
        toClose.add(injector.getInstance(IndicesClusterStateService.class));
        toClose.add(() -> stopWatch.stop().start("indices"));
        toClose.add(injector.getInstance(IndicesService.class));
        // close filter/fielddata caches after indices
        toClose.add(injector.getInstance(IndicesStore.class));
        toClose.add(injector.getInstance(PeerRecoverySourceService.class));
        toClose.add(() -> stopWatch.stop().start("cluster"));
        toClose.add(injector.getInstance(ClusterService.class));
        toClose.add(() -> stopWatch.stop().start("node_connections_service"));
        toClose.add(injector.getInstance(NodeConnectionsService.class));
        toClose.add(() -> stopWatch.stop().start("discovery"));
        toClose.add(injector.getInstance(Discovery.class));
        toClose.add(() -> stopWatch.stop().start("monitor"));
        toClose.add(nodeService.getMonitorService());
        toClose.add(() -> stopWatch.stop().start("fsHealth"));
        toClose.add(injector.getInstance(FsHealthService.class));
        toClose.add(() -> stopWatch.stop().start("gateway"));
        toClose.add(injector.getInstance(GatewayService.class));
        toClose.add(() -> stopWatch.stop().start("search"));
        toClose.add(injector.getInstance(SearchService.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));

        toClose.add(() -> stopWatch.stop().start("script"));
        toClose.add(injector.getInstance(ScriptService.class));

        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        // Don't call shutdownNow here, it might break ongoing operations on Lucene indices.
        // See https://issues.apache.org/jira/browse/LUCENE-7248. We call shutdownNow in
        // awaitClose if the node doesn't finish closing within the specified time.

        toClose.add(() -> stopWatch.stop().start("gateway_meta_state"));
        toClose.add(injector.getInstance(GatewayMetaState.class));

        toClose.add(() -> stopWatch.stop().start("node_environment"));
        toClose.add(injector.getInstance(NodeEnvironment.class));
        toClose.add(stopWatch::stop);

        if (logger.isTraceEnabled()) {
            toClose.add(() -> logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint()));
        }
        IOUtils.close(toClose);
        logger.info("closed");
    }

    /**
     * Wait for this node to be effectively closed.
     */
    // synchronized to prevent running concurrently with close()
    public synchronized boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (lifecycle.closed() == false) {
            // We don't want to shutdown the threadpool or interrupt threads on a node that is not
            // closed yet.
            throw new IllegalStateException("Call close() first");
        }


        ThreadPool threadPool = injector.getInstance(ThreadPool.class);
        final boolean terminated = ThreadPool.terminate(threadPool, timeout, timeUnit);
        if (terminated) {
            // All threads terminated successfully. Because search, recovery and all other operations
            // that run on shards run in the threadpool, indices should be effectively closed by now.
            if (nodeService.awaitClose(0, TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException("Some shards are still open after the threadpool terminated. " +
                        "Something is leaking index readers or store references.");
            }
        }
        return terminated;
    }

    /**
     * Returns {@code true} if the node is closed.
     */
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    /**
     * Hook for validating the node after network
     * services are started but before the cluster service is started
     * and before the network service starts accepting incoming network
     * requests.
     *
     * @param context               the bootstrap context for this node
     * @param boundTransportAddress the network addresses the node is
     *                              bound and publishing to
     */
    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(
        final BootstrapContext context,
        final BoundTransportAddress boundTransportAddress, List<BootstrapCheck> bootstrapChecks) throws NodeValidationException {
    }

    /** Writes a file to the logs dir containing the ports for the given transport type */
    private void writePortsFile(String type, BoundTransportAddress boundAddress) {
        Path tmpPortsFile = environment.logsFile().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, Charset.forName("UTF-8"))) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                writer.write(NetworkAddress.format(new InetSocketAddress(inetAddress, address.getPort())) + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ports file", e);
        }
        Path portsFile = environment.logsFile().resolve(type + ".ports");
        try {
            Files.move(tmpPortsFile, portsFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename ports file", e);
        }
    }

    /**
     * The {@link PluginsService} used to build this node's components.
     */
    protected PluginsService getPluginsService() {
        return pluginsService;
    }

    /**
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     * @see #BREAKER_TYPE_KEY
     */
    public static CircuitBreakerService createCircuitBreakerService(Settings settings,
                                                                    List<BreakerSettings> breakerSettings,
                                                                    ClusterSettings clusterSettings) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, breakerSettings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        return new PageCacheRecycler(settings);
    }

    /**
     * Creates a new the SearchService. This method can be overwritten by tests to inject mock implementations.
     */
    protected SearchService newSearchService(ClusterService clusterService, IndicesService indicesService,
                                             ThreadPool threadPool, ScriptService scriptService, BigArrays bigArrays,
                                             FetchPhase fetchPhase, ResponseCollectorService responseCollectorService,
                                             CircuitBreakerService circuitBreakerService) {
        return new SearchService(clusterService, indicesService, threadPool,
            scriptService, bigArrays, fetchPhase, responseCollectorService, circuitBreakerService);
    }

    /**
     * Creates a new the ScriptService. This method can be overwritten by tests to inject mock implementations.
     */
    protected ScriptService newScriptService(Settings settings, Map<String, ScriptEngine> engines, Map<String, ScriptContext<?>> contexts) {
        return new ScriptService(settings, engines, contexts);
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     * @param discoveryPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(settings());
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }

    /** Constructs a ClusterInfoService which may be mocked for tests. */
    protected ClusterInfoService newClusterInfoService(Settings settings, ClusterService clusterService,
                                                       ThreadPool threadPool, NodeClient client) {
        final InternalClusterInfoService service = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        if (DiscoveryNode.isMasterNode(settings)) {
            // listen for state changes (this node starts/stops being the elected master, or new nodes are added)
            clusterService.addListener(service);
        }
        return service;
    }

    /** Constructs a {@link org.havenask.http.HttpServerTransport} which may be mocked for tests. */
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        return networkModule.getHttpServerTransportSupplier().get();
    }

    private static class LocalNodeFactory implements Function<BoundTransportAddress, DiscoveryNode> {
        private final SetOnce<DiscoveryNode> localNode = new SetOnce<>();
        private final String persistentNodeId;
        private final Settings settings;

        private LocalNodeFactory(Settings settings, String persistentNodeId) {
            this.persistentNodeId = persistentNodeId;
            this.settings = settings;
        }

        @Override
        public DiscoveryNode apply(BoundTransportAddress boundTransportAddress) {
            localNode.set(DiscoveryNode.createLocal(settings, boundTransportAddress.publishAddress(), persistentNodeId));
            return localNode.get();
        }

        DiscoveryNode getNode() {
            assert localNode.get() != null;
            return localNode.get();
        }
    }
}
