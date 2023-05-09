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

import org.havenask.index.IndexingPressure;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.Build;
import org.havenask.Version;
import org.havenask.action.admin.cluster.node.info.NodeInfo;
import org.havenask.action.admin.cluster.node.stats.NodeStats;
import org.havenask.action.admin.indices.stats.CommonStatsFlags;
import org.havenask.action.search.SearchTransportService;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.SettingsFilter;
import org.havenask.discovery.Discovery;
import org.havenask.http.HttpServerTransport;
import org.havenask.indices.IndicesService;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.ingest.IngestService;
import org.havenask.monitor.MonitorService;
import org.havenask.plugins.PluginsService;
import org.havenask.script.ScriptService;
import org.havenask.search.aggregations.support.AggregationUsageService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NodeService implements Closeable {
    private final Settings settings;
    private final ThreadPool threadPool;
    private final MonitorService monitorService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PluginsService pluginService;
    private final CircuitBreakerService circuitBreakerService;
    private final IngestService ingestService;
    private final SettingsFilter settingsFilter;
    private final ScriptService scriptService;
    private final HttpServerTransport httpServerTransport;
    private final ResponseCollectorService responseCollectorService;
    private final SearchTransportService searchTransportService;
    private final IndexingPressure indexingPressure;
    private final AggregationUsageService aggregationUsageService;

    private final Discovery discovery;

    NodeService(Settings settings, ThreadPool threadPool, MonitorService monitorService, Discovery discovery,
                TransportService transportService, IndicesService indicesService, PluginsService pluginService,
                CircuitBreakerService circuitBreakerService, ScriptService scriptService,
                @Nullable HttpServerTransport httpServerTransport, IngestService ingestService, ClusterService clusterService,
                SettingsFilter settingsFilter, ResponseCollectorService responseCollectorService,
                SearchTransportService searchTransportService, IndexingPressure indexingPressure,
                AggregationUsageService aggregationUsageService) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.monitorService = monitorService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.discovery = discovery;
        this.pluginService = pluginService;
        this.circuitBreakerService = circuitBreakerService;
        this.httpServerTransport = httpServerTransport;
        this.ingestService = ingestService;
        this.settingsFilter = settingsFilter;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.searchTransportService = searchTransportService;
        this.indexingPressure = indexingPressure;
        this.aggregationUsageService = aggregationUsageService;
        clusterService.addStateApplier(ingestService);
    }

    public NodeInfo info(boolean settings, boolean os, boolean process, boolean jvm, boolean threadPool,
                boolean transport, boolean http, boolean plugin, boolean ingest, boolean aggs, boolean indices) {
        return new NodeInfo(Version.CURRENT, Build.CURRENT, transportService.getLocalNode(),
                settings ? settingsFilter.filter(this.settings) : null,
                os ? monitorService.osService().info() : null,
                process ? monitorService.processService().info() : null,
                jvm ? monitorService.jvmService().info() : null,
                threadPool ? this.threadPool.info() : null,
                transport ? transportService.info() : null,
                http ? (httpServerTransport == null ? null : httpServerTransport.info()) : null,
                plugin ? (pluginService == null ? null : pluginService.info()) : null,
                ingest ? (ingestService == null ? null : ingestService.info()) : null,
                aggs ? (aggregationUsageService == null ? null : aggregationUsageService.info()) : null,
                indices ? indicesService.getTotalIndexingBufferBytes() : null
        );
    }

    public NodeStats stats(CommonStatsFlags indices, boolean os, boolean process, boolean jvm, boolean threadPool,
                           boolean fs, boolean transport, boolean http, boolean circuitBreaker,
                           boolean script, boolean discoveryStats, boolean ingest, boolean adaptiveSelection, boolean scriptCache,
                           boolean indexingPressure) {
        // for indices stats we want to include previous allocated shards stats as well (it will
        // only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
        return new NodeStats(transportService.getLocalNode(), System.currentTimeMillis(),
                indices.anySet() ? indicesService.stats(indices) : null,
                os ? monitorService.osService().stats() : null,
                process ? monitorService.processService().stats() : null,
                jvm ? monitorService.jvmService().stats() : null,
                threadPool ? this.threadPool.stats() : null,
                fs ? monitorService.fsService().stats() : null,
                transport ? transportService.stats() : null,
                http ? (httpServerTransport == null ? null : httpServerTransport.stats()) : null,
                circuitBreaker ? circuitBreakerService.stats() : null,
                script ? scriptService.stats() : null,
                discoveryStats ? discovery.stats() : null,
                ingest ? ingestService.stats() : null,
                adaptiveSelection ? responseCollectorService.getAdaptiveStats(searchTransportService.getPendingSearchRequests()) : null,
                scriptCache ? scriptService.cacheStats() : null,
                indexingPressure ? this.indexingPressure.stats() : null
        );
    }

    public IngestService getIngestService() {
        return ingestService;
    }

    public MonitorService getMonitorService() {
        return monitorService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indicesService);
    }

    /**
     * Wait for the node to be effectively closed.
     * @see IndicesService#awaitClose(long, TimeUnit)
     */
    public boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return indicesService.awaitClose(timeout, timeUnit);
    }

}
