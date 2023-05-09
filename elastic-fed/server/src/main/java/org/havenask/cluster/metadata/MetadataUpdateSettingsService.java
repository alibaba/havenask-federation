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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.ExceptionsHelper;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.havenask.action.admin.indices.upgrade.post.UpgradeSettingsClusterStateUpdateRequest;
import org.havenask.cluster.AckedClusterStateUpdateTask;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ack.ClusterStateUpdateResponse;
import org.havenask.cluster.block.ClusterBlock;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Priority;
import org.havenask.common.ValidationException;
import org.havenask.common.collect.Tuple;
import org.havenask.common.inject.Inject;
import org.havenask.common.regex.Regex;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;
import org.havenask.indices.IndicesService;
import org.havenask.indices.ShardLimitValidator;
import org.havenask.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.havenask.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.havenask.index.IndexSettings.same;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetadataUpdateSettingsService {
    private static final Logger logger = LogManager.getLogger(MetadataUpdateSettingsService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ShardLimitValidator shardLimitValidator;
    private final ThreadPool threadPool;

    @Inject
    public MetadataUpdateSettingsService(ClusterService clusterService, AllocationService allocationService,
                                         IndexScopedSettings indexScopedSettings, IndicesService indicesService,
                                         ShardLimitValidator shardLimitValidator, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
        this.shardLimitValidator = shardLimitValidator;
    }

    public void updateSettings(final UpdateSettingsClusterStateUpdateRequest request,
                               final ActionListener<ClusterStateUpdateResponse> listener) {
        final Settings normalizedSettings =
            Settings.builder().put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        indexScopedSettings.validate(
                normalizedSettings.filter(s -> Regex.isSimpleMatchPattern(s) == false), // don't validate wildcards
                false, // don't validate dependencies here we check it below never allow to change the number of shards
                true); // validate internal or private index settings
        for (String key : normalizedSettings.keySet()) {
            Setting setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            assert setting != null // we already validated the normalized settings
                || (isWildcard && normalizedSettings.hasValue(key) == false)
                : "unknown setting: " + key + " isWildcard: " + isWildcard + " hasValue: " + normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (isWildcard || setting.isDynamic()) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key);
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        final boolean preserveExisting = request.isPreserveExisting();

        clusterService.submitStateUpdateTask("update-settings " + Arrays.toString(request.indices()),
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request,
                    wrapPreservingContext(listener, threadPool.getThreadContext())) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {

                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                // allow to change any settings to a close index, and only allow dynamic settings to be changed
                // on an open index
                Set<Index> openIndices = new HashSet<>();
                Set<Index> closeIndices = new HashSet<>();
                final String[] actualIndices = new String[request.indices().length];
                for (int i = 0; i < request.indices().length; i++) {
                    Index index = request.indices()[i];
                    actualIndices[i] = index.getName();
                    final IndexMetadata metadata = currentState.metadata().getIndexSafe(index);
                    if (metadata.getState() == IndexMetadata.State.OPEN) {
                        openIndices.add(index);
                    } else {
                        closeIndices.add(index);
                    }
                }

                if (!skippedSettings.isEmpty() && !openIndices.isEmpty()) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Can't update non dynamic settings [%s] for open indices %s", skippedSettings, openIndices));
                }

                if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(openSettings)) {
                    final int updatedNumberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(openSettings);
                    if (preserveExisting == false) {
                        // Verify that this won't take us over the cluster shard limit.
                        int totalNewShards = Arrays.stream(request.indices())
                            .mapToInt(i -> getTotalNewShards(i, currentState, updatedNumberOfReplicas))
                            .sum();
                        Optional<String> error = shardLimitValidator.checkShardLimit(totalNewShards, currentState);
                        if (error.isPresent()) {
                            ValidationException ex = new ValidationException();
                            ex.addValidationError(error.get());
                            throw ex;
                        }

                        /*
                         * We do not update the in-sync allocation IDs as they will be removed upon the first index operation which makes
                         * these copies stale.
                         *
                         * TODO: should we update the in-sync allocation IDs once the data is deleted by the node?
                         */
                        routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                    }
                }

                if (!openIndices.isEmpty()) {
                    for (Index index : openIndices) {
                        IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                        Settings.Builder updates = Settings.builder();
                        Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                        if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                            if (preserveExisting) {
                                indexSettings.put(indexMetadata.getSettings());
                            }
                            /*
                             * The setting index.number_of_replicas is special; we require that this setting has a value in the index. When
                             * creating the index, we ensure this by explicitly providing a value for the setting to the default (one) if
                             * there is a not value provided on the source of the index creation. A user can update this setting though,
                             * including updating it to null, indicating that they want to use the default value. In this case, we again
                             * have to provide an explicit value for the setting to the default (one).
                             */
                            if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(indexSettings) == false) {
                                indexSettings.put(
                                    IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                                    IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(Settings.EMPTY));
                            }
                            Settings finalSettings = indexSettings.build();
                            indexScopedSettings.validate(
                                finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                            metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                        }
                    }
                }

                if (!closeIndices.isEmpty()) {
                    for (Index index : closeIndices) {
                        IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                        Settings.Builder updates = Settings.builder();
                        Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                        if (indexScopedSettings.updateSettings(closedSettings, indexSettings, updates, index.getName())) {
                            if (preserveExisting) {
                                indexSettings.put(indexMetadata.getSettings());
                            }
                            /*
                             * The setting index.number_of_replicas is special; we require that this setting has a value in the index. When
                             * creating the index, we ensure this by explicitly providing a value for the setting to the default (one) if
                             * there is a not value provided on the source of the index creation. A user can update this setting though,
                             * including updating it to null, indicating that they want to use the default value. In this case, we again
                             * have to provide an explicit value for the setting to the default (one).
                             */
                            if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(indexSettings) == false) {
                                indexSettings.put(
                                    IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                                    IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(Settings.EMPTY));
                            }
                            Settings finalSettings = indexSettings.build();
                            indexScopedSettings.validate(
                                finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                            metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                        }
                    }
                }

                if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(normalizedSettings) ||
                    IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(normalizedSettings)) {
                    for (String index : actualIndices) {
                        MetadataCreateIndexService.validateTranslogRetentionSettings(metadataBuilder.get(index).getSettings());
                    }
                }
                boolean changed = false;
                // increment settings versions
                for (final String index : actualIndices) {
                    if (same(currentState.metadata().index(index).getSettings(), metadataBuilder.get(index).getSettings()) == false) {
                        changed = true;
                        final IndexMetadata.Builder builder = IndexMetadata.builder(metadataBuilder.get(index));
                        builder.settingsVersion(1 + builder.settingsVersion());
                        metadataBuilder.put(builder);
                    }
                }

                final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                for (IndexMetadata.APIBlock block : IndexMetadata.APIBlock.values()) {
                    changed |= maybeUpdateClusterBlock(actualIndices, blocks, block.block, block.setting, openSettings);
                }

                if (changed == false) {
                    return currentState;
                }

                ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder)
                    .routingTable(routingTableBuilder.build()).blocks(blocks).build();

                // now, reroute in case things change that require it (like number of replicas)
                updatedState = allocationService.reroute(updatedState, "settings update");
                try {
                    for (Index index : openIndices) {
                        final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                        final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                        indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                    }
                    for (Index index : closeIndices) {
                        final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                        final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                        // Verifies that the current index settings can be updated with the updated dynamic settings.
                        indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                        // Now check that we can create the index with the updated settings (dynamic and non-dynamic).
                        // This step is mandatory since we allow to update non-dynamic settings on closed indices.
                        indicesService.verifyIndexMetadata(updatedMetadata, updatedMetadata);
                    }
                } catch (IOException ex) {
                    throw ExceptionsHelper.convertToHavenaskException(ex);
                }
                return updatedState;
            }
        });
    }

    private int getTotalNewShards(Index index, ClusterState currentState, int updatedNumberOfReplicas) {
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        int shardsInIndex = indexMetadata.getNumberOfShards();
        int oldNumberOfReplicas = indexMetadata.getNumberOfReplicas();
        int replicaIncrease = updatedNumberOfReplicas - oldNumberOfReplicas;
        return replicaIncrease * shardsInIndex;
    }

    /**
     * Updates the cluster block only iff the setting exists in the given settings
     */
    private static boolean maybeUpdateClusterBlock(String[] actualIndices, ClusterBlocks.Builder blocks, ClusterBlock block,
                                                Setting<Boolean> setting, Settings openSettings) {
        boolean changed = false;
        if (setting.exists(openSettings)) {
            final boolean updateBlock = setting.get(openSettings);
            for (String index : actualIndices) {
                if (updateBlock) {
                    if (blocks.hasIndexBlock(index, block) == false) {
                        blocks.addIndexBlock(index, block);
                        changed = true;
                    }
                } else {
                    if (blocks.hasIndexBlock(index, block)) {
                        blocks.removeIndexBlock(index, block);
                        changed = true;
                    }
                }
            }
        }
        return changed;
    }


    public void upgradeIndexSettings(final UpgradeSettingsClusterStateUpdateRequest request,
                                     final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("update-index-compatibility-versions",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request,
                wrapPreservingContext(listener, threadPool.getThreadContext())) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                for (Map.Entry<String, Tuple<Version, String>> entry : request.versions().entrySet()) {
                    String index = entry.getKey();
                    IndexMetadata indexMetadata = metadataBuilder.get(index);
                    if (indexMetadata != null) {
                        if (Version.CURRENT.equals(indexMetadata.getCreationVersion()) == false) {
                            // no reason to pollute the settings, we didn't really upgrade anything
                            metadataBuilder.put(
                                    IndexMetadata
                                            .builder(indexMetadata)
                                            .settings(
                                                    Settings
                                                            .builder()
                                                            .put(indexMetadata.getSettings())
                                                            .put(IndexMetadata.SETTING_VERSION_UPGRADED, entry.getValue().v1()))
                                            .settingsVersion(1 + indexMetadata.getSettingsVersion()));
                        }
                    }
                }
                return ClusterState.builder(currentState).metadata(metadataBuilder).build();
            }
        });
    }
}
