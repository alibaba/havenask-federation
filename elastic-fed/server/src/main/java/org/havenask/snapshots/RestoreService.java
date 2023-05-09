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

package org.havenask.snapshots;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.StepListener;
import org.havenask.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.havenask.action.support.IndicesOptions;
import org.havenask.cluster.ClusterChangedEvent;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateApplier;
import org.havenask.cluster.ClusterStateTaskConfig;
import org.havenask.cluster.ClusterStateTaskExecutor;
import org.havenask.cluster.ClusterStateTaskListener;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.RestoreInProgress;
import org.havenask.cluster.RestoreInProgress.ShardRestoreStatus;
import org.havenask.cluster.SnapshotDeletionsInProgress;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.cluster.metadata.DataStreamMetadata;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexTemplateMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataCreateIndexService;
import org.havenask.cluster.metadata.MetadataIndexStateService;
import org.havenask.cluster.metadata.MetadataIndexUpgradeService;
import org.havenask.cluster.metadata.RepositoriesMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.RecoverySource;
import org.havenask.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.havenask.cluster.routing.RoutingChangesObserver;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.UnassignedInfo;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Priority;
import org.havenask.common.UUIDs;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.lucene.Lucene;
import org.havenask.common.regex.Regex;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.ShardLimitValidator;
import org.havenask.repositories.IndexId;
import org.havenask.repositories.RepositoriesService;
import org.havenask.repositories.Repository;
import org.havenask.repositories.RepositoryData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_HISTORY_UUID;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_VERSION_UPGRADED;
import static org.havenask.common.util.set.Sets.newHashSet;
import static org.havenask.snapshots.SnapshotUtils.filterIndices;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreSnapshotRequest, org.havenask.action.ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using
 * {@link RoutingTable.Builder#addAsRestore(IndexMetadata, SnapshotRecoverySource)} method.
 * <p>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link IndexShard#restoreFromRepository} )}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link ShardRouting#recoverySource()} property.
 * <p>
 * At the end of the successful restore process {@code RestoreService} calls {@link #cleanupRestoreState(ClusterChangedEvent)},
 * which removes {@link RestoreInProgress} when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 */
public class RestoreService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RestoreService.class);

    private static final Set<String> UNMODIFIABLE_SETTINGS = unmodifiableSet(newHashSet(
            SETTING_NUMBER_OF_SHARDS,
            SETTING_VERSION_CREATED,
            SETTING_INDEX_UUID,
            SETTING_CREATION_DATE,
            SETTING_HISTORY_UUID));

    // It's OK to change some settings, but we shouldn't allow simply removing them
    private static final Set<String> UNREMOVABLE_SETTINGS;

    static {
        Set<String> unremovable = new HashSet<>(UNMODIFIABLE_SETTINGS.size() + 4);
        unremovable.addAll(UNMODIFIABLE_SETTINGS);
        unremovable.add(SETTING_NUMBER_OF_REPLICAS);
        unremovable.add(SETTING_AUTO_EXPAND_REPLICAS);
        unremovable.add(SETTING_VERSION_UPGRADED);
        UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
    }

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final AllocationService allocationService;

    private final MetadataCreateIndexService createIndexService;

    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    private final ShardLimitValidator shardLimitValidator;

    private final ClusterSettings clusterSettings;

    private static final CleanRestoreStateTaskExecutor cleanRestoreStateTaskExecutor = new CleanRestoreStateTaskExecutor();

    public RestoreService(ClusterService clusterService, RepositoriesService repositoriesService,
                          AllocationService allocationService, MetadataCreateIndexService createIndexService,
                          MetadataIndexUpgradeService metadataIndexUpgradeService, ClusterSettings clusterSettings,
                          ShardLimitValidator shardLimitValidator) {
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addStateApplier(this);
        }
        this.clusterSettings = clusterService.getClusterSettings();
        this.shardLimitValidator = shardLimitValidator;
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restoreSnapshot(final RestoreSnapshotRequest request, final ActionListener<RestoreCompletionResponse> listener) {
        try {
            // Read snapshot info and metadata from the repository
            final String repositoryName = request.repository();
            Repository repository = repositoriesService.repository(repositoryName);
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            repository.getRepositoryData(repositoryDataListener);
            repositoryDataListener.whenComplete(repositoryData -> {
                final String snapshotName = request.snapshot();
                final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds().stream()
                    .filter(s -> snapshotName.equals(s.getName())).findFirst();
                if (matchingSnapshotId.isPresent() == false) {
                    throw new SnapshotRestoreException(repositoryName, snapshotName, "snapshot does not exist");
                }

                final SnapshotId snapshotId = matchingSnapshotId.get();
                if (request.snapshotUuid() != null && request.snapshotUuid().equals(snapshotId.getUUID()) == false) {
                    throw new SnapshotRestoreException(repositoryName, snapshotName,
                        "snapshot UUID mismatch: expected [" + request.snapshotUuid() + "] but got [" + snapshotId.getUUID() + "]");
                }

                final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

                // Make sure that we can restore from this snapshot
                validateSnapshotRestorable(repositoryName, snapshotInfo);

                Metadata globalMetadata = null;
                // Resolve the indices from the snapshot that need to be restored
                Map<String, DataStream> dataStreams;
                List<String> requestIndices = new ArrayList<>(Arrays.asList(request.indices()));

                List<String> requestedDataStreams = filterIndices(snapshotInfo.dataStreams(), requestIndices.toArray(new String[0]),
                    IndicesOptions.fromOptions(true, true, true, true));
                if (requestedDataStreams.isEmpty()) {
                    dataStreams = new HashMap<>();
                } else {
                    globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
                    final Map<String, DataStream> dataStreamsInSnapshot = globalMetadata.dataStreams();
                    dataStreams = new HashMap<>(requestedDataStreams.size());
                    for (String requestedDataStream : requestedDataStreams) {
                        final DataStream dataStreamInSnapshot = dataStreamsInSnapshot.get(requestedDataStream);
                        assert dataStreamInSnapshot != null : "DataStream [" + requestedDataStream + "] not found in snapshot";
                        dataStreams.put(requestedDataStream, dataStreamInSnapshot);
                    }
                }
                requestIndices.removeAll(dataStreams.keySet());
                Set<String> dataStreamIndices = dataStreams.values().stream()
                    .flatMap(ds -> ds.getIndices().stream())
                    .map(Index::getName)
                    .collect(Collectors.toSet());
                requestIndices.addAll(dataStreamIndices);

                final List<String> indicesInSnapshot = filterIndices(snapshotInfo.indices(), requestIndices.toArray(new String[0]),
                    request.indicesOptions());

                final Metadata.Builder metadataBuilder;
                if (request.includeGlobalState()) {
                    if (globalMetadata == null) {
                        globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
                    }
                    metadataBuilder = Metadata.builder(globalMetadata);
                } else {
                    metadataBuilder = Metadata.builder();
                }

                final List<IndexId> indexIdsInSnapshot = repositoryData.resolveIndices(indicesInSnapshot);
                for (IndexId indexId : indexIdsInSnapshot) {
                    metadataBuilder.put(repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId), false);
                }

                final Metadata metadata = metadataBuilder.build();

                // Apply renaming on index names, returning a map of names where
                // the key is the renamed index and the value is the original name
                final Map<String, String> indices = renamedIndices(request, indicesInSnapshot, dataStreamIndices);

                // Now we can start the actual restore process by adding shards to be recovered in the cluster state
                // and updating cluster metadata (global and index) as needed
                clusterService.submitStateUpdateTask("restore_snapshot[" + snapshotName + ']', new ClusterStateUpdateTask() {
                    final String restoreUUID = UUIDs.randomBase64UUID();
                    RestoreInfo restoreInfo = null;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
                        if (currentState.getNodes().getMinNodeVersion().before(LegacyESVersion.V_7_0_0)) {
                            // Check if another restore process is already running - cannot run two restore processes at the
                            // same time in versions prior to 7.0
                            if (restoreInProgress.isEmpty() == false) {
                                throw new ConcurrentSnapshotExecutionException(snapshot,
                                    "Restore process is already running in this cluster");
                            }
                        }
                        // Check if the snapshot to restore is currently being deleted
                        SnapshotDeletionsInProgress deletionsInProgress =
                            currentState.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
                        if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(snapshotId))) {
                            throw new ConcurrentSnapshotExecutionException(snapshot,
                                "cannot restore a snapshot while a snapshot deletion is in-progress [" +
                                    deletionsInProgress.getEntries().get(0) + "]");
                        }

                        // Updating cluster state
                        ClusterState.Builder builder = ClusterState.builder(currentState);
                        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                        ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards;
                        Set<String> aliases = new HashSet<>();

                        if (indices.isEmpty() == false) {
                            // We have some indices to restore
                            ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder =
                                ImmutableOpenMap.builder();
                            final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
                                .minimumIndexCompatibilityVersion();
                            for (Map.Entry<String, String> indexEntry : indices.entrySet()) {
                                String index = indexEntry.getValue();
                                boolean partial = checkPartial(index);
                                SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(restoreUUID, snapshot,
                                    snapshotInfo.version(), repositoryData.resolveIndexId(index));
                                String renamedIndexName = indexEntry.getKey();
                                IndexMetadata snapshotIndexMetadata = metadata.index(index);
                                snapshotIndexMetadata = updateIndexSettings(snapshotIndexMetadata,
                                    request.indexSettings(), request.ignoreIndexSettings());
                                try {
                                    snapshotIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(snapshotIndexMetadata,
                                        minIndexCompatibilityVersion);
                                } catch (Exception ex) {
                                    throw new SnapshotRestoreException(snapshot, "cannot restore index [" + index +
                                        "] because it cannot be upgraded", ex);
                                }
                                // Check that the index is closed or doesn't exist
                                IndexMetadata currentIndexMetadata = currentState.metadata().index(renamedIndexName);
                                IntSet ignoreShards = new IntHashSet();
                                final Index renamedIndex;
                                if (currentIndexMetadata == null) {
                                    // Index doesn't exist - create it and start recovery
                                    // Make sure that the index we are about to create has a validate name
                                    boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(snapshotIndexMetadata.getSettings());
                                    createIndexService.validateIndexName(renamedIndexName, currentState);
                                    createIndexService.validateDotIndex(renamedIndexName, isHidden);
                                    createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);
                                    IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                                        .state(IndexMetadata.State.OPEN)
                                        .index(renamedIndexName);
                                    indexMdBuilder.settings(Settings.builder()
                                        .put(snapshotIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()));
                                    shardLimitValidator.validateShardLimit(snapshotIndexMetadata.getSettings(), currentState);
                                    if (!request.includeAliases() && !snapshotIndexMetadata.getAliases().isEmpty()) {
                                        // Remove all aliases - they shouldn't be restored
                                        indexMdBuilder.removeAllAliases();
                                    } else {
                                        for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                            aliases.add(alias.value);
                                        }
                                    }
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.build();
                                    if (partial) {
                                        populateIgnoredShards(index, ignoreShards);
                                    }
                                    rtBuilder.addAsNewRestore(updatedIndexMetadata, recoverySource, ignoreShards);
                                    blocks.addBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                } else {
                                    validateExistingIndex(currentIndexMetadata, snapshotIndexMetadata, renamedIndexName, partial);
                                    // Index exists and it's closed - open it in metadata and start recovery
                                    IndexMetadata.Builder indexMdBuilder =
                                        IndexMetadata.builder(snapshotIndexMetadata).state(IndexMetadata.State.OPEN);
                                    indexMdBuilder.version(
                                        Math.max(snapshotIndexMetadata.getVersion(), 1 + currentIndexMetadata.getVersion()));
                                    indexMdBuilder.mappingVersion(
                                        Math.max(snapshotIndexMetadata.getMappingVersion(), 1 + currentIndexMetadata.getMappingVersion()));
                                    indexMdBuilder.settingsVersion(
                                        Math.max(
                                            snapshotIndexMetadata.getSettingsVersion(),
                                            1 + currentIndexMetadata.getSettingsVersion()));
                                    indexMdBuilder.aliasesVersion(
                                        Math.max(snapshotIndexMetadata.getAliasesVersion(), 1 + currentIndexMetadata.getAliasesVersion()));

                                    for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                        indexMdBuilder.primaryTerm(shard,
                                            Math.max(snapshotIndexMetadata.primaryTerm(shard), currentIndexMetadata.primaryTerm(shard)));
                                    }

                                    if (!request.includeAliases()) {
                                        // Remove all snapshot aliases
                                        if (!snapshotIndexMetadata.getAliases().isEmpty()) {
                                            indexMdBuilder.removeAllAliases();
                                        }
                                        /// Add existing aliases
                                        for (ObjectCursor<AliasMetadata> alias : currentIndexMetadata.getAliases().values()) {
                                            indexMdBuilder.putAlias(alias.value);
                                        }
                                    } else {
                                        for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                            aliases.add(alias.value);
                                        }
                                    }
                                    final Settings.Builder indexSettingsBuilder = Settings.builder()
                                            .put(snapshotIndexMetadata.getSettings())
                                            .put(IndexMetadata.SETTING_INDEX_UUID, currentIndexMetadata.getIndexUUID());
                                    // Only add a restore uuid if either all nodes in the cluster support it (version >= 7.9) or if the
                                    // index itself was created after 7.9 and thus won't be restored to a node that doesn't support the
                                    // setting anyway
                                    if (snapshotIndexMetadata.getCreationVersion().onOrAfter(LegacyESVersion.V_7_9_0) ||
                                            currentState.nodes().getMinNodeVersion().onOrAfter(LegacyESVersion.V_7_9_0)) {
                                        indexSettingsBuilder.put(SETTING_HISTORY_UUID, UUIDs.randomBase64UUID());
                                    }
                                    indexMdBuilder.settings(indexSettingsBuilder);
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.index(renamedIndexName).build();
                                    rtBuilder.addAsRestore(updatedIndexMetadata, recoverySource);
                                    blocks.updateBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                }

                                for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                    if (!ignoreShards.contains(shard)) {
                                        shardsBuilder.put(new ShardId(renamedIndex, shard),
                                            new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId()));
                                    } else {
                                        shardsBuilder.put(new ShardId(renamedIndex, shard),
                                            new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId(),
                                                RestoreInProgress.State.FAILURE));
                                    }
                                }
                            }

                            shards = shardsBuilder.build();
                            RestoreInProgress.Entry restoreEntry = new RestoreInProgress.Entry(
                                restoreUUID, snapshot, overallState(RestoreInProgress.State.INIT, shards),
                                Collections.unmodifiableList(new ArrayList<>(indices.keySet())),
                                shards
                            );
                            builder.putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder(
                                currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).add(restoreEntry).build());
                        } else {
                            shards = ImmutableOpenMap.of();
                        }

                        checkAliasNameConflicts(indices, aliases);

                        Map<String, DataStream> updatedDataStreams = new HashMap<>(currentState.metadata().dataStreams());
                        updatedDataStreams.putAll(dataStreams.values().stream()
                            .map(ds -> updateDataStream(ds, mdBuilder, request))
                            .collect(Collectors.toMap(DataStream::getName, Function.identity())));
                        mdBuilder.dataStreams(updatedDataStreams);

                        // Restore global state if needed
                        if (request.includeGlobalState()) {
                            if (metadata.persistentSettings() != null) {
                                Settings settings = metadata.persistentSettings();
                                clusterSettings.validateUpdate(settings);
                                mdBuilder.persistentSettings(settings);
                            }
                            if (metadata.templates() != null) {
                                // TODO: Should all existing templates be deleted first?
                                for (ObjectCursor<IndexTemplateMetadata> cursor : metadata.templates().values()) {
                                    mdBuilder.put(cursor.value);
                                }
                            }
                            if (metadata.customs() != null) {
                                for (ObjectObjectCursor<String, Metadata.Custom> cursor : metadata.customs()) {
                                    if (RepositoriesMetadata.TYPE.equals(cursor.key) == false
                                            && DataStreamMetadata.TYPE.equals(cursor.key) == false) {
                                        // Don't restore repositories while we are working with them
                                        // TODO: Should we restore them at the end?
                                        // Also, don't restore data streams here, we already added them to the metadata builder above
                                        mdBuilder.putCustom(cursor.key, cursor.value);
                                    }
                                }
                            }
                        }

                        if (completed(shards)) {
                            // We don't have any indices to restore - we are done
                            restoreInfo = new RestoreInfo(snapshotId.getName(),
                                Collections.unmodifiableList(new ArrayList<>(indices.keySet())),
                                shards.size(),
                                shards.size() - failedShards(shards));
                        }

                        RoutingTable rt = rtBuilder.build();
                        ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
                        return allocationService.reroute(updatedState, "restored snapshot [" + snapshot + "]");
                    }

                    private void checkAliasNameConflicts(Map<String, String> renamedIndices, Set<String> aliases) {
                        for (Map.Entry<String, String> renamedIndex : renamedIndices.entrySet()) {
                            if (aliases.contains(renamedIndex.getKey())) {
                                throw new SnapshotRestoreException(snapshot,
                                    "cannot rename index [" + renamedIndex.getValue() + "] into [" + renamedIndex.getKey()
                                        + "] because of conflict with an alias with the same name");
                            }
                        }
                    }

                    private void populateIgnoredShards(String index, IntSet ignoreShards) {
                        for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
                            if (index.equals(failure.index())) {
                                ignoreShards.add(failure.shardId());
                            }
                        }
                    }

                    private boolean checkPartial(String index) {
                        // Make sure that index was fully snapshotted
                        if (failed(snapshotInfo, index)) {
                            if (request.partial()) {
                                return true;
                            } else {
                                throw new SnapshotRestoreException(snapshot, "index [" + index + "] wasn't fully snapshotted - cannot " +
                                    "restore");
                            }
                        } else {
                            return false;
                        }
                    }

                    private void validateExistingIndex(IndexMetadata currentIndexMetadata, IndexMetadata snapshotIndexMetadata,
                        String renamedIndex, boolean partial) {
                        // Index exist - checking that it's closed
                        if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                            // TODO: Enable restore for open indices
                            throw new SnapshotRestoreException(snapshot, "cannot restore index [" + renamedIndex
                                + "] because an open index " +
                                "with same name already exists in the cluster. Either close or delete the existing index or restore the " +
                                "index under a different name by providing a rename pattern and replacement name");
                        }
                        // Index exist - checking if it's partial restore
                        if (partial) {
                            throw new SnapshotRestoreException(snapshot, "cannot restore partial index [" + renamedIndex
                                + "] because such index already exists");
                        }
                        // Make sure that the number of shards is the same. That's the only thing that we cannot change
                        if (currentIndexMetadata.getNumberOfShards() != snapshotIndexMetadata.getNumberOfShards()) {
                            throw new SnapshotRestoreException(snapshot,
                                "cannot restore index [" + renamedIndex + "] with [" + currentIndexMetadata.getNumberOfShards()
                                    + "] shards from a snapshot of index [" + snapshotIndexMetadata.getIndex().getName() + "] with [" +
                                    snapshotIndexMetadata.getNumberOfShards() + "] shards");
                        }
                    }

                    /**
                     * Optionally updates index settings in indexMetadata by removing settings listed in ignoreSettings and
                     * merging them with settings in changeSettings.
                     */
                    private IndexMetadata updateIndexSettings(IndexMetadata indexMetadata, Settings changeSettings,
                                                              String[] ignoreSettings) {
                        Settings normalizedChangeSettings = Settings.builder()
                            .put(changeSettings)
                            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
                            .build();
                        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings()) &&
                            IndexSettings.INDEX_SOFT_DELETES_SETTING.exists(changeSettings) &&
                            IndexSettings.INDEX_SOFT_DELETES_SETTING.get(changeSettings) == false) {
                            throw new SnapshotRestoreException(snapshot,
                                "cannot disable setting [" + IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey() + "] on restore");
                        }
                        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
                        Settings settings = indexMetadata.getSettings();
                        Set<String> keyFilters = new HashSet<>();
                        List<String> simpleMatchPatterns = new ArrayList<>();
                        for (String ignoredSetting : ignoreSettings) {
                            if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                                if (UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                                    throw new SnapshotRestoreException(
                                        snapshot, "cannot remove setting [" + ignoredSetting + "] on restore");
                                } else {
                                    keyFilters.add(ignoredSetting);
                                }
                            } else {
                                simpleMatchPatterns.add(ignoredSetting);
                            }
                        }
                        Predicate<String> settingsFilter = k -> {
                            if (UNREMOVABLE_SETTINGS.contains(k) == false) {
                                for (String filterKey : keyFilters) {
                                    if (k.equals(filterKey)) {
                                        return false;
                                    }
                                }
                                for (String pattern : simpleMatchPatterns) {
                                    if (Regex.simpleMatch(pattern, k)) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        };
                        Settings.Builder settingsBuilder = Settings.builder()
                            .put(settings.filter(settingsFilter))
                            .put(normalizedChangeSettings.filter(k -> {
                                if (UNMODIFIABLE_SETTINGS.contains(k)) {
                                    throw new SnapshotRestoreException(snapshot, "cannot modify setting [" + k + "] on restore");
                                } else {
                                    return true;
                                }
                            }));
                        settingsBuilder.remove(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey());
                        return builder.settings(settingsBuilder).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot", snapshotId), e);
                        listener.onFailure(e);
                    }

                    @Override
                    public TimeValue timeout() {
                        return request.masterNodeTimeout();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new RestoreCompletionResponse(restoreUUID, snapshot, restoreInfo));
                    }
                });
            }, listener::onFailure);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot",
                request.repository() + ":" + request.snapshot()), e);
            listener.onFailure(e);
        }
    }

    //visible for testing
    static DataStream updateDataStream(DataStream dataStream, Metadata.Builder metadata, RestoreSnapshotRequest request) {
        String dataStreamName = dataStream.getName();
        if (request.renamePattern() != null && request.renameReplacement() != null) {
            dataStreamName = dataStreamName.replaceAll(request.renamePattern(), request.renameReplacement());
        }
        List<Index> updatedIndices = dataStream.getIndices().stream()
            .map(i -> metadata.get(renameIndex(i.getName(), request, true)).getIndex())
            .collect(Collectors.toList());
        return new DataStream(dataStreamName, dataStream.getTimeStampField(), updatedIndices, dataStream.getGeneration());
    }

    public static RestoreInProgress updateRestoreStateWithDeletedIndices(RestoreInProgress oldRestore, Set<Index> deletedIndices) {
        boolean changesMade = false;
        RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
        for (RestoreInProgress.Entry entry : oldRestore) {
            ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = null;
            for (ObjectObjectCursor<ShardId, ShardRestoreStatus> cursor : entry.shards()) {
                ShardId shardId = cursor.key;
                if (deletedIndices.contains(shardId.getIndex())) {
                    changesMade = true;
                    if (shardsBuilder == null) {
                        shardsBuilder = ImmutableOpenMap.builder(entry.shards());
                    }
                    shardsBuilder.put(shardId,
                        new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "index was deleted"));
                }
            }
            if (shardsBuilder != null) {
                ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(),
                    overallState(RestoreInProgress.State.STARTED, shards), entry.indices(), shards));
            } else {
                builder.add(entry);
            }
        }
        if (changesMade) {
            return builder.build();
        } else {
            return oldRestore;
        }
    }

    public static final class RestoreCompletionResponse {
        private final String uuid;
        private final Snapshot snapshot;
        private final RestoreInfo restoreInfo;

        private RestoreCompletionResponse(final String uuid, final Snapshot snapshot, final RestoreInfo restoreInfo) {
            this.uuid = uuid;
            this.snapshot = snapshot;
            this.restoreInfo = restoreInfo;
        }

        public String getUuid() {
            return uuid;
        }

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

    public static class RestoreInProgressUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
        // Map of RestoreUUID to a of changes to the shards' restore statuses
        private final Map<String, Map<ShardId, ShardRestoreStatus>> shardChanges = new HashMap<>();

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            // mark snapshot as completed
            if (initializingShard.primary()) {
                RecoverySource recoverySource = initializingShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    changes(recoverySource).put(
                        initializingShard.shardId(),
                        new ShardRestoreStatus(initializingShard.currentNodeId(), RestoreInProgress.State.SUCCESS));
                }
            }
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            if (failedShard.primary() && failedShard.initializing()) {
                RecoverySource recoverySource = failedShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    // mark restore entry for this shard as failed when it's due to a file corruption. There is no need wait on retries
                    // to restore this shard on another node if the snapshot files are corrupt. In case where a node just left or crashed,
                    // however, we only want to acknowledge the restore operation once it has been successfully restored on another node.
                    if (unassignedInfo.getFailure() != null && Lucene.isCorruptionException(unassignedInfo.getFailure().getCause())) {
                        changes(recoverySource).put(
                            failedShard.shardId(), new ShardRestoreStatus(failedShard.currentNodeId(),
                                RestoreInProgress.State.FAILURE, unassignedInfo.getFailure().getCause().getMessage()));
                    }
                }
            }
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            // if we force an empty primary, we should also fail the restore entry
            if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT &&
                initializedShard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                changes(unassignedShard.recoverySource()).put(
                    unassignedShard.shardId(),
                    new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE,
                        "recovery source type changed from snapshot to " + initializedShard.recoverySource())
                );
            }
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            RecoverySource recoverySource = unassignedShard.recoverySource();
            if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                if (newUnassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
                    String reason = "shard could not be allocated to any of the nodes";
                    changes(recoverySource).put(
                        unassignedShard.shardId(),
                        new ShardRestoreStatus(unassignedShard.currentNodeId(), RestoreInProgress.State.FAILURE, reason));
                }
            }
        }

        /**
         * Helper method that creates update entry for the given recovery source's restore uuid
         * if such an entry does not exist yet.
         */
        private Map<ShardId, ShardRestoreStatus> changes(RecoverySource recoverySource) {
            assert recoverySource.getType() == RecoverySource.Type.SNAPSHOT;
            return shardChanges.computeIfAbsent(((SnapshotRecoverySource) recoverySource).restoreUUID(), k -> new HashMap<>());
        }

        public RestoreInProgress applyChanges(final RestoreInProgress oldRestore) {
            if (shardChanges.isEmpty() == false) {
                RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
                for (RestoreInProgress.Entry entry : oldRestore) {
                    Map<ShardId, ShardRestoreStatus> updates = shardChanges.get(entry.uuid());
                    ImmutableOpenMap<ShardId, ShardRestoreStatus> shardStates = entry.shards();
                    if (updates != null && updates.isEmpty() == false) {
                        ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder(shardStates);
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.entrySet()) {
                            ShardId shardId = shard.getKey();
                            ShardRestoreStatus status = shardStates.get(shardId);
                            if (status == null || status.state().completed() == false) {
                                shardsBuilder.put(shardId, shard.getValue());
                            }
                        }

                        ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), newState, entry.indices(), shards));
                    } else {
                        builder.add(entry);
                    }
                }
                return builder.build();
            } else {
                return oldRestore;
            }
        }
    }

    public static RestoreInProgress.Entry restoreInProgress(ClusterState state, String restoreUUID) {
        return state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).get(restoreUUID);
    }

    static class CleanRestoreStateTaskExecutor implements ClusterStateTaskExecutor<CleanRestoreStateTaskExecutor.Task>,
        ClusterStateTaskListener {

        static class Task {
            final String uuid;

            Task(String uuid) {
                this.uuid = uuid;
            }

            @Override
            public String toString() {
                return "clean restore state for restore " + uuid;
            }
        }

        @Override
        public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) {
            final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
            Set<String> completedRestores = tasks.stream().map(e -> e.uuid).collect(Collectors.toSet());
            RestoreInProgress.Builder restoreInProgressBuilder = new RestoreInProgress.Builder();
            boolean changed = false;
            for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
                if (completedRestores.contains(entry.uuid())) {
                    changed = true;
                } else {
                    restoreInProgressBuilder.add(entry);
                }
            }
            if (changed == false) {
                return resultBuilder.build(currentState);
            }
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(currentState.getCustoms());
            builder.put(RestoreInProgress.TYPE, restoreInProgressBuilder.build());
            ImmutableOpenMap<String, ClusterState.Custom> customs = builder.build();
            return resultBuilder.build(ClusterState.builder(currentState).customs(customs).build());
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("no longer master while processing restore state update [{}]", source);
        }

    }

    private void cleanupRestoreState(ClusterChangedEvent event) {
        for (RestoreInProgress.Entry entry : event.state().custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            if (entry.state().completed()) {
                assert completed(entry.shards()) : "state says completed but restore entries are not";
                clusterService.submitStateUpdateTask(
                    "clean up snapshot restore state",
                    new CleanRestoreStateTaskExecutor.Task(entry.uuid()),
                    ClusterStateTaskConfig.build(Priority.URGENT),
                    cleanRestoreStateTaskExecutor,
                    cleanRestoreStateTaskExecutor);
            }
        }
    }

    private static RestoreInProgress.State overallState(RestoreInProgress.State nonCompletedState,
                                                        ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        boolean hasFailed = false;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return nonCompletedState;
            }
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                hasFailed = true;
            }
        }
        if (hasFailed) {
            return RestoreInProgress.State.FAILURE;
        } else {
            return RestoreInProgress.State.SUCCESS;
        }
    }

    public static boolean completed(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return false;
            }
        }
        return true;
    }

    public static int failedShards(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    private static Map<String, String> renamedIndices(RestoreSnapshotRequest request, List<String> filteredIndices,
                                                      Set<String> dataStreamIndices) {
        Map<String, String> renamedIndices = new HashMap<>();
        for (String index : filteredIndices) {
            String renamedIndex = renameIndex(index, request, dataStreamIndices.contains(index));
            String previousIndex = renamedIndices.put(renamedIndex, index);
            if (previousIndex != null) {
                throw new SnapshotRestoreException(request.repository(), request.snapshot(),
                        "indices [" + index + "] and [" + previousIndex + "] are renamed into the same index [" + renamedIndex + "]");
            }
        }
        return Collections.unmodifiableMap(renamedIndices);
    }

    private static String renameIndex(String index, RestoreSnapshotRequest request, boolean partOfDataStream) {
        String renamedIndex = index;
        if (request.renameReplacement() != null && request.renamePattern() != null) {
            partOfDataStream = partOfDataStream && index.startsWith(DataStream.BACKING_INDEX_PREFIX);
            if (partOfDataStream) {
                index = index.substring(DataStream.BACKING_INDEX_PREFIX.length());
            }
            renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
            if (partOfDataStream) {
                renamedIndex = DataStream.BACKING_INDEX_PREFIX + renamedIndex;
            }
        }
        return renamedIndex;
    }

    /**
     * Checks that snapshots can be restored and have compatible version
     *
     * @param repository      repository name
     * @param snapshotInfo    snapshot metadata
     */
    private static void validateSnapshotRestorable(final String repository, final SnapshotInfo snapshotInfo) {
        if (!snapshotInfo.state().restorable()) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "unsupported snapshot state [" + snapshotInfo.state() + "]");
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "the snapshot was created with Havenask version [" + snapshotInfo.version() +
                                                   "] which is higher than the version of this node [" + Version.CURRENT + "]");
        }
    }

    public static boolean failed(SnapshotInfo snapshot, String index) {
        for (SnapshotShardFailure failure : snapshot.shardFailures()) {
            if (index.equals(failure.index())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the indices that are currently being restored and that are contained in the indices-to-check set.
     */
    public static Set<Index> restoringIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            for (ObjectObjectCursor<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards()) {
                Index index = shard.key.getIndex();
                if (indicesToCheck.contains(index)
                    && shard.value.state().completed() == false
                    && currentState.getMetadata().index(index) != null) {
                    indices.add(index);
                }
            }
        }
        return indices;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                cleanupRestoreState(event);
            }
        } catch (Exception t) {
            logger.warn("Failed to update restore state ", t);
        }
    }
}
