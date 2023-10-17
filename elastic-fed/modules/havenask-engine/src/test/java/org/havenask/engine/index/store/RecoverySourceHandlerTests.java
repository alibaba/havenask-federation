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

package org.havenask.engine.index.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.UUIDs;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.engine.HavenaskEngine.HavenaskCommitInfo;
import org.havenask.engine.util.Utils;
import org.havenask.index.IndexSettings;
import org.havenask.index.engine.Engine;
import org.havenask.index.seqno.ReplicationTracker;
import org.havenask.index.seqno.RetentionLeases;
import org.havenask.index.seqno.SequenceNumbers;
import org.havenask.index.shard.ShardId;
import org.havenask.index.store.Store;
import org.havenask.index.store.Store.OnClose;
import org.havenask.index.store.StoreFileMetadata;
import org.havenask.index.translog.Translog;
import org.havenask.indices.recovery.AsyncRecoveryTarget;
import org.havenask.indices.recovery.MultiFileWriter;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.indices.recovery.RecoverySourceHandler;
import org.havenask.indices.recovery.RecoveryState;
import org.havenask.indices.recovery.RecoveryTargetHandler;
import org.havenask.indices.recovery.StartRecoveryRequest;
import org.havenask.test.DummyShardLock;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.IndexSettingsModule;
import org.havenask.threadpool.FixedExecutorBuilder;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class RecoverySourceHandlerTests extends HavenaskTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.havenask.Version.CURRENT)
            .put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK)
            .build()
    );
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    private ThreadPool threadPool;
    private Executor recoveryExecutor;

    @Before
    public void setUpThreadPool() {
        if (randomBoolean()) {
            threadPool = new TestThreadPool(getTestName());
            recoveryExecutor = threadPool.generic();
        } else {
            // verify that both sending and receiving files can be completed with a single thread
            threadPool = new TestThreadPool(
                getTestName(),
                new FixedExecutorBuilder(Settings.EMPTY, "recovery_executor", between(1, 16), between(16, 128), "recovery_executor")
            );
            recoveryExecutor = threadPool.executor("recovery_executor");
        }
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testSendFiles() throws Throwable {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Path shardPath = createTempDir();
        Store store = newStore(createTempDir(), shardPath);

        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        if (randomBoolean()) {
            writer.w.setLiveCommitData(() -> {
                final Map<String, String> commitData = new HashMap<>();
                String version = randomFrom("0", "1", "2", "536870913", "536870914", "536870915");
                commitData.put(HavenaskCommitInfo.COMMIT_VERSION_KEY, version);
                return commitData.entrySet().iterator();
            });
        }
        writer.commit();
        IndexCommit commit = randomBoolean() ? writer.w.getConfig().getIndexCommit() : null;
        writer.close();

        // add havenask data
        String havenaskFiles = RecoverySourceHandlerTests.class.getResource("/partition.tar.gz").getPath();
        Utils.executeTarCommand(havenaskFiles, shardPath.toString());

        Store.MetadataSnapshot metadata = store.getMetadata(commit);
        List<StoreFileMetadata> metas = new ArrayList<>();
        for (StoreFileMetadata md : metadata) {
            metas.add(md);
        }
        Store targetStore = newStore(createTempDir(), createTempDir());
        MultiFileWriter multiFileWriter = new MultiFileWriter(targetStore, mock(RecoveryState.Index.class), "", logger, () -> {});
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void writeFileChunk(
                StoreFileMetadata md,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                ActionListener.completeWith(listener, () -> {
                    multiFileWriter.writeFileChunk(md, position, content, lastChunk);
                    return null;
                });
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(
            null,
            new AsyncRecoveryTarget(target, recoveryExecutor),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 5),
            between(1, 5)
        );
        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
        sendFilesFuture.actionGet();
        Store.MetadataSnapshot targetStoreMetadata = targetStore.getMetadata(commit);
        Store.RecoveryDiff recoveryDiff = targetStoreMetadata.recoveryDiff(metadata);
        assertEquals(metas.size(), recoveryDiff.identical.size());
        assertEquals(0, recoveryDiff.different.size());
        assertEquals(0, recoveryDiff.missing.size());
        IOUtils.close(store, multiFileWriter, targetStore);
    }

    public StartRecoveryRequest getStartRecoveryRequest() throws IOException {
        Store.MetadataSnapshot metadataSnapshot = randomBoolean()
            ? Store.MetadataSnapshot.EMPTY
            : new Store.MetadataSnapshot(
                Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()),
                randomIntBetween(0, 100)
            );
        return new StartRecoveryRequest(
            shardId,
            null,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong()
        );
    }

    private Store newStore(Path path, Path havenaskPath) throws IOException {
        return newStore(path, havenaskPath, false);
    }

    private Store newStore(Path path, Path havenaskPath, boolean checkIndex) throws IOException {
        BaseDirectoryWrapper baseDirectoryWrapper = newFSDirectory(path);
        if (checkIndex == false) {
            baseDirectoryWrapper.setCheckIndexOnClose(false); // don't run checkindex we might corrupt the index in these tests
        }
        return new HavenaskStore(shardId, INDEX_SETTINGS, baseDirectoryWrapper, new DummyShardLock(shardId), OnClose.EMPTY, havenaskPath);
    }

    class TestRecoveryTargetHandler implements RecoveryTargetHandler {
        @Override
        public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {}

        @Override
        public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {}

        @Override
        public void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext) {}

        @Override
        public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long timestamp,
            final long msu,
            final RetentionLeases retentionLeases,
            final long mappingVersion,
            final ActionListener<Long> listener
        ) {}

        @Override
        public void receiveFileInfo(
            List<String> phase1FileNames,
            List<Long> phase1FileSizes,
            List<String> phase1ExistingFileNames,
            List<Long> phase1ExistingFileSizes,
            int totalTranslogOps,
            ActionListener<Void> listener
        ) {

        }

        @Override
        public void cleanFiles(
            int totalTranslogOps,
            long globalCheckpoint,
            Store.MetadataSnapshot sourceMetadata,
            ActionListener<Void> listener
        ) {}

        @Override
        public void writeFileChunk(
            StoreFileMetadata fileMetadata,
            long position,
            BytesReference content,
            boolean lastChunk,
            int totalTranslogOps,
            ActionListener<Void> listener
        ) {}
    }
}
