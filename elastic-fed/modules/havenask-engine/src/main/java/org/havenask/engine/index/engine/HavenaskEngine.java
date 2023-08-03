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

package org.havenask.engine.index.engine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import javax.management.MBeanTrustPermission;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.havenask.HavenaskException;
import org.havenask.action.bulk.BackoffPolicy;
import org.havenask.common.Nullable;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.index.config.generator.RuntimeSegmentGenerator;
import org.havenask.engine.index.mapper.VectorField;
import org.havenask.engine.rpc.HavenaskClient;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.engine.rpc.SqlClientInfoResponse;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.rpc.WriteRequest;
import org.havenask.engine.rpc.WriteResponse;
import org.havenask.engine.util.Utils;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.engine.EngineException;
import org.havenask.index.engine.InternalEngine;
import org.havenask.index.engine.TranslogLeafReader;
import org.havenask.index.mapper.IdFieldMapper;
import org.havenask.index.mapper.ParseContext;
import org.havenask.index.mapper.ParsedDocument;
import org.havenask.index.mapper.SourceFieldMapper;
import org.havenask.index.mapper.Uid;
import org.havenask.index.seqno.SequenceNumbers;
import org.havenask.index.shard.ShardId;
import org.havenask.index.translog.Translog;
import org.havenask.index.translog.TranslogConfig;
import org.havenask.index.translog.TranslogDeletionPolicy;
import suez.service.proto.ErrorCode;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class HavenaskEngine extends InternalEngine {

    private final HavenaskClient searcherHttpClient;
    private final QrsClient qrsHttpClient;
    private final SearcherClient searcherClient;
    private final HavenaskEngineEnvironment env;
    private final NativeProcessControlService nativeProcessControlService;
    private final ShardId shardId;
    private final boolean realTimeEnable;
    private final String kafkaTopic;
    private int kafkaPartition;
    private KafkaProducer<String, String> producer = null;
    private volatile HavenaskCommitInfo lastCommitInfo = null;
    private CheckpointCalc checkpointCalc = null;

    public HavenaskEngine(
        EngineConfig engineConfig,
        HavenaskClient searcherHttpClient,
        QrsClient qrsHttpClient,
        SearcherClient searcherClient,
        HavenaskEngineEnvironment env,
        NativeProcessControlService nativeProcessControlService
    ) {
        super(engineConfig);

        this.searcherHttpClient = searcherHttpClient;
        this.qrsHttpClient = qrsHttpClient;
        this.searcherClient = searcherClient;
        this.env = env;
        this.nativeProcessControlService = nativeProcessControlService;
        this.shardId = engineConfig.getShardId();
        this.realTimeEnable = EngineSettings.HAVENASK_REALTIME_ENABLE.get(engineConfig.getIndexSettings().getSettings());
        this.kafkaTopic = realTimeEnable
            ? EngineSettings.HAVENASK_REALTIME_TOPIC_NAME.get(engineConfig.getIndexSettings().getSettings())
            : null;
        try {
            this.producer = realTimeEnable ? initKafkaProducer(engineConfig.getIndexSettings().getSettings()) : null;
            this.kafkaPartition = realTimeEnable ? getKafkaPartition(engineConfig.getIndexSettings().getSettings(), kafkaTopic) : -1;
        } catch (Exception e) {
            if (realTimeEnable && producer != null) {
                producer.close();
            }
            failEngine("init kafka producer failed", e);
            throw new EngineException(shardId, "init kafka producer failed", e);
        }

        long commitTimestamp = getLastCommittedSegmentInfos().userData.containsKey(HavenaskCommitInfo.COMMIT_TIMESTAMP_KEY)
            ? Long.valueOf(getLastCommittedSegmentInfos().userData.get(HavenaskCommitInfo.COMMIT_TIMESTAMP_KEY))
            : -1L;
        long commitVersion = getLastCommittedSegmentInfos().userData.containsKey(HavenaskCommitInfo.COMMIT_VERSION_KEY)
            ? Long.valueOf(getLastCommittedSegmentInfos().userData.get(HavenaskCommitInfo.COMMIT_VERSION_KEY))
            : -1L;
        long commitCheckpoint = getLastCommittedSegmentInfos().userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY)
            ? Long.valueOf(getLastCommittedSegmentInfos().userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))
            : -1L;
        this.lastCommitInfo = new HavenaskCommitInfo(commitTimestamp, commitVersion, commitCheckpoint);
        this.checkpointCalc = new CheckpointCalc();
        if (commitTimestamp >= 0 && commitCheckpoint >= 0) {
            this.checkpointCalc.addCheckpoint(commitTimestamp, commitCheckpoint);
        }
        logger.info(
            "havenask engine init, shardId: {}, commitTimestamp: {}, commitVersion: {}, commitCheckpoint: {}",
            shardId,
            commitTimestamp,
            commitVersion,
            commitCheckpoint
        );

        // 加载配置表
        try {
            activeTable();
            checkTableStatus();
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("shard [{}] activeTable exception", engineConfig.getShardId()), e);
            failEngine("active havenask table failed", e);
            throw new EngineException(shardId, "active havenask table failed", e);
        }

        nativeProcessControlService.addHavenaskEngine(this);
    }

    static KafkaProducer<String, String> initKafkaProducer(Settings settings) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EngineSettings.HAVENASK_REALTIME_BOOTSTRAP_SERVERS.get(settings));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Thread.currentThread().setContextClassLoader(null);
        return AccessController.doPrivileged(
            (PrivilegedAction<KafkaProducer<String, String>>) () -> { return new KafkaProducer<>(props); },
            AccessController.getContext(),
            new MBeanTrustPermission("register")
        );
    }

    @Override
    public void close() throws IOException {
        super.close();
        logger.info("[{}] close havenask engine", shardId);
        if (realTimeEnable && producer != null) {
            producer.close();
        }

        nativeProcessControlService.removeHavenaskEngine(this);
    }

    /**
     * 获取kafka topic partition数量
     *
     * @param settings   settings
     * @param kafkaTopic kafkaTopic name
     * @return partition数量
     */
    static int getKafkaPartition(Settings settings, String kafkaTopic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EngineSettings.HAVENASK_REALTIME_BOOTSTRAP_SERVERS.get(settings));
        try (
            AdminClient adminClient = AccessController.doPrivileged((PrivilegedAction<AdminClient>) () -> KafkaAdminClient.create(props))
        ) {
            DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(kafkaTopic));
            Map<String, TopicDescription> topicDescriptionMap = null;
            try {
                topicDescriptionMap = result.all().get();
            } catch (Exception e) {
                throw new HavenaskException("get kafka partition exception", e);
            }
            TopicDescription topicDescription = topicDescriptionMap.get(kafkaTopic);

            return topicDescription.partitions().size();
        }
    }

    /**
     * 加载数据表
     * TODO 注意加锁,防止并发更新冲突
     *
     * @throws IOException TODO
     */
    private void activeTable() throws IOException {
        // 初始化segment信息
        RuntimeSegmentGenerator.generateRuntimeSegment(
            env,
            nativeProcessControlService,
            shardId.getIndexName(),
            engineConfig.getIndexSettings().getSettings()
        );
        // 更新配置表信息
        nativeProcessControlService.updateDataNodeTargetAsync();
        nativeProcessControlService.updateIngestNodeTargetAsync();
    }

    private void checkTableStatus() throws IOException {
        long timeout = 600000;
        while (timeout > 0) {
            try {
                Thread.sleep(5000);
                HeartbeatTargetResponse heartbeatTargetResponse = searcherHttpClient.getHeartbeatTarget();
                if (heartbeatTargetResponse.getSignature() == null) {
                    throw new IOException("havenask get heartbeat target failed");
                }
                TargetInfo targetInfo = heartbeatTargetResponse.getSignature();
                if (false == targetInfo.table_info.containsKey(shardId.getIndexName())) {
                    throw new IOException("havenask table not found in searcher");
                }

                SqlClientInfoResponse sqlClientInfoResponse = qrsHttpClient.executeSqlClientInfo();
                if (sqlClientInfoResponse.getErrorCode() != 0) {
                    throw new IOException("havenask execute sql client info failed");
                }

                if (sqlClientInfoResponse.getResult().getJSONObject("default").getJSONObject("general").getJSONObject("tables").containsKey(shardId.getIndexName())) {
                    throw new IOException("havenask table not found in qrs");
                }
                return;
            } catch (Exception e) {
                logger.info(() -> new ParameterizedMessage("shard [{}] checkTableStatus exception", engineConfig.getShardId()), e);
                timeout -= 5000;
            }
        }

        if (timeout <= 0) {
            throw new IOException("shard [" + engineConfig.getShardId() + "] check havenask table status timeout");
        }
    }

    /**
     * convert lucene fields to indexlib fields.
     */
    static Map<String, String> toHaIndex(ParsedDocument parsedDocument) throws IOException {
        Map<String, String> haDoc = new HashMap<>();
        haDoc.put("_id", parsedDocument.id());
        if (parsedDocument.routing() != null) {
            haDoc.put("_routing", parsedDocument.routing());
        }
        if (parsedDocument.rootDoc() == null) {
            return haDoc;
        }
        ParseContext.Document rootDoc = parsedDocument.rootDoc();
        for (IndexableField field : rootDoc.getFields()) {
            String fieldName = field.name();
            if (haDoc.containsKey(fieldName) || fieldName.contains(".")) {
                continue;
            }

            // for string or number
            String stringVal = field.stringValue();
            if (Objects.isNull(stringVal)) {
                stringVal = Optional.ofNullable(field.numericValue()).map(Number::toString).orElse(null);
            }

            if (Objects.nonNull(stringVal)) {
                haDoc.put(field.name(), stringVal);
                continue;
            }

            BytesRef binaryVal = field.binaryValue();
            if (binaryVal == null) {
                throw new IOException("invalid field value!");
            }
            if (field.name().equals(IdFieldMapper.NAME)) {
                haDoc.put(field.name(), Uid.decodeId(binaryVal.bytes));
            } else if (field.name().equals(SourceFieldMapper.NAME)) {
                BytesReference bytes = new BytesArray(binaryVal);
                String src = XContentHelper.convertToJson(bytes, false, parsedDocument.getXContentType());
                haDoc.put(field.name(), src);
            } else if (field instanceof VectorField) {
                VectorField vectorField = (VectorField) field;
                float[] array = (float[]) VectorField.readValue(vectorField.binaryValue().bytes);
                int iMax = array.length - 1;
                StringBuilder b = new StringBuilder();
                for (int i = 0;; i++) {
                    b.append(array[i]);
                    if (i == iMax) {
                        break;
                    }
                    b.append(",");
                }
                haDoc.put(field.name(), b.toString());
            } else { // TODO other special fields support.
                haDoc.put(field.name(), binaryVal.utf8ToString());
            }
        }

        return haDoc;
    }

    /**
     * build producer record
     */
    static ProducerRecord<String, String> buildProducerRecord(
        String id,
        Operation.TYPE type,
        String topicName,
        int topicPartition,
        Map<String, String> haDoc
    ) {
        StringBuilder message = new StringBuilder();
        switch (type) {
            case INDEX:
                message.append("CMD=add\u001F\n");
                break;
            case DELETE:
                message.append("CMD=delete\u001F\n");
                break;
            default:
                throw new IllegalArgumentException("invalid operation type!");
        }

        for (Map.Entry<String, String> entry : haDoc.entrySet()) {
            message.append(entry.getKey()).append("=").append(entry.getValue()).append("\u001F\n");
        }
        message.append("\u001E\n");
        long hashId = HashAlgorithm.getHashId(id);
        long partition = HashAlgorithm.getPartitionId(hashId, topicPartition);

        return new ProducerRecord<>(topicName, (int) partition, id, message.toString());
    }

    static WriteRequest buildWriteRequest(String table, String id, Operation.TYPE type, Map<String, String> haDoc) {
        StringBuilder message = new StringBuilder();
        switch (type) {
            case INDEX:
                message.append("CMD=add\u001F\n");
                break;
            case DELETE:
                message.append("CMD=delete\u001F\n");
                break;
            default:
                throw new IllegalArgumentException("invalid operation type!");
        }

        for (Map.Entry<String, String> entry : haDoc.entrySet()) {
            message.append(entry.getKey()).append("=").append(entry.getValue()).append("\u001F\n");
        }
        message.append("\u001E\n");
        long hashId = HashAlgorithm.getHashId(id);
        return new WriteRequest(table, (int) hashId, message.toString());
    }

    @Override
    protected IndexResult indexIntoLucene(Index index, IndexingStrategy plan) throws IOException {
        index.parsedDoc().updateSeqID(index.seqNo(), index.primaryTerm());
        index.parsedDoc().version().setLongValue(plan.versionForIndexing);
        Map<String, String> haDoc = toHaIndex(index.parsedDoc());
        if (realTimeEnable) {
            ProducerRecord<String, String> record = buildProducerRecord(
                index.id(),
                index.operationType(),
                kafkaTopic,
                kafkaPartition,
                haDoc
            );
            try {
                producer.send(record).get();
            } catch (Exception e) {
                throw new HavenaskException("havenask realtime index exception", e);
            }
            return new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), true);
        } else {
            try {
                WriteRequest writeRequest = buildWriteRequest(shardId.getIndexName(), index.id(), index.operationType(), haDoc);
                WriteResponse writeResponse = retryWrite(shardId, searcherClient, writeRequest);
                if (writeResponse.getErrorCode() != null) {
                    throw new IOException(
                        "havenask index exception, error code: "
                            + writeResponse.getErrorCode()
                            + ", error message:"
                            + writeResponse.getErrorMessage()
                    );
                }
                return new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), true);
            } catch (IOException e) {
                logger.warn("havenask index exception", e);
                failEngine(e.getMessage(), e);
                throw e;
            }
        }
    }

    @Override
    protected DeleteResult deleteInLucene(Delete delete, DeletionStrategy plan) throws IOException {
        Map<String, String> haDoc = new HashMap<>();
        haDoc.put(IdFieldMapper.NAME, delete.id());
        if (realTimeEnable) {
            ProducerRecord<String, String> record = buildProducerRecord(
                delete.id(),
                delete.operationType(),
                kafkaTopic,
                kafkaPartition,
                haDoc
            );
            try {
                producer.send(record).get();
            } catch (Exception e) {
                throw new HavenaskException("havenask realtime delete exception", e);
            }
            return new DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
        } else {
            try {
                WriteRequest writeRequest = buildWriteRequest(shardId.getIndexName(), delete.id(), delete.operationType(), haDoc);
                WriteResponse writeResponse = retryWrite(shardId, searcherClient, writeRequest);
                if (writeResponse.getErrorCode() != null) {
                    throw new IOException(
                        "havenask delete exception, error code: "
                            + writeResponse.getErrorCode()
                            + ", error message:"
                            + writeResponse.getErrorMessage()
                    );
                }
                return new DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
            } catch (IOException e) {
                logger.warn("havenask delete exception", e);
                failEngine(e.getMessage(), e);
                throw e;
            }
        }
    }

    static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMillis(50);
    static final int MAX_RETRY = 6;
    private static final Logger LOGGER = LogManager.getLogger(HavenaskEngine.class);

    static WriteResponse retryWrite(ShardId shardId, SearcherClient searcherClient, WriteRequest writeRequest) {
        WriteResponse writeResponse = searcherClient.write(writeRequest);
        if (isWriteRetry(writeResponse)) {
            long start = System.currentTimeMillis();
            // retry if write queue is full or write response is null
            Iterator<TimeValue> backoff = BackoffPolicy.exponentialBackoff(DEFAULT_TIMEOUT, MAX_RETRY).iterator();
            int retryCount = 0;
            while (backoff.hasNext()) {
                TimeValue timeValue = backoff.next();
                try {
                    Thread.sleep(timeValue.millis());
                } catch (InterruptedException e) {
                    // pass
                }
                writeResponse = searcherClient.write(writeRequest);
                retryCount++;
                if (false == isWriteRetry(writeResponse)) {
                    break;
                }
            }
            long cost = System.currentTimeMillis() - start;
            LOGGER.info("[{}] havenask write retry, retry count: {}, cost: {}ms", shardId, retryCount, cost);
        }

        return writeResponse;
    }

    private static boolean isWriteRetry(WriteResponse writeResponse) {
        if ((writeResponse.getErrorCode() == ErrorCode.TBS_ERROR_UNKOWN
            && writeResponse.getErrorMessage().contains("write response is null"))
            || (writeResponse.getErrorCode() == ErrorCode.TBS_ERROR_OTHERS
                && writeResponse.getErrorMessage().contains("doc queue is full"))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * not support
     */
    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        try {
            String sql = String.format(
                Locale.ROOT,
                "select _routing,_seq_no,_primary_term,_version,_source from %s_summary_ where _id='%s'",
                shardId.getIndexName(),
                get.id()
            );
            String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;
            QrsSqlRequest request = new QrsSqlRequest(sql, kvpair);
            QrsSqlResponse response = qrsHttpClient.executeSql(request);
            JSONObject jsonObject = JSON.parseObject(response.getResult());
            JSONObject sqlResult = jsonObject.getJSONObject("sql_result");
            JSONArray datas = sqlResult.getJSONArray("data");
            if (datas.size() == 0) {
                return GetResult.NOT_EXISTS;
            }

            assert datas.size() == 1;
            JSONArray row = datas.getJSONArray(0);
            assert row.size() == 5;
            String routing = row.getString(0);
            routing = routing == null || routing.isEmpty() ? null : routing;
            long seqNo = row.getLongValue(1);
            long primaryTerm = row.getLongValue(2);
            long version = row.getLongValue(3);
            String source = row.getString(4);

            Translog.Index operation = new Translog.Index(
                get.type(),
                get.id(),
                seqNo,
                primaryTerm,
                version,
                source.getBytes(StandardCharsets.UTF_8),
                routing,
                -1L
            );
            TranslogLeafReader reader = new TranslogLeafReader(operation);
            DocIdAndVersion docIdAndVersion = new DocIdAndVersion(0, version, seqNo, primaryTerm, reader, 0);
            return new GetResult(null, docIdAndVersion, false);
        } catch (Exception e) {
            throw new EngineException(shardId, e.getMessage());
        }
    }

    /**
     * do nothing
     */
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        @Nullable String forceMergeUUID
    ) throws EngineException {
        throw new UnsupportedOperationException("havenask engine not support force merge operation");
    }

    @Override
    protected Translog newTranslog(
        TranslogConfig translogConfig,
        String translogUUID,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer
    ) throws IOException {
        LongSupplier checkpointSupplier = () -> lastCommitInfo != null ? lastCommitInfo.getCommitCheckpoint() : -1L;
        return new Translog(
            translogConfig,
            translogUUID,
            translogDeletionPolicy,
            checkpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer
        );
    }

    @Override
    public void refresh(String source) throws EngineException {
        maybeRefresh(source);
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        long time = System.currentTimeMillis();
        long checkpoint = getPersistedLocalCheckpoint();
        checkpointCalc.addCheckpoint(time, checkpoint);

        Long havenaskTime = Utils.getIndexCheckpoint(env.getRuntimedataPath().resolve(shardId.getIndexName()));
        long havenaskTimePoint;

        if (havenaskTime != null) {
            havenaskTimePoint = havenaskTime / 1000;
        } else {
            havenaskTimePoint = -1;
        }

        long currentCheckpoint = checkpointCalc.getCheckpoint(havenaskTimePoint);

        logger.debug(
            "havenask engine maybeRefresh, source: {}, time: {}, checkpoint: {}, havenask time point: {}, current checkpoint: {}",
            source,
            time,
            checkpoint,
            havenaskTimePoint,
            currentCheckpoint
        );

        if (currentCheckpoint > lastCommitInfo.getCommitCheckpoint()) {
            logger.info(
                "havenask engine refresh checkpoint, checkpoint time: {}, current checkpoint: {}, last commit " + "checkpoint: {}",
                havenaskTime,
                currentCheckpoint,
                lastCommitInfo.getCommitCheckpoint()
            );
            refreshCommitInfo(havenaskTimePoint, 0, currentCheckpoint);
            return true;
        }
        return false;
    }

    @Override
    public boolean refreshNeeded() {
        return true;
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        throw new UnsupportedOperationException("havenask engine not support sync flush operation");
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        // havenask不能手动触发flush,只能等待commit信息发生变化,再触发flush
        return hasNewCommitInfo();
    }

    @Override
    protected boolean hasTriggerFlush(boolean force) {
        if (force || hasNewCommitInfo()) {
            logger.info("has new commit info, need to flush, force: {}", force);
            return true;
        } else {
            return false;
        }
    }

    /**
     * 刷新commit信息
     * @param commitTimestamp  commit timestamp
     * @param commitVersion  commit version
     */
    private void refreshCommitInfo(long commitTimestamp, long commitVersion, long localCheckpoint) {
        lastCommitInfo = new HavenaskCommitInfo(commitTimestamp, commitVersion, localCheckpoint);
    }

    /**
     * 判断commit信息是否发生变化
     */
    public boolean hasNewCommitInfo() {
        if (lastCommitInfo == null || lastCommitInfo.getCommitTimestamp() <= 0) {
            return false;
        }

        long lastCommitTimestamp = getLastCommittedSegmentInfos().userData.containsKey(HavenaskCommitInfo.COMMIT_TIMESTAMP_KEY)
            ? Long.valueOf(getLastCommittedSegmentInfos().userData.get(HavenaskCommitInfo.COMMIT_TIMESTAMP_KEY))
            : -1L;
        if (lastCommitInfo.getCommitTimestamp() > lastCommitTimestamp) {
            logger.info(
                "commit info changed, last commit timestamp: {}, new commit timestamp: {}",
                lastCommitInfo.getCommitTimestamp(),
                lastCommitTimestamp
            );
            return true;
        } else {
            return false;
        }
    }

    /**
     * 返回记录在lucene commit信息的checkpint
     * @return the local checkpoint that has been persisted to disk
     */
    public long getCommitLocalCheckpoint() {
        return lastCommitInfo.getCommitCheckpoint();
    }

    /**
     * add custom commit data to the commit data map
     * @param commitData the commit data
     */
    @Override
    protected void addCustomCommitData(Map<String, String> commitData) {
        commitData.put(HavenaskCommitInfo.COMMIT_TIMESTAMP_KEY, Long.toString(lastCommitInfo.getCommitTimestamp()));
        commitData.put(HavenaskCommitInfo.COMMIT_VERSION_KEY, Long.toString(lastCommitInfo.getCommitVersion()));
    }

    public static class HavenaskCommitInfo {
        public static final String COMMIT_TIMESTAMP_KEY = "commit_timestamp";
        public static final String COMMIT_VERSION_KEY = "commit_version";

        private final long commitTimestamp;
        private final long commitVersion;
        private final long commitCheckpoint;

        public HavenaskCommitInfo(long commitTimestamp, long commitVersion, long commitCheckpoint) {
            this.commitTimestamp = commitTimestamp;
            this.commitVersion = commitVersion;
            this.commitCheckpoint = commitCheckpoint;
        }

        public long getCommitTimestamp() {
            return commitTimestamp;
        }

        public long getCommitVersion() {
            return commitVersion;
        }

        public long getCommitCheckpoint() {
            return commitCheckpoint;
        }
    }
}
