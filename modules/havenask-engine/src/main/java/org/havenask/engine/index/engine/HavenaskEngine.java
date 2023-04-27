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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.havenask.HavenaskException;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.index.config.generator.BizConfigGenerator;
import org.havenask.engine.index.config.generator.RuntimeSegmentGenerator;
import org.havenask.engine.index.config.generator.TableConfigGenerator;
import org.havenask.engine.rpc.HavenaskClient;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.engine.InternalEngine;
import org.havenask.index.mapper.IdFieldMapper;
import org.havenask.index.mapper.ParseContext;
import org.havenask.index.mapper.ParsedDocument;
import org.havenask.index.mapper.SourceFieldMapper;
import org.havenask.index.mapper.Uid;
import org.havenask.index.shard.ShardId;

public class HavenaskEngine extends InternalEngine {

    private final HavenaskClient havenaskClient;
    private final HavenaskEngineEnvironment env;
    private final NativeProcessControlService nativeProcessControlService;
    private final ShardId shardId;
    private final boolean realTimeEnable;
    private final KafkaProducer<String, String> producer;
    private final String kafkaTopic;
    private final int kafkaPartition;

    public HavenaskEngine(
        EngineConfig engineConfig,
        HavenaskClient havenaskClient,
        HavenaskEngineEnvironment env,
        NativeProcessControlService nativeProcessControlService
    ) {
        super(engineConfig);
        this.havenaskClient = havenaskClient;
        this.env = env;
        this.nativeProcessControlService = nativeProcessControlService;
        this.shardId = engineConfig.getShardId();
        this.realTimeEnable = EngineSettings.HAVENASK_REALTIME_ENABLE.get(engineConfig.getIndexSettings().getSettings());
        this.producer = realTimeEnable ? initKafkaProducer(engineConfig.getIndexSettings().getSettings()) : null;
        this.kafkaTopic = realTimeEnable
            ? EngineSettings.HAVENASK_REALTIME_TOPIC_NAME.get(engineConfig.getIndexSettings().getSettings())
            : null;
        this.kafkaPartition = realTimeEnable ? getKafkaPartition(engineConfig.getIndexSettings().getSettings(), kafkaTopic) : -1;

        // 加载配置表
        try {
            activeTable();
        } catch (IOException e) {
            // TODO
            logger.error(() -> new ParameterizedMessage("shard [{}] activeTable exception", engineConfig.getShardId()), e);
            throw new HavenaskException("activeTable exception", e);
        }
    }

    private KafkaProducer<String, String> initKafkaProducer(Settings settings) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EngineSettings.HAVENASK_REALTIME_BOOTSTRAP_SERVERS.get(settings));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     *  获取kafka topic partition数量
     * @param settings settings
     * @param kafkaTopic kafkaTopic name
     * @return
     */
    private int getKafkaPartition(Settings settings, String kafkaTopic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EngineSettings.HAVENASK_REALTIME_BOOTSTRAP_SERVERS.get(settings));
        AdminClient adminClient = KafkaAdminClient.create(props);

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

    /**
     * TODO 如何像es一样,解决在关闭engine时,不影响正在进行的查询请求
     * TODO 解决关闭节点时,会对每个shard执行inactiveTable操作的问题
     */
    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        super.closeNoLock(reason, closedLatch);
        try {
            inactiveTable();
        } catch (IOException e) {
            // TODO
            logger.error(() -> new ParameterizedMessage("shard [{}] inactiveTable exception", engineConfig.getShardId()), e);
        }
    }

    /**
     * 加载数据表
     * TODO 注意加锁,防止并发更新冲突
     *
     * @throws IOException
     */
    private void activeTable() throws IOException {
        BizConfigGenerator.generateBiz(engineConfig, env.getConfigPath());
        TableConfigGenerator.generateTable(engineConfig, env.getConfigPath());
        // 初始化segment信息
        RuntimeSegmentGenerator.generateRuntimeSegment(
            env,
            nativeProcessControlService,
            shardId.getIndexName(),
            engineConfig.getIndexSettings().getSettings()
        );
        // 更新配置表信息
        nativeProcessControlService.updateDataNodeTarget();
        nativeProcessControlService.updateIngestNodeTarget();
    }

    /**
     * 卸载数据表
     *
     * @throws IOException
     */
    private synchronized void inactiveTable() throws IOException {
        BizConfigGenerator.removeBiz(engineConfig, env.getConfigPath());
        TableConfigGenerator.removeTable(engineConfig, env.getConfigPath());
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
                String src = binaryVal.utf8ToString();
                haDoc.put(field.name(), src);
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
        StringBuffer message = new StringBuffer();
        switch (type) {
            case INDEX:
                message.append("CMD=add\\x1F\\n");
                break;
            case DELETE:
                message.append("CMD=delete\\x1F\\n");
                break;
            default:
                throw new IllegalArgumentException("invalid operation type!");
        }

        for (Map.Entry<String, String> entry : haDoc.entrySet()) {
            message.append(entry.getKey()).append("=").append(entry.getValue()).append("\\x1F\\n");
        }
        message.append("\\x1E\\n");
        long hashId = HashAlgorithm.getHashId(id);
        long partition = HashAlgorithm.getPartitionId(hashId, topicPartition);

        return new ProducerRecord<>(topicName, (int) partition, id, message.toString());
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        if (false == realTimeEnable) {
            throw new HavenaskException("havenask realtime is not enable! not support index operation!");
        }

        Map<String, String> haDoc = toHaIndex(index.parsedDoc());
        ProducerRecord<String, String> record = buildProducerRecord(index.id(), index.operationType(), kafkaTopic, kafkaPartition, haDoc);
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new HavenaskException("havenask realtime index exception", e);
        }
        return new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), true);
    }

    @Override
    public DeleteResult delete(Delete delete) {
        if (false == realTimeEnable) {
            throw new HavenaskException("havenask realtime is not enable! not support delete operation!");
        }

        Map<String, String> haDoc = new HashMap<>();
        haDoc.put(IdFieldMapper.NAME, delete.id());
        ProducerRecord<String, String> record = buildProducerRecord(delete.id(), delete.operationType(), kafkaTopic, kafkaPartition, haDoc);
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new HavenaskException("havenask realtime delete exception", e);
        }
        return new DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
    }
}
