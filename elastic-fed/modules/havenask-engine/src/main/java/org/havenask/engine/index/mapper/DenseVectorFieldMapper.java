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

package org.havenask.engine.index.mapper;

import org.apache.lucene.search.Query;
import org.havenask.Version;
import org.havenask.common.Explicit;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.support.XContentMapValues;
import org.havenask.engine.index.config.Schema;
import org.havenask.index.mapper.DocumentMapperParser;
import org.havenask.index.mapper.FieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperParsingException;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.ParametrizedFieldMapper;
import org.havenask.index.mapper.ParseContext;
import org.havenask.index.mapper.TextSearchInfo;
import org.havenask.index.mapper.ValueFetcher;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class DenseVectorFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "vector";
    public static final String DIMS = "dims";
    public static final String DIMENSION = "dimension";
    public static final String SIMILARITY = "similarity";
    public static final String CATEGORY = "category";
    public static final String INDEX_OPTIONS = "index_options";

    // index_options params
    public static final String INDEX_OPTIONS_TYPE = "type";
    public static final String EMBEDDING_DELIMITER = "embedding_delimiter";
    public static final String MAJOR_ORDER = "major_order";
    public static final String IGNORE_INVALID_DOC = "ignore_invalid_doc";
    public static final String ENABLE_RECALL_REPORT = "enable_recall_report";
    public static final String IS_EMBEDDING_SAVED = "is_embedding_saved";
    public static final String MIN_SCAN_DOC_CNT = "min_scan_doc_cnt";
    public static final String LINEAR_BUILD_THRESHOLD = "linear_build_threshold";
    public static final String ENABLE_RT_BUILD = "enable_rt_build";
    public static final String RT_INDEX_PARAMS = "rt_index_params";
    public static final String BUILD_INDEX_PARAMS = "build_index_params";
    public static final String SEARCH_INDEX_PARAMS = "search_index_params";

    // index_options_schema_params
    public static final String DISTANCE_TYPE = "distance_type";
    public static final String BUILDER_NAME = "builder_name";
    public static final String SEARCHER_NAME = "searcher_name";

    // rt_index_params
    public static final String OSWG_STREAMER_SEGMENT_SIZE = "proxima.oswg.streamer.segment_size";
    public static final String OSWG_STREAMER_EFCONSTRUCTION = "proxima.oswg.streamer.efconstruction";

    // hnsw_build_index_params
    public static final String HNSW_BUILDER_MAX_NEIGHBOR_COUNT = "proxima.hnsw.builder.max_neighbor_count";
    public static final String HNSW_BUILDER_EFCONSTRUCTION = "proxima.hnsw.builder.efconstruction";
    public static final String HNSW_BUILDER_THREAD_COUNT = "proxima.hnsw.builder.thread_count";

    // linear_build_index_params
    public static final String LINEAR_BUILDER_COLUMN_MAJOR_ORDER = "proxima.linear.builder.column_major_order";

    // qc_build_index_params
    public static final String QC_BUILDER_TRAIN_SAMPLE_COUNT = "proxima.qc.builder.train_sample_count";
    public static final String QC_BUILDER_THREAD_COUNT = "proxima.qc.builder.thread_count";
    public static final String QC_BUILDER_CENTROID_COUNT = "proxima.qc.builder.centroid_count";
    public static final String QC_BUILDER_CLUSTER_AUTO_TUNING = "proxima.qc.builder.cluster_auto_tuning";
    public static final String QC_BUILDER_OPTIMIZER_CLASS = "proxima.qc.builder.optimizer_class";
    public static final String QC_BUILDER_OPTIMIZER_PARAMS = "proxima.qc.builder.optimizer_params";
    public static final String QC_BUILDER_QUANTIZER_CLASS = "proxima.qc.builder.quantizer_class";
    public static final String QC_BUILDER_QUANTIZE_BY_CENTROID = "proxima.qc.builder.quantize_by_centroid";
    public static final String QC_BUILDER_STORE_ORIGINAL_FEATURES = "proxima.qc.builder.store_original_features";
    public static final String QC_BUILDER_TRAIN_SAMPLE_RATIO = "proxima.qc.builder.train_sample_ratio";

    // hnsw_search_index_params
    public static final String HNSW_SEARCHER_EF = "proxima.hnsw.searcher.ef";

    // qc_search_index_params
    public static final String QC_SEARCHER_SCAN_RATIO = "proxima.qc.searcher.scan_ratio";
    public static final String QC_SEARCHER_OPTIMIZER_PARAMS = "proxima.qc.searcher.optimizer_params";
    public static final String QC_SEARCHER_BRUTE_FORCE_THRESHOLD = "proxima.qc.searcher.brute_force_threshold";

    private static final int DIM_MAX = 2048;

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper) in;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        protected final Parameter<Integer> dimension = new Parameter<>(DIMS, false, () -> -1, (n, c, o) -> {
            if (o == null) {
                throw new IllegalArgumentException("dims cannot be null");
            }
            int value;
            try {
                value = XContentMapValues.nodeIntegerValue(o);
            } catch (Exception exception) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Unable to parse [dims] from provided value [%s] for vector [%s]", o, name)
                );
            }
            if (value <= 0) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Dimension value must be greater than 0 for vector: %s", name)
                );
            }
            if (value > DIM_MAX) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Dimension value must be less than %d for vector: %s", DIM_MAX, name)
                );
            }
            return value;
        }, m -> toType(m).dims);

        private final Parameter<String> similarity = Parameter.stringParam(SIMILARITY, false, m -> toType(m).similarity.name(), "l2_norm");

        private final Parameter<String> category = Parameter.stringParam(CATEGORY, false, m -> toType(m).category, "");

        private final Parameter<IndexOptions> indexOptions = new Parameter<>(
            INDEX_OPTIONS,
            false,
            () -> HnswIndexOptions.parseIndexOptions("", Collections.emptyMap()),
            (n, c, o) -> o == null ? null : parseIndexOptions(n, o),
            m -> toType(m).indexOptions
        );

        protected final Parameter<Map<String, String>> meta = Parameter.metaParam();

        /**
         * Creates a new Builder with a field name
         *
         * @param name the name of the field
         */
        protected Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(dimension, similarity, category, indexOptions, meta);
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            DenseVectorFieldType fieldType = new DenseVectorFieldType(
                buildFullName(context),
                meta.getValue(),
                dimension.get(),
                Similarity.fromString(similarity.get()),
                category.get(),
                indexOptions.get()
            );
            return new DenseVectorFieldMapper(
                name,
                fieldType,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                new Explicit<>(false, false)
            );
        }
    }

    public enum Algorithm {
        HNSW("hnsw"),
        LINEAR("linear"),
        QC("qc");

        private final String value;

        Algorithm(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Algorithm fromString(String value) {
            for (Algorithm algorithm : Algorithm.values()) {
                if (algorithm.getValue().equalsIgnoreCase(value)) {
                    return algorithm;
                }
            }
            throw new IllegalArgumentException("No algorithm matches " + value);
        }
    }

    public enum Similarity {
        L2_NORM("l2_norm", "l2"),
        DOT_PRODUCT("dot_product", "ip");

        private final String value;
        private final String alias;

        Similarity(String value, String alias) {
            this.value = value;
            this.alias = alias;
        }

        public String getValue() {
            return value;
        }

        public String getAlias() {
            return alias;
        }

        public static Similarity fromString(String value) {
            for (Similarity similarity : Similarity.values()) {
                if (similarity.getValue().equalsIgnoreCase(value)) {
                    return similarity;
                }
            }
            throw new IllegalArgumentException("No similarity matches " + value);
        }
    }

    public enum MajorOrder {
        COL("col"),
        ROW("row");

        private final String value;

        MajorOrder(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static MajorOrder fromString(String value) {
            for (MajorOrder majorOrder : MajorOrder.values()) {
                if (majorOrder.getValue().equalsIgnoreCase(value)) {
                    return majorOrder;
                }
            }
            throw new IllegalArgumentException("No major order matches " + value);
        }
    }

    /**
     *     "embedding_delimiter": ",",
     *     "builder_name": "QcBuilder",
     *     "searcher_name": "QcSearcher",
     *     "search_index_params": "{\"proxima.qc.searcher.scan_ratio\":0.01}",
     *     "build_index_params": "{\"proxima.qc.builder.quantizer_class\":\"Int8QuantizerConverter\",
     *          \"proxima.qc.builder.quantize_by_centroid\":true,\"proxima.qc.builder.optimizer_class\":
     *          \"BruteForceBuilder\",\"proxima.qc.builder.thread_count\":10,\"proxima.qc.builder.optimizer_params\":
     *          {\"proxima.linear.builder.column_major_order\":true},\"proxima.qc.builder.store_original_features\":false,
     *          \"proxima.qc.builder.train_sample_count\":3000000,\"proxima.qc.builder.train_sample_ratio\":0.5}",
     *     "major_order": "col",
     *     "enable_rt_build": "true",
     *     "ignore_invalid_doc" : "true",
     *     "enable_recall_report": "true",
     *     "is_embedding_saved": "true",
     *     "min_scan_doc_cnt": "20000",
     *     "linear_build_threshold": "5000",
     */
    public static class IndexOptions implements ToXContent {
        public final Algorithm type;
        public final String embeddingDelimiter;
        public final MajorOrder majorOrder;
        public final Boolean ignoreInvalidDoc;
        public final Boolean enableRecallReport;
        public final Boolean isEmbeddingSaved;
        public final Integer minScanDocCnt;
        public final Integer linearBuildThreshold;
        public final Integer oswgStreamerSegmentSize;
        public final Integer oswgStreamerEfConstruction;

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            Object embeddingDelimiterNode = indexOptionsMap.remove(EMBEDDING_DELIMITER);
            String embeddingDelimiter = embeddingDelimiterNode != null ? XContentMapValues.nodeStringValue(embeddingDelimiterNode) : null;
            Object majorOrderNode = indexOptionsMap.remove(MAJOR_ORDER);
            MajorOrder majorOrder = majorOrderNode != null
                ? MajorOrder.fromString(XContentMapValues.nodeStringValue(majorOrderNode))
                : null;
            Object ignoreInvalidDocNode = indexOptionsMap.remove(IGNORE_INVALID_DOC);
            Boolean ignoreInvalidDoc = ignoreInvalidDocNode != null ? XContentMapValues.nodeBooleanValue(ignoreInvalidDocNode) : true;
            Object enableRecallReportNode = indexOptionsMap.remove(ENABLE_RECALL_REPORT);
            Boolean enableRecallReport = enableRecallReportNode != null ? XContentMapValues.nodeBooleanValue(enableRecallReportNode) : null;
            Object isEmbeddingSavedNode = indexOptionsMap.remove(IS_EMBEDDING_SAVED);
            Boolean isEmbeddingSaved = isEmbeddingSavedNode != null ? XContentMapValues.nodeBooleanValue(isEmbeddingSavedNode) : null;
            Object minScanDocCntNode = indexOptionsMap.remove(MIN_SCAN_DOC_CNT);
            Integer minScanDocCnt = minScanDocCntNode != null ? XContentMapValues.nodeIntegerValue(minScanDocCntNode) : null;
            Object linearBuildThresholdNode = indexOptionsMap.remove(LINEAR_BUILD_THRESHOLD);
            Integer linearBuildThreshold = linearBuildThresholdNode != null
                ? XContentMapValues.nodeIntegerValue(linearBuildThresholdNode)
                : null;
            Integer oswgStreamerSegmentSize = null;
            Integer oswgStreamerEfConstruction = null;
            Object rtIndexParamsNode = indexOptionsMap.remove(RT_INDEX_PARAMS);
            Map<String, ?> rtIndexParamsMap = rtIndexParamsNode != null
                ? XContentMapValues.nodeMapValue(rtIndexParamsNode, RT_INDEX_PARAMS)
                : null;
            if (rtIndexParamsMap != null) {
                Object oswgStreamerSegmentSizeNode = rtIndexParamsMap.remove(OSWG_STREAMER_SEGMENT_SIZE);
                oswgStreamerSegmentSize = oswgStreamerSegmentSizeNode != null
                    ? XContentMapValues.nodeIntegerValue(oswgStreamerSegmentSizeNode)
                    : null;
                Object oswgStreamerEfConstructionNode = rtIndexParamsMap.remove(OSWG_STREAMER_EFCONSTRUCTION);
                oswgStreamerEfConstruction = oswgStreamerEfConstructionNode != null
                    ? XContentMapValues.nodeIntegerValue(oswgStreamerEfConstructionNode)
                    : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, rtIndexParamsMap, Version.CURRENT);
            }

            // TODO valid value
            return new IndexOptions(
                embeddingDelimiter,
                majorOrder,
                ignoreInvalidDoc,
                enableRecallReport,
                isEmbeddingSaved,
                minScanDocCnt,
                linearBuildThreshold,
                oswgStreamerSegmentSize,
                oswgStreamerEfConstruction
            );
        }

        IndexOptions(
            String embeddingDelimiter,
            MajorOrder majorOrder,
            Boolean ignoreInvalidDoc,
            Boolean enableRecallReport,
            Boolean isEmbeddingSaved,
            Integer minScanDocCnt,
            Integer linearBuildThreshold,
            Integer oswgStreamerSegmentSize,
            Integer oswgStreamerEfConstruction
        ) {
            this.type = null;
            this.embeddingDelimiter = embeddingDelimiter;
            this.majorOrder = majorOrder;
            this.ignoreInvalidDoc = ignoreInvalidDoc;
            this.enableRecallReport = enableRecallReport;
            this.isEmbeddingSaved = isEmbeddingSaved;
            this.minScanDocCnt = minScanDocCnt;
            this.linearBuildThreshold = linearBuildThreshold;
            this.oswgStreamerSegmentSize = oswgStreamerSegmentSize;
            this.oswgStreamerEfConstruction = oswgStreamerEfConstruction;
        }

        IndexOptions(
            Algorithm type,
            String embeddingDelimiter,
            MajorOrder majorOrder,
            Boolean ignoreInvalidDoc,
            Boolean enableRecallReport,
            Boolean isEmbeddingSaved,
            Integer minScanDocCnt,
            Integer linearBuildThreshold,
            Integer oswgStreamerSegmentSize,
            Integer oswgStreamerEfConstruction
        ) {
            this.type = type;
            this.embeddingDelimiter = embeddingDelimiter;
            this.majorOrder = majorOrder;
            this.ignoreInvalidDoc = ignoreInvalidDoc;
            this.enableRecallReport = enableRecallReport;
            this.isEmbeddingSaved = isEmbeddingSaved;
            this.minScanDocCnt = minScanDocCnt;
            this.linearBuildThreshold = linearBuildThreshold;
            this.oswgStreamerSegmentSize = oswgStreamerSegmentSize;
            this.oswgStreamerEfConstruction = oswgStreamerEfConstruction;
        }

        IndexOptions(Algorithm type, IndexOptions other) {
            this.type = type;
            this.embeddingDelimiter = other.embeddingDelimiter;
            this.majorOrder = other.majorOrder;
            this.ignoreInvalidDoc = other.ignoreInvalidDoc;
            this.enableRecallReport = other.enableRecallReport;
            this.isEmbeddingSaved = other.isEmbeddingSaved;
            this.minScanDocCnt = other.minScanDocCnt;
            this.linearBuildThreshold = other.linearBuildThreshold;
            this.oswgStreamerSegmentSize = other.oswgStreamerSegmentSize;
            this.oswgStreamerEfConstruction = other.oswgStreamerEfConstruction;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_OPTIONS_TYPE, type.name());
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexOptions that = (IndexOptions) o;
            return Objects.equals(embeddingDelimiter, that.embeddingDelimiter)
                && Objects.equals(majorOrder, that.majorOrder)
                && Objects.equals(ignoreInvalidDoc, that.ignoreInvalidDoc)
                && Objects.equals(enableRecallReport, that.enableRecallReport)
                && Objects.equals(isEmbeddingSaved, that.isEmbeddingSaved)
                && Objects.equals(minScanDocCnt, that.minScanDocCnt)
                && Objects.equals(linearBuildThreshold, that.linearBuildThreshold)
                && Objects.equals(oswgStreamerSegmentSize, that.oswgStreamerSegmentSize)
                && Objects.equals(oswgStreamerEfConstruction, that.oswgStreamerEfConstruction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                embeddingDelimiter,
                majorOrder,
                ignoreInvalidDoc,
                enableRecallReport,
                isEmbeddingSaved,
                minScanDocCnt,
                linearBuildThreshold,
                oswgStreamerSegmentSize,
                oswgStreamerEfConstruction
            );
        }

        @Override
        public String toString() {
            return "IndexOptions{"
                + "type="
                + type
                + ", embeddingDelimiter='"
                + embeddingDelimiter
                + ", majorOrder='"
                + majorOrder
                + ", ignoreInvalidDoc="
                + ignoreInvalidDoc
                + ", enableRecallReport="
                + enableRecallReport
                + ", isEmbeddingSaved="
                + isEmbeddingSaved
                + ", minScanDocCnt="
                + minScanDocCnt
                + ", linearBuildThreshold="
                + linearBuildThreshold
                + ", oswgStreamerSegmentSize="
                + oswgStreamerSegmentSize
                + "oswgStreamerEfConstruction="
                + oswgStreamerEfConstruction
                + '}';
        }

        public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_OPTIONS_TYPE, type.name());
            if (embeddingDelimiter != null) {
                builder.field(EMBEDDING_DELIMITER, embeddingDelimiter);
            }
            if (majorOrder != null) {
                builder.field(MAJOR_ORDER, majorOrder.getValue());
            }
            if (ignoreInvalidDoc != null) {
                builder.field(IGNORE_INVALID_DOC, ignoreInvalidDoc);
            }
            if (enableRecallReport != null) {
                builder.field(ENABLE_RECALL_REPORT, enableRecallReport);
            }
            if (isEmbeddingSaved != null) {
                builder.field(IS_EMBEDDING_SAVED, isEmbeddingSaved);
            }
            if (minScanDocCnt != null) {
                builder.field(MIN_SCAN_DOC_CNT, minScanDocCnt);
            }
            if (linearBuildThreshold != null) {
                builder.field(LINEAR_BUILD_THRESHOLD, linearBuildThreshold);
            }
            if (oswgStreamerSegmentSize != null || oswgStreamerEfConstruction != null) {
                builder.startObject(RT_INDEX_PARAMS);
                {
                    if (oswgStreamerSegmentSize != null) {
                        builder.field(OSWG_STREAMER_SEGMENT_SIZE, oswgStreamerSegmentSize);
                    }
                    if (oswgStreamerEfConstruction != null) {
                        builder.field(OSWG_STREAMER_EFCONSTRUCTION, oswgStreamerEfConstruction);
                    }
                }
                builder.endObject();
            }
            return builder;
        }
    }

    /**
     * {
     * "proxima.linear.builder.column_major_order" : "false"
     * }
     */

    public static class LinearIndexOptions extends IndexOptions {
        public final String builderColumnMajorOrder;

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            IndexOptions baseIndexOptions = IndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
            Object buildIndexParamsNode = indexOptionsMap.remove(BUILD_INDEX_PARAMS);
            Map<String, ?> buildIndexParamsMap = buildIndexParamsNode != null
                ? XContentMapValues.nodeMapValue(buildIndexParamsNode, BUILD_INDEX_PARAMS)
                : null;

            String builderColumnMajorOrder = null;
            if (buildIndexParamsMap != null) {
                Object builderColumnMajorOrderNode = buildIndexParamsMap.remove(LINEAR_BUILDER_COLUMN_MAJOR_ORDER);
                builderColumnMajorOrder = builderColumnMajorOrderNode != null
                    ? XContentMapValues.nodeStringValue(builderColumnMajorOrderNode)
                    : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, buildIndexParamsMap, Version.CURRENT);
            }

            DocumentMapperParser.checkNoRemainingFields(fieldName, indexOptionsMap, Version.CURRENT);
            // TODO vaild value
            return new LinearIndexOptions(baseIndexOptions, builderColumnMajorOrder);
        }

        private LinearIndexOptions(IndexOptions baseIndexOptions, String builderColumnMajorOrder) {
            super(Algorithm.LINEAR, baseIndexOptions);
            this.builderColumnMajorOrder = builderColumnMajorOrder;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            XContentBuilder baseBuilder = toInnerXContent(builder, params);
            if (builderColumnMajorOrder != null) {
                baseBuilder.startObject(BUILD_INDEX_PARAMS);
                {
                    baseBuilder.field(LINEAR_BUILDER_COLUMN_MAJOR_ORDER, builderColumnMajorOrder);
                }
                baseBuilder.endObject();
            }

            // end innerXContent
            baseBuilder.endObject();
            return baseBuilder;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) {
                return false;
            }
            LinearIndexOptions that = (LinearIndexOptions) o;
            return Objects.equals(builderColumnMajorOrder, that.builderColumnMajorOrder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), builderColumnMajorOrder);
        }

        @Override
        public String toString() {
            return "LinearIndexOptions{" + super.toString() + "builderColumnMajorOrder='" + builderColumnMajorOrder + '\'' + '}';
        }
    }

    /**
     * {
     * "proxima.qc.builder.train_sample_count" : "0",
     * "proxima.qc.builder.thread_count" : "0",
     * "proxima.qc.builder.centroid_count" : "1000",
     * "proxima.qc.builder.cluster_auto_tuning" : "false",
     * "proxima.qc.builder.optimizer_class" : "HcBuilder",
     * "proxima.qc.builder.optimizer_params" : "-",
     * "proxima.qc.builder.quantizer_class" : "-",
     * "proxima.qc.builder.quantize_by_centroid" : "false",
     * "proxima.qc.builder.store_original_features" : "false",
     * "proxima.qc.builder.train_sample_count" : "0",
     * "proxima.qc.builder.train_sample_ratio" : "1",
     * "proxima.qc.searcher.scan_ratio" : "0.01",
     * "proxima.qc.searcher.optimizer_params" : "-",
     * "proxima.qc.searcher.brute_force_threshold" : "1000",
     * }
     */
    public static class QCIndexOptions extends IndexOptions {
        public final Integer builderTrainSampleCount;
        public final Integer builderThreadCount;
        public final String builderCentroidCount;
        public final Boolean builderClusterAutoTuning;
        public final String builderOptimizerClass;
        public final String builderOptimizerParams;
        public final String builderQuantizerClass;
        public final Boolean builderQuantizeByCentroid;
        public final Boolean builderStoreOriginalFeatures;
        public final Double builderTrainSampleRatio;
        public final Double searcherScanRatio;
        public final String searcherOptimizerParams;
        public final Integer searcherBruteForceThreshold;

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            IndexOptions baseIndexOptions = IndexOptions.parseIndexOptions(fieldName, indexOptionsMap);

            Integer builderTrainSampleCount = null;
            Integer builderThreadCount = null;
            String builderCentroidCount = null;
            Boolean builderClusterAutoTuning = null;
            String builderOptimizerClass = null;
            String builderOptimizerParams = null;
            String builderQuantizerClass = null;
            Boolean builderQuantizeByCentroid = null;
            Boolean builderStoreOriginalFeatures = null;
            Double builderTrainSampleRatio = null;

            Object buildIndexParamsNode = indexOptionsMap.remove(BUILD_INDEX_PARAMS);
            Map<String, ?> buildIndexParamsMap = buildIndexParamsNode != null
                ? XContentMapValues.nodeMapValue(buildIndexParamsNode, BUILD_INDEX_PARAMS)
                : null;
            if (buildIndexParamsMap != null) {
                Object builderTrainSampleCountNode = buildIndexParamsMap.remove(QC_BUILDER_TRAIN_SAMPLE_COUNT);
                builderTrainSampleCount = builderTrainSampleCountNode != null
                    ? XContentMapValues.nodeIntegerValue(builderTrainSampleCountNode)
                    : null;
                Object builderThreadCountNode = buildIndexParamsMap.remove(QC_BUILDER_THREAD_COUNT);
                builderThreadCount = builderThreadCountNode != null ? XContentMapValues.nodeIntegerValue(builderThreadCountNode) : null;
                Object builderCentroidCountNode = buildIndexParamsMap.remove(QC_BUILDER_CENTROID_COUNT);
                builderCentroidCount = builderCentroidCountNode != null
                    ? XContentMapValues.nodeStringValue(builderCentroidCountNode)
                    : null;
                Object builderClusterAutoTuningNode = buildIndexParamsMap.remove(QC_BUILDER_CLUSTER_AUTO_TUNING);
                builderClusterAutoTuning = builderClusterAutoTuningNode != null
                    ? XContentMapValues.nodeBooleanValue(builderClusterAutoTuningNode)
                    : null;
                Object builderOptimizerClassNode = buildIndexParamsMap.remove(QC_BUILDER_OPTIMIZER_CLASS);
                builderOptimizerClass = builderOptimizerClassNode != null
                    ? XContentMapValues.nodeStringValue(builderOptimizerClassNode)
                    : null;
                Object builderOptimizerParamsNode = buildIndexParamsMap.remove(QC_BUILDER_OPTIMIZER_PARAMS);
                builderOptimizerParams = builderOptimizerParamsNode != null
                    ? XContentMapValues.nodeStringValue(builderOptimizerParamsNode)
                    : null;
                Object builderQuantizerClassNode = buildIndexParamsMap.remove(QC_BUILDER_QUANTIZER_CLASS);
                builderQuantizerClass = builderQuantizerClassNode != null
                    ? XContentMapValues.nodeStringValue(builderQuantizerClassNode)
                    : null;
                Object builderQuantizeByCentroidNode = buildIndexParamsMap.remove(QC_BUILDER_QUANTIZE_BY_CENTROID);
                builderQuantizeByCentroid = builderQuantizeByCentroidNode != null
                    ? XContentMapValues.nodeBooleanValue(builderQuantizeByCentroidNode)
                    : null;
                Object builderStoreOriginalFeaturesNode = buildIndexParamsMap.remove(QC_BUILDER_STORE_ORIGINAL_FEATURES);
                builderStoreOriginalFeatures = builderStoreOriginalFeaturesNode != null
                    ? XContentMapValues.nodeBooleanValue(builderStoreOriginalFeaturesNode)
                    : null;
                Object builderTrainSampleRatioNode = buildIndexParamsMap.remove(QC_BUILDER_TRAIN_SAMPLE_RATIO);
                builderTrainSampleRatio = builderTrainSampleRatioNode != null
                    ? XContentMapValues.nodeDoubleValue(builderTrainSampleRatioNode)
                    : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, buildIndexParamsMap, Version.CURRENT);
            }

            Double searcherScanRatio = null;
            String searcherOptimizerParams = null;
            Integer searcherBruteForceThreshold = null;

            Object searcherParamsNode = indexOptionsMap.remove(SEARCH_INDEX_PARAMS);
            Map<String, ?> searcherParamsMap = searcherParamsNode != null
                ? XContentMapValues.nodeMapValue(searcherParamsNode, SEARCH_INDEX_PARAMS)
                : null;
            if (searcherParamsMap != null) {
                Object searcherScanRatioNode = searcherParamsMap.remove(QC_SEARCHER_SCAN_RATIO);
                searcherScanRatio = searcherScanRatioNode != null ? XContentMapValues.nodeDoubleValue(searcherScanRatioNode) : null;
                Object searcherOptimizerParamsNode = searcherParamsMap.remove(QC_SEARCHER_OPTIMIZER_PARAMS);
                searcherOptimizerParams = searcherOptimizerParamsNode != null
                    ? XContentMapValues.nodeStringValue(searcherOptimizerParamsNode)
                    : null;
                Object searcherBruteForceThresholdNode = searcherParamsMap.remove(QC_SEARCHER_BRUTE_FORCE_THRESHOLD);
                searcherBruteForceThreshold = searcherBruteForceThresholdNode != null
                    ? XContentMapValues.nodeIntegerValue(searcherBruteForceThresholdNode)
                    : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, searcherParamsMap, Version.CURRENT);
            }
            DocumentMapperParser.checkNoRemainingFields(fieldName, indexOptionsMap, Version.CURRENT);
            // TODO vaild value
            return new QCIndexOptions(
                baseIndexOptions,
                builderTrainSampleCount,
                builderThreadCount,
                builderCentroidCount,
                builderClusterAutoTuning,
                builderOptimizerClass,
                builderOptimizerParams,
                builderQuantizerClass,
                builderQuantizeByCentroid,
                builderStoreOriginalFeatures,
                builderTrainSampleRatio,
                searcherScanRatio,
                searcherOptimizerParams,
                searcherBruteForceThreshold
            );
        }

        private QCIndexOptions(
            IndexOptions baseIndexOptions,
            Integer builderTrainSampleCount,
            Integer builderThreadCount,
            String builderCentroidCount,
            Boolean builderClusterAutoTuning,
            String builderOptimizerClass,
            String builderOptimizerParams,
            String builderQuantizerClass,
            Boolean builderQuantizeByCentroid,
            Boolean builderStoreOriginalFeatures,
            Double builderTrainSampleRatio,
            Double searcherScanRatio,
            String searcherOptimizerParams,
            Integer searcherBruteForceThreshold
        ) {
            super(Algorithm.QC, baseIndexOptions);
            this.builderTrainSampleCount = builderTrainSampleCount;
            this.builderThreadCount = builderThreadCount;
            this.builderCentroidCount = builderCentroidCount;
            this.builderClusterAutoTuning = builderClusterAutoTuning;
            this.builderOptimizerClass = builderOptimizerClass;
            this.builderOptimizerParams = builderOptimizerParams;
            this.builderQuantizerClass = builderQuantizerClass;
            this.builderQuantizeByCentroid = builderQuantizeByCentroid;
            this.builderStoreOriginalFeatures = builderStoreOriginalFeatures;
            this.builderTrainSampleRatio = builderTrainSampleRatio;
            this.searcherScanRatio = searcherScanRatio;
            this.searcherOptimizerParams = searcherOptimizerParams;
            this.searcherBruteForceThreshold = searcherBruteForceThreshold;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            XContentBuilder baseBuilder = toInnerXContent(builder, params);

            baseBuilder.startObject(BUILD_INDEX_PARAMS);
            {
                if (builderTrainSampleCount != null) {
                    baseBuilder.field(QC_BUILDER_TRAIN_SAMPLE_COUNT, builderTrainSampleCount);
                }
                if (builderThreadCount != null) {
                    baseBuilder.field(QC_BUILDER_THREAD_COUNT, builderThreadCount);
                }
                if (builderCentroidCount != null) {
                    baseBuilder.field(QC_BUILDER_CENTROID_COUNT, builderCentroidCount);
                }
                if (builderClusterAutoTuning != null) {
                    baseBuilder.field(QC_BUILDER_CLUSTER_AUTO_TUNING, builderClusterAutoTuning);
                }
                if (builderOptimizerClass != null) {
                    baseBuilder.field(QC_BUILDER_OPTIMIZER_CLASS, builderOptimizerClass);
                }
                if (builderOptimizerParams != null) {
                    baseBuilder.field(QC_BUILDER_OPTIMIZER_PARAMS, builderOptimizerParams);
                }
                if (builderQuantizerClass != null) {
                    baseBuilder.field(QC_BUILDER_QUANTIZER_CLASS, builderQuantizerClass);
                }
                if (builderQuantizeByCentroid != null) {
                    baseBuilder.field(QC_BUILDER_QUANTIZE_BY_CENTROID, builderQuantizeByCentroid);
                }
                if (builderStoreOriginalFeatures != null) {
                    baseBuilder.field(QC_BUILDER_STORE_ORIGINAL_FEATURES, builderStoreOriginalFeatures);
                }
                if (builderTrainSampleRatio != null) {
                    baseBuilder.field(QC_BUILDER_TRAIN_SAMPLE_RATIO, builderTrainSampleRatio);
                }
            }
            baseBuilder.endObject();

            baseBuilder.startObject(SEARCH_INDEX_PARAMS);
            {
                if (searcherScanRatio != null) {
                    baseBuilder.field(QC_SEARCHER_SCAN_RATIO, searcherScanRatio);
                }
                if (searcherOptimizerParams != null) {
                    baseBuilder.field(QC_SEARCHER_OPTIMIZER_PARAMS, searcherOptimizerParams);
                }
                if (searcherBruteForceThreshold != null) {
                    baseBuilder.field(QC_SEARCHER_BRUTE_FORCE_THRESHOLD, searcherBruteForceThreshold);
                }
            }
            baseBuilder.endObject();

            // end innerXContent
            baseBuilder.endObject();
            return baseBuilder;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) {
                return false;
            }
            QCIndexOptions that = (QCIndexOptions) o;
            return Objects.equals(builderTrainSampleCount, that.builderTrainSampleCount)
                && Objects.equals(builderThreadCount, that.builderThreadCount)
                && Objects.equals(builderCentroidCount, that.builderCentroidCount)
                && Objects.equals(builderClusterAutoTuning, that.builderClusterAutoTuning)
                && Objects.equals(builderOptimizerClass, that.builderOptimizerClass)
                && Objects.equals(builderOptimizerParams, that.builderOptimizerParams)
                && Objects.equals(builderQuantizerClass, that.builderQuantizerClass)
                && Objects.equals(builderQuantizeByCentroid, that.builderQuantizeByCentroid)
                && Objects.equals(builderStoreOriginalFeatures, that.builderStoreOriginalFeatures)
                && Objects.equals(builderTrainSampleRatio, that.builderTrainSampleRatio)
                && Objects.equals(searcherScanRatio, that.searcherScanRatio)
                && Objects.equals(searcherOptimizerParams, that.searcherOptimizerParams)
                && Objects.equals(searcherBruteForceThreshold, that.searcherBruteForceThreshold);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                super.hashCode(),
                builderTrainSampleCount,
                builderThreadCount,
                builderCentroidCount,
                builderClusterAutoTuning,
                builderOptimizerClass,
                builderOptimizerParams,
                builderQuantizerClass,
                builderQuantizeByCentroid,
                builderStoreOriginalFeatures,
                builderTrainSampleRatio,
                searcherScanRatio,
                searcherOptimizerParams,
                searcherBruteForceThreshold
            );
        }

        @Override
        public String toString() {
            return "QcIndexOptions{"
                + super.toString()
                + ", builderTrainSampleCount="
                + builderTrainSampleCount
                + ", builderThreadCount="
                + builderThreadCount
                + ", builderCentroidCount="
                + builderCentroidCount
                + ", builderClusterAutoTuning="
                + builderClusterAutoTuning
                + ", builderOptimizerClass="
                + builderOptimizerClass
                + ", builderOptimizerParams="
                + builderOptimizerParams
                + ", builderQuantizerClass="
                + builderQuantizerClass
                + ", builderQuantizeByCentroid="
                + builderQuantizeByCentroid
                + ", builderStoreOriginalFeatures="
                + builderStoreOriginalFeatures
                + ", builderTrainSampleRatio="
                + builderTrainSampleRatio
                + ", searcherScanRatio="
                + searcherScanRatio
                + ", searcherOptimizerParams="
                + searcherOptimizerParams
                + ", searcherBruteForceThreshold="
                + searcherBruteForceThreshold
                + '}';
        }
    }

    /**
     * {
     * "proxima.hnsw.builder.max_neighbor_count" : "100",
     * "proxima.hnsw.builder.efconstruction" : "500",
     * "proxima.hnsw.builder.thread_count" : "0",
     * "proxima.hnsw.searcher.ef" : "500",
     * }
     */
    public static class HnswIndexOptions extends IndexOptions {
        public final Integer builderMaxNeighborCnt;
        public final Integer builderEfConstruction;
        public final Integer builderThreadCnt;
        public final Integer searcherEf;

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            IndexOptions baseIndexOptions = IndexOptions.parseIndexOptions(fieldName, indexOptionsMap);

            Integer builderMaxNeighborCnt = null;
            Integer builderEfConstruction = null;
            Integer builderThreadCnt = null;
            Object buildIndexParamsNode = indexOptionsMap.remove(BUILD_INDEX_PARAMS);
            Map<String, ?> buildIndexParamsMap = buildIndexParamsNode != null
                ? XContentMapValues.nodeMapValue(buildIndexParamsNode, BUILD_INDEX_PARAMS)
                : null;
            if (buildIndexParamsMap != null) {
                Object builderMaxNeighborCntNode = buildIndexParamsMap.remove(HNSW_BUILDER_MAX_NEIGHBOR_COUNT);
                builderMaxNeighborCnt = builderMaxNeighborCntNode != null
                    ? XContentMapValues.nodeIntegerValue(builderMaxNeighborCntNode)
                    : null;
                Object builderEfConstructionNode = buildIndexParamsMap.remove(HNSW_BUILDER_EFCONSTRUCTION);
                builderEfConstruction = builderEfConstructionNode != null
                    ? XContentMapValues.nodeIntegerValue(builderEfConstructionNode)
                    : null;
                Object builderThreadCntNode = buildIndexParamsMap.remove(HNSW_BUILDER_THREAD_COUNT);
                builderThreadCnt = builderThreadCntNode != null ? XContentMapValues.nodeIntegerValue(builderThreadCntNode) : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, buildIndexParamsMap, Version.CURRENT);
            }

            Integer searcherEf = null;
            Object searcherParamsNode = indexOptionsMap.remove(SEARCH_INDEX_PARAMS);
            Map<String, ?> searcherParamsMap = searcherParamsNode != null
                ? XContentMapValues.nodeMapValue(searcherParamsNode, SEARCH_INDEX_PARAMS)
                : null;
            if (searcherParamsMap != null) {
                Object searcherEfNode = searcherParamsMap.remove(HNSW_SEARCHER_EF);
                searcherEf = searcherEfNode != null ? XContentMapValues.nodeIntegerValue(searcherEfNode) : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, searcherParamsMap, Version.CURRENT);
            }

            DocumentMapperParser.checkNoRemainingFields(fieldName, indexOptionsMap, Version.CURRENT);

            // TODO vaild value
            return new HnswIndexOptions(baseIndexOptions, builderMaxNeighborCnt, builderEfConstruction, builderThreadCnt, searcherEf);
        }

        private HnswIndexOptions(
            IndexOptions baseIndexOptions,
            Integer builderMaxNeighborCnt,
            Integer builderEfConstruction,
            Integer builderThreadCnt,
            Integer searcherEf
        ) {
            super(Algorithm.HNSW, baseIndexOptions);
            this.builderMaxNeighborCnt = builderMaxNeighborCnt;
            this.builderEfConstruction = builderEfConstruction;
            this.builderThreadCnt = builderThreadCnt;
            this.searcherEf = searcherEf;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            XContentBuilder baseBuilder = toInnerXContent(builder, params);
            baseBuilder.startObject(BUILD_INDEX_PARAMS);
            {
                if (builderMaxNeighborCnt != null) {
                    baseBuilder.field(HNSW_BUILDER_MAX_NEIGHBOR_COUNT, builderMaxNeighborCnt);
                }
                if (builderEfConstruction != null) {
                    baseBuilder.field(HNSW_BUILDER_EFCONSTRUCTION, builderEfConstruction);
                }
                if (builderThreadCnt != null) {
                    baseBuilder.field(HNSW_BUILDER_THREAD_COUNT, builderThreadCnt);
                }
            }
            baseBuilder.endObject();
            baseBuilder.startObject(SEARCH_INDEX_PARAMS);
            {
                if (searcherEf != null) {
                    baseBuilder.field(HNSW_SEARCHER_EF, searcherEf);
                }
            }
            baseBuilder.endObject();

            // end innerXContent
            baseBuilder.endObject();
            return baseBuilder;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) {
                return false;
            }
            HnswIndexOptions that = (HnswIndexOptions) o;
            return Objects.equals(builderMaxNeighborCnt, that.builderMaxNeighborCnt)
                && Objects.equals(builderEfConstruction, that.builderEfConstruction)
                && Objects.equals(builderThreadCnt, that.builderThreadCnt)
                && Objects.equals(searcherEf, that.searcherEf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), builderMaxNeighborCnt, builderEfConstruction, builderThreadCnt, searcherEf);
        }

        @Override
        public String toString() {
            return "HnswIndexOptions{"
                + super.toString()
                + ", builderMaxNeighborCnt="
                + builderMaxNeighborCnt
                + ", builderEfConstruction="
                + builderEfConstruction
                + ", builderThreadCnt="
                + builderThreadCnt
                + ", searcherEf="
                + searcherEf
                + '}';
        }
    }

    private static IndexOptions parseIndexOptions(String fieldName, Object propNode) {
        @SuppressWarnings("unchecked")
        Map<String, ?> indexOptionsMap = (Map<String, ?>) propNode;
        Object typeNode = indexOptionsMap.remove(INDEX_OPTIONS_TYPE);
        if (typeNode == null) {
            throw new MapperParsingException("[index_options] requires field [type] to be configured");
        }
        Algorithm type = Algorithm.fromString(XContentMapValues.nodeStringValue(typeNode));
        if (type == Algorithm.QC) {
            return QCIndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        } else if (type == Algorithm.HNSW) {
            return HnswIndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        } else if (type == Algorithm.LINEAR) {
            return LinearIndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        } else {
            return IndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        }
    }

    private final int dims;
    private final Similarity similarity;

    private final String category;
    private final IndexOptions indexOptions;

    private DenseVectorFieldMapper(
        String simpleName,
        DenseVectorFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.dims = mappedFieldType.dims;
        this.similarity = mappedFieldType.similarity;
        this.category = mappedFieldType.category;
        this.indexOptions = mappedFieldType.indexOptions;
    }

    public Similarity getSimilarity() {
        return similarity;
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        String simpleName = simpleName();
        String fieldName = name();
        float squaredMagnitude = 0;

        context.path().add(simpleName);

        ArrayList<Float> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();

        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                vector.add(context.parser().floatValue(false));
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            vector.add(context.parser().floatValue(false));
            context.parser().nextToken();
        }

        int size = vector.size();
        if (size != dims) {
            throw new IllegalArgumentException("vector length expects: " + dims + ", actually: " + size);
        }

        float[] array = new float[size];

        for (int i = 0; i < vector.size(); i++) {
            array[i] = vector.get(i);
            squaredMagnitude += array[i] * array[i];
        }
        checkVectorMagnitude(this.getSimilarity(), squaredMagnitude);

        VectorField point = new VectorField(fieldName, array, fieldType);
        context.doc().add(point);
        context.path().remove();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse method");
    }

    public DenseVectorFieldMapper clone() {
        return (DenseVectorFieldMapper) super.clone();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private void checkVectorMagnitude(Similarity similarity, float squaredMagnitude) {
        if (similarity == Similarity.DOT_PRODUCT && Math.abs(squaredMagnitude - 1.0f) > 1e-4f) {
            throw new IllegalArgumentException(
                "The [" + Similarity.DOT_PRODUCT.getValue() + "] " + "similarity can only be used with unit-length vectors."
            );
        }
    }

    public static class DenseVectorFieldType extends MappedFieldType {

        private final int dims;
        private final Similarity similarity;
        private final String category;
        private final IndexOptions indexOptions;

        public DenseVectorFieldType(
            String name,
            Map<String, String> meta,
            int dims,
            Similarity similarity,
            String category,
            IndexOptions indexOptions
        ) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.dims = dims;
            this.similarity = similarity;
            if (category.contains(".") || category.contains(("@"))) {
                this.category = Schema.encodeFieldWithDot(category);
            } else {
                this.category = category;
            }
            this.indexOptions = indexOptions;
        }

        public int getDims() {
            return dims;
        }

        public Similarity getSimilarity() {
            return similarity;
        }

        public String getCategory() {
            return category;
        }

        public IndexOptions getIndexOptions() {
            return indexOptions;
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("valueFetcher not supported for vector");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("termQuery not supported for vector");
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new DenseVectorFieldMapper.Builder(simpleName()).init(this);
    }

    public static class TypeParser implements FieldMapper.TypeParser {

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, parserContext, node);
            validateDims(builder);
            return builder;
        }

        private static void validateDims(DenseVectorFieldMapper.Builder builder) throws IllegalArgumentException {
            int dims = builder.dimension.get();

            if (dims <= 0 || dims > DIM_MAX) {
                throw new IllegalArgumentException("dims must be set from " + 1 + " to " + DIM_MAX);
            }
        }
    }
}
