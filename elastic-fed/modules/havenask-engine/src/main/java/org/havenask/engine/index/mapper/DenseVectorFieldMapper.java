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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.havenask.Version;
import org.havenask.common.Explicit;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.support.XContentMapValues;
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

public class DenseVectorFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "dense_vector";
    private static final int DIM_MAX = 2048;

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper) in;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        protected final Parameter<Integer> dimension = new Parameter<>("dims", false, () -> -1, (n, c, o) -> {
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

        private final Parameter<String> similarity = Parameter.stringParam(
            "similarity",
            false,
            m -> toType(m).similarity.name(),
            "l2_norm"
        );

        private final Parameter<IndexOptions> indexOptions = new Parameter<>(
            "index_options",
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
            return Arrays.asList(dimension, similarity, indexOptions, meta);
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            DenseVectorFieldType fieldType = new DenseVectorFieldType(
                name,
                meta.getValue(),
                dimension.get(),
                Similarity.fromString(similarity.get()),
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

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            Object embeddingDelimiterNode = indexOptionsMap.remove("embedding_delimiter");
            String embeddingDelimiter = embeddingDelimiterNode != null ? XContentMapValues.nodeStringValue(embeddingDelimiterNode) : null;
            Object majorOrderNode = indexOptionsMap.remove("major_order");
            MajorOrder majorOrder = majorOrderNode != null
                ? MajorOrder.fromString(XContentMapValues.nodeStringValue(majorOrderNode))
                : null;
            Object ignoreInvalidDocNode = indexOptionsMap.remove("ignore_invalid_doc");
            Boolean ignoreInvalidDoc = ignoreInvalidDocNode != null ? XContentMapValues.nodeBooleanValue(ignoreInvalidDocNode) : null;
            Object enableRecallReportNode = indexOptionsMap.remove("enable_recall_report");
            Boolean enableRecallReport = enableRecallReportNode != null ? XContentMapValues.nodeBooleanValue(enableRecallReportNode) : null;
            Object isEmbeddingSavedNode = indexOptionsMap.remove("is_embedding_saved");
            Boolean isEmbeddingSaved = isEmbeddingSavedNode != null ? XContentMapValues.nodeBooleanValue(isEmbeddingSavedNode) : null;
            Object minScanDocCntNode = indexOptionsMap.remove("min_scan_doc_cnt");
            Integer minScanDocCnt = minScanDocCntNode != null ? XContentMapValues.nodeIntegerValue(minScanDocCntNode) : null;
            Object linearBuildThresholdNode = indexOptionsMap.remove("linear_build_threshold");
            Integer linearBuildThreshold = linearBuildThresholdNode != null
                ? XContentMapValues.nodeIntegerValue(linearBuildThresholdNode)
                : null;

            // TODO valid value
            return new IndexOptions(
                embeddingDelimiter,
                majorOrder,
                ignoreInvalidDoc,
                enableRecallReport,
                isEmbeddingSaved,
                minScanDocCnt,
                linearBuildThreshold
            );
        }

        IndexOptions(
            String embeddingDelimiter,
            MajorOrder majorOrder,
            Boolean ignoreInvalidDoc,
            Boolean enableRecallReport,
            Boolean isEmbeddingSaved,
            Integer minScanDocCnt,
            Integer linearBuildThreshold
        ) {
            this.type = null;
            this.embeddingDelimiter = embeddingDelimiter;
            this.majorOrder = majorOrder;
            this.ignoreInvalidDoc = ignoreInvalidDoc;
            this.enableRecallReport = enableRecallReport;
            this.isEmbeddingSaved = isEmbeddingSaved;
            this.minScanDocCnt = minScanDocCnt;
            this.linearBuildThreshold = linearBuildThreshold;
        }

        IndexOptions(
            Algorithm type,
            String embeddingDelimiter,
            MajorOrder majorOrder,
            Boolean ignoreInvalidDoc,
            Boolean enableRecallReport,
            Boolean isEmbeddingSaved,
            Integer minScanDocCnt,
            Integer linearBuildThreshold
        ) {
            this.type = type;
            this.embeddingDelimiter = embeddingDelimiter;
            this.majorOrder = majorOrder;
            this.ignoreInvalidDoc = ignoreInvalidDoc;
            this.enableRecallReport = enableRecallReport;
            this.isEmbeddingSaved = isEmbeddingSaved;
            this.minScanDocCnt = minScanDocCnt;
            this.linearBuildThreshold = linearBuildThreshold;
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
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type.name());
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
                && Objects.equals(linearBuildThreshold, that.linearBuildThreshold);
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
                linearBuildThreshold
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
                + '}';
        }

        public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type.name());
            if (embeddingDelimiter != null) {
                builder.field("embedding_delimiter", embeddingDelimiter);
            }
            if (majorOrder != null) {
                builder.field("major_order", majorOrder.getValue());
            }
            if (ignoreInvalidDoc != null) {
                builder.field("ignore_invalid_doc", ignoreInvalidDoc);
            }
            if (enableRecallReport != null) {
                builder.field("enable_recall_report", enableRecallReport);
            }
            if (isEmbeddingSaved != null) {
                builder.field("is_embedding_saved", isEmbeddingSaved);
            }
            if (minScanDocCnt != null) {
                builder.field("min_scan_doc_cnt", minScanDocCnt);
            }
            if (linearBuildThreshold != null) {
                builder.field("linear_build_threshold", linearBuildThreshold);
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
            Object buildIndexParamsNode = indexOptionsMap.remove("build_index_params");
            Map<String, ?> buildIndexParamsMap = buildIndexParamsNode != null
                ? XContentMapValues.nodeMapValue(buildIndexParamsNode, "build_index_params")
                : null;

            String builderColumnMajorOrder = null;
            if (buildIndexParamsMap != null) {
                Object builderColumnMajorOrderNode = buildIndexParamsMap.remove("proxima.linear.builder.column_major_order");
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
                baseBuilder.startObject("build_index_params");
                {
                    baseBuilder.field("proxima.linear.builder.column_major_order", builderColumnMajorOrder);
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

            Object buildIndexParamsNode = indexOptionsMap.remove("build_index_params");
            Map<String, ?> buildIndexParamsMap = buildIndexParamsNode != null
                ? XContentMapValues.nodeMapValue(buildIndexParamsNode, "build_index_params")
                : null;
            if (buildIndexParamsMap != null) {
                Object builderTrainSampleCountNode = buildIndexParamsMap.remove("proxima.qc.builder.train_sample_count");
                builderTrainSampleCount = builderTrainSampleCountNode != null
                    ? XContentMapValues.nodeIntegerValue(builderTrainSampleCountNode)
                    : null;
                Object builderThreadCountNode = buildIndexParamsMap.remove("proxima.qc.builder.thread_count");
                builderThreadCount = builderThreadCountNode != null ? XContentMapValues.nodeIntegerValue(builderThreadCountNode) : null;
                Object builderCentroidCountNode = buildIndexParamsMap.remove("proxima.qc.builder.centroid_count");
                builderCentroidCount = builderCentroidCountNode != null
                    ? XContentMapValues.nodeStringValue(builderCentroidCountNode)
                    : null;
                Object builderClusterAutoTuningNode = buildIndexParamsMap.remove("proxima.qc.builder.cluster_auto_tuning");
                builderClusterAutoTuning = builderClusterAutoTuningNode != null
                    ? XContentMapValues.nodeBooleanValue(builderClusterAutoTuningNode)
                    : null;
                Object builderOptimizerClassNode = buildIndexParamsMap.remove("proxima.qc.builder.optimizer_class");
                builderOptimizerClass = builderOptimizerClassNode != null
                    ? XContentMapValues.nodeStringValue(builderOptimizerClassNode)
                    : null;
                Object builderOptimizerParamsNode = buildIndexParamsMap.remove("proxima.qc.builder.optimizer_params");
                builderOptimizerParams = builderOptimizerParamsNode != null
                    ? XContentMapValues.nodeStringValue(builderOptimizerParamsNode)
                    : null;
                Object builderQuantizerClassNode = buildIndexParamsMap.remove("proxima.qc.builder.quantizer_class");
                builderQuantizerClass = builderQuantizerClassNode != null
                    ? XContentMapValues.nodeStringValue(builderQuantizerClassNode)
                    : null;
                Object builderQuantizeByCentroidNode = buildIndexParamsMap.remove("proxima.qc.builder.quantize_by_centroid");
                builderQuantizeByCentroid = builderQuantizeByCentroidNode != null
                    ? XContentMapValues.nodeBooleanValue(builderQuantizeByCentroidNode)
                    : null;
                Object builderStoreOriginalFeaturesNode = buildIndexParamsMap.remove("proxima.qc.builder.store_original_features");
                builderStoreOriginalFeatures = builderStoreOriginalFeaturesNode != null
                    ? XContentMapValues.nodeBooleanValue(builderStoreOriginalFeaturesNode)
                    : null;
                Object builderTrainSampleRatioNode = buildIndexParamsMap.remove("proxima.qc.builder.train_sample_ratio");
                builderTrainSampleRatio = builderTrainSampleRatioNode != null
                    ? XContentMapValues.nodeDoubleValue(builderTrainSampleRatioNode)
                    : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, buildIndexParamsMap, Version.CURRENT);
            }

            Double searcherScanRatio = null;
            String searcherOptimizerParams = null;
            Integer searcherBruteForceThreshold = null;

            Object searcherParamsNode = indexOptionsMap.remove("search_index_params");
            Map<String, ?> searcherParamsMap = searcherParamsNode != null
                ? XContentMapValues.nodeMapValue(searcherParamsNode, "search_index_params")
                : null;
            if (searcherParamsMap != null) {
                Object searcherScanRatioNode = searcherParamsMap.remove("proxima.qc.searcher.scan_ratio");
                searcherScanRatio = searcherScanRatioNode != null ? XContentMapValues.nodeDoubleValue(searcherScanRatioNode) : null;
                Object searcherOptimizerParamsNode = searcherParamsMap.remove("proxima.qc.searcher.optimizer_params");
                searcherOptimizerParams = searcherOptimizerParamsNode != null
                    ? XContentMapValues.nodeStringValue(searcherOptimizerParamsNode)
                    : null;
                Object searcherBruteForceThresholdNode = searcherParamsMap.remove("proxima.qc.searcher.brute_force_threshold");
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

            baseBuilder.startObject("build_index_params");
            {
                if (builderTrainSampleCount != null) {
                    baseBuilder.field("proxima.qc.builder.train_sample_count", builderTrainSampleCount);
                }
                if (builderThreadCount != null) {
                    baseBuilder.field("proxima.qc.builder.thread_count", builderThreadCount);
                }
                if (builderCentroidCount != null) {
                    baseBuilder.field("proxima.qc.builder.centroid_count", builderCentroidCount);
                }
                if (builderClusterAutoTuning != null) {
                    baseBuilder.field("proxima.qc.builder.cluster_auto_tuning", builderClusterAutoTuning);
                }
                if (builderOptimizerClass != null) {
                    baseBuilder.field("proxima.qc.builder.optimizer_class", builderOptimizerClass);
                }
                if (builderOptimizerParams != null) {
                    baseBuilder.field("proxima.qc.builder.optimizer_params", builderOptimizerParams);
                }
                if (builderQuantizerClass != null) {
                    baseBuilder.field("proxima.qc.builder.quantizer_class", builderQuantizerClass);
                }
                if (builderQuantizeByCentroid != null) {
                    baseBuilder.field("proxima.qc.builder.quantize_by_centroid", builderQuantizeByCentroid);
                }
                if (builderStoreOriginalFeatures != null) {
                    baseBuilder.field("proxima.qc.builder.store_original_features", builderStoreOriginalFeatures);
                }
                if (builderTrainSampleRatio != null) {
                    baseBuilder.field("proxima.qc.builder.train_sample_ratio", builderTrainSampleRatio);
                }
            }
            baseBuilder.endObject();

            baseBuilder.startObject("search_index_params");
            {
                if (searcherScanRatio != null) {
                    baseBuilder.field("proxima.qc.searcher.scan_ratio", searcherScanRatio);
                }
                if (searcherOptimizerParams != null) {
                    baseBuilder.field("proxima.qc.searcher.optimizer_params", searcherOptimizerParams);
                }
                if (searcherBruteForceThreshold != null) {
                    baseBuilder.field("proxima.qc.searcher.brute_force_threshold", searcherBruteForceThreshold);
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
            Object buildIndexParamsNode = indexOptionsMap.remove("build_index_params");
            Map<String, ?> buildIndexParamsMap = buildIndexParamsNode != null
                ? XContentMapValues.nodeMapValue(buildIndexParamsNode, "build_index_params")
                : null;
            if (buildIndexParamsMap != null) {
                Object builderMaxNeighborCntNode = buildIndexParamsMap.remove("proxima.hnsw.builder.max_neighbor_count");
                builderMaxNeighborCnt = builderMaxNeighborCntNode != null
                    ? XContentMapValues.nodeIntegerValue(builderMaxNeighborCntNode)
                    : null;
                Object builderEfConstructionNode = buildIndexParamsMap.remove("proxima.hnsw.builder.efconstruction");
                builderEfConstruction = builderEfConstructionNode != null
                    ? XContentMapValues.nodeIntegerValue(builderEfConstructionNode)
                    : null;
                Object builderThreadCntNode = buildIndexParamsMap.remove("proxima.hnsw.builder.thread_count");
                builderThreadCnt = builderThreadCntNode != null ? XContentMapValues.nodeIntegerValue(builderThreadCntNode) : null;
                DocumentMapperParser.checkNoRemainingFields(fieldName, buildIndexParamsMap, Version.CURRENT);
            }

            Integer searcherEf = null;
            Object searcherParamsNode = indexOptionsMap.remove("search_index_params");
            Map<String, ?> searcherParamsMap = searcherParamsNode != null
                ? XContentMapValues.nodeMapValue(searcherParamsNode, "search_index_params")
                : null;
            if (searcherParamsMap != null) {
                Object searcherEfNode = searcherParamsMap.remove("proxima.hnsw.searcher.ef");
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
            baseBuilder.startObject("build_index_params");
            {
                if (builderMaxNeighborCnt != null) {
                    baseBuilder.field("proxima.hnsw.builder.max_neighbor_count", builderMaxNeighborCnt);
                }
                if (builderEfConstruction != null) {
                    baseBuilder.field("proxima.hnsw.builder.efconstruction", builderEfConstruction);
                }
                if (builderThreadCnt != null) {
                    baseBuilder.field("proxima.hnsw.builder.thread_count", builderThreadCnt);
                }
            }
            baseBuilder.endObject();
            baseBuilder.startObject("search_index_params");
            {
                if (searcherEf != null) {
                    baseBuilder.field("proxima.hnsw.searcher.ef", searcherEf);
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
        Object typeNode = indexOptionsMap.remove("type");
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
        private final IndexOptions indexOptions;

        public DenseVectorFieldType(String name, Map<String, String> meta, int dims, Similarity similarity, IndexOptions indexOptions) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.dims = dims;
            this.similarity = similarity;
            this.indexOptions = indexOptions;
        }

        public int getDims() {
            return dims;
        }

        public Similarity getSimilarity() {
            return similarity;
        }

        public IndexOptions getIndexOptions() {
            return indexOptions;
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("valueFetcher not supported for dense_vector");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("termQuery not supported for dense_vector");
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
