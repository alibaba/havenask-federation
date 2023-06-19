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
        HC("hc");

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

    public static class IndexOptions implements ToXContent {
        public final Algorithm type;

        IndexOptions(Algorithm type) {
            this.type = type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type.name());
            builder.endObject();
            return builder;
        }
    }

    /**
     * {
     * "proxima.hc.builder.num_in_level_1": "1000",
     * "proxima.hc.builder.num_in_level_2": "100",
     * "proxima.hc.common.leaf_centroid_num": "100000",
     * "proxima.general.builder.train_sample_count": "0",
     * "proxima.general.builder.train_sample_ratio": "0.0",
     * "proxima.hc.searcher.scan_num_in_level_1": "60",
     * "proxima.hc.searcher.scan_num_in_level_2": "6000",
     * "proxima.hc.searcher.max_scan_num": "50000",
     * "use_linear_threshold":"10000",
     * "use_dynamic_params":"1"
     * }
     */
    public static class HCIndexOptions extends IndexOptions {
        public final Integer numInLevel1;
        public final Integer numInLevel2;
        public final Integer leafCentroidNum;
        public final Integer trainSampleCount;
        public final Float trainSampleRatio;
        public final Integer scanNumInLevel1;
        public final Integer scanNumInLevel2;
        public final Integer maxScanNum;
        public final Integer useLinearThreshold;
        public final Integer useDynamicParams;

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            Object numInLevel1Node = indexOptionsMap.remove("proxima.hc.builder.num_in_level_1");
            Integer numInLevel1 = numInLevel1Node != null ? XContentMapValues.nodeIntegerValue(numInLevel1Node) : null;
            Object numInLevel2Node = indexOptionsMap.remove("proxima.hc.builder.num_in_level_2");
            Integer numInLevel2 = numInLevel2Node != null ? XContentMapValues.nodeIntegerValue(numInLevel2Node) : null;
            Object leafCentroidNumNode = indexOptionsMap.remove("proxima.hc.common.leaf_centroid_num");
            Integer leafCentroidNum = leafCentroidNumNode != null ? XContentMapValues.nodeIntegerValue(leafCentroidNumNode) : null;
            Object trainSampleCountNode = indexOptionsMap.remove("proxima.general.builder.train_sample_count");
            Integer trainSampleCount = trainSampleCountNode != null ? XContentMapValues.nodeIntegerValue(trainSampleCountNode) : null;
            Object trainSampleRatioNode = indexOptionsMap.remove("proxima.general.builder.train_sample_ratio");
            Float trainSampleRatio = trainSampleRatioNode != null ? XContentMapValues.nodeFloatValue(trainSampleRatioNode) : null;
            Object scanNumInLevel1Node = indexOptionsMap.remove("proxima.hc.searcher.scan_num_in_level_1");
            Integer scanNumInLevel1 = scanNumInLevel1Node != null ? XContentMapValues.nodeIntegerValue(scanNumInLevel1Node) : null;
            Object scanNumInLevel2Node = indexOptionsMap.remove("proxima.hc.searcher.scan_num_in_level_2");
            Integer scanNumInLevel2 = scanNumInLevel2Node != null ? XContentMapValues.nodeIntegerValue(scanNumInLevel2Node) : null;
            Object maxScanNumNode = indexOptionsMap.remove("proxima.hc.searcher.max_scan_num");
            Integer maxScanNum = maxScanNumNode != null ? XContentMapValues.nodeIntegerValue(maxScanNumNode) : null;
            Object useLinearThresholdNode = indexOptionsMap.remove("use_linear_threshold");
            Integer useLinearThreshold = useLinearThresholdNode != null ? XContentMapValues.nodeIntegerValue(useLinearThresholdNode) : null;
            Object useDynamicParamsNode = indexOptionsMap.remove("use_dynamic_params");
            Integer useDynamicParams = useDynamicParamsNode != null ? XContentMapValues.nodeIntegerValue(useDynamicParamsNode) : null;

            // TODO 参数校验
            DocumentMapperParser.checkNoRemainingFields(fieldName, indexOptionsMap, Version.CURRENT);
            if (numInLevel1 != null && numInLevel2 != null && leafCentroidNumNode != null) {
                if (numInLevel1 * numInLevel2 != leafCentroidNum) {
                    throw new IllegalArgumentException("num_in_level_1 * num_in_level_2 != leaf_centroid_num");
                }
            }

            // TODO vaild value
            return new HCIndexOptions(
                numInLevel1,
                numInLevel2,
                leafCentroidNum,
                trainSampleCount,
                trainSampleRatio,
                scanNumInLevel1,
                scanNumInLevel2,
                maxScanNum,
                useLinearThreshold,
                useDynamicParams
            );
        }

        public HCIndexOptions(
            Integer numInLevel1,
            Integer numInLevel2,
            Integer leafCentroidNum,
            Integer trainSampleCount,
            Float trainSampleRatio,
            Integer scanNumInLevel1,
            Integer scanNumInLevel2,
            Integer maxScanNum,
            Integer useLinearThreshold,
            Integer useDynamicParams
        ) {
            super(Algorithm.HC);
            this.numInLevel1 = numInLevel1;
            this.numInLevel2 = numInLevel2;
            this.leafCentroidNum = leafCentroidNum;
            this.trainSampleCount = trainSampleCount;
            this.trainSampleRatio = trainSampleRatio;
            this.scanNumInLevel1 = scanNumInLevel1;
            this.scanNumInLevel2 = scanNumInLevel2;
            this.maxScanNum = maxScanNum;
            this.useLinearThreshold = useLinearThreshold;
            this.useDynamicParams = useDynamicParams;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type.name());
            if (numInLevel1 != null) {
                builder.field("proxima.hc.builder.num_in_level_1", numInLevel1);
            }
            if (numInLevel2 != null) {
                builder.field("proxima.hc.builder.num_in_level_2", numInLevel2);
            }
            if (leafCentroidNum != null) {
                builder.field("proxima.hc.common.leaf_centroid_num", leafCentroidNum);
            }
            if (trainSampleCount != null) {
                builder.field("proxima.general.builder.train_sample_count", trainSampleCount);
            }
            if (trainSampleRatio != null) {
                builder.field("proxima.general.builder.train_sample_ratio", trainSampleRatio);
            }
            if (scanNumInLevel1 != null) {
                builder.field("proxima.hc.searcher.scan_num_in_level_1", scanNumInLevel1);
            }
            if (scanNumInLevel2 != null) {
                builder.field("proxima.hc.searcher.scan_num_in_level_2", scanNumInLevel2);
            }
            if (maxScanNum != null) {
                builder.field("proxima.hc.searcher.max_scan_num", maxScanNum);
            }
            if (useLinearThreshold != null) {
                builder.field("use_linear_threshold", useLinearThreshold);
            }
            if (useDynamicParams != null) {
                builder.field("use_dynamic_params", useDynamicParams);
            }
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
            HCIndexOptions that = (HCIndexOptions) o;
            return Objects.equals(numInLevel1, that.numInLevel1)
                && Objects.equals(numInLevel2, that.numInLevel2)
                && Objects.equals(leafCentroidNum, that.leafCentroidNum)
                && Objects.equals(trainSampleCount, that.trainSampleCount)
                && Objects.equals(trainSampleRatio, that.trainSampleRatio)
                && Objects.equals(scanNumInLevel1, that.scanNumInLevel1)
                && Objects.equals(scanNumInLevel2, that.scanNumInLevel2)
                && Objects.equals(maxScanNum, that.maxScanNum)
                && Objects.equals(useLinearThreshold, that.useLinearThreshold)
                && Objects.equals(useDynamicParams, that.useDynamicParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                numInLevel1,
                numInLevel2,
                leafCentroidNum,
                trainSampleCount,
                trainSampleRatio,
                scanNumInLevel1,
                scanNumInLevel2,
                maxScanNum,
                useLinearThreshold,
                useDynamicParams
            );
        }

        @Override
        public String toString() {
            return "HCIndexOptions{"
                + "numInLevel1="
                + numInLevel1
                + ", numInLevel2="
                + numInLevel2
                + ", leafCentroidNum="
                + leafCentroidNum
                + ", trainSampleCount="
                + trainSampleCount
                + ", trainSampleRatio="
                + trainSampleRatio
                + ", scanNumInLevel1="
                + scanNumInLevel1
                + ", scanNumInLevel2="
                + scanNumInLevel2
                + ", maxScanNum="
                + maxScanNum
                + ", useLinearThreshold="
                + useLinearThreshold
                + ", useDynamicParams="
                + useDynamicParams
                + '}';
        }
    }

    /**
     * {
     * "proxima.graph.common.graph_type" : "hnsw",
     * "proxima.graph.common.max_doc_cnt" : "50000000",
     * "proxima.graph.common.max_scan_num" : "25000",
     * "proxima.general.builder.memory_quota" : "0",
     * "proxima.hnsw.builder.efconstruction" : "400",
     * "proxima.hnsw.builder.max_level" : "6",
     * "proxima.hnsw.builder.scaling_factor" : "50",
     * "proxima.hnsw.builder.upper_neighbor_cnt" : "50",
     * "proxima.hnsw.searcher.ef" : "200",
     * "proxima.hnsw.searcher.max_scan_cnt" : "15000"
     * }
     */
    public static class HnswIndexOptions extends IndexOptions {
        public final Integer maxDocCnt;
        public final Integer maxScanNum;
        public final Integer memoryQuota;
        public final Integer efConstruction;
        public final Integer maxLevel;
        public final Integer scalingFactor;
        public final Integer upperNeighborCnt;
        public final Integer ef;
        public final Integer maxScanCnt;

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            Object maxDocCntNode = indexOptionsMap.remove("proxima.graph.common.max_doc_cnt");
            Integer maxDocCnt = maxDocCntNode != null ? XContentMapValues.nodeIntegerValue(maxDocCntNode) : null;
            Object maxScanNumNode = indexOptionsMap.remove("proxima.graph.common.max_scan_num");
            Integer maxScanNum = maxScanNumNode != null ? XContentMapValues.nodeIntegerValue(maxScanNumNode) : null;
            Object memoryQuotaNode = indexOptionsMap.remove("proxima.general.builder.memory_quota");
            Integer memoryQuota = memoryQuotaNode != null ? XContentMapValues.nodeIntegerValue(memoryQuotaNode) : null;
            Object efConstructionNode = indexOptionsMap.remove("proxima.hnsw.builder.efconstruction");
            Integer efConstruction = efConstructionNode != null ? XContentMapValues.nodeIntegerValue(efConstructionNode) : null;
            Object maxLevelNode = indexOptionsMap.remove("proxima.hnsw.builder.max_level");
            Integer maxLevel = maxLevelNode != null ? XContentMapValues.nodeIntegerValue(maxLevelNode) : null;
            Object scalingFactorNode = indexOptionsMap.remove("proxima.hnsw.builder.scaling_factor");
            Integer scalingFactor = scalingFactorNode != null ? XContentMapValues.nodeIntegerValue(scalingFactorNode) : null;
            Object upperNeighborCntNode = indexOptionsMap.remove("proxima.hnsw.builder.upper_neighbor_cnt");
            Integer upperNeighborCnt = upperNeighborCntNode != null ? XContentMapValues.nodeIntegerValue(upperNeighborCntNode) : null;
            Object efNode = indexOptionsMap.remove("proxima.hnsw.searcher.ef");
            Integer ef = efNode != null ? XContentMapValues.nodeIntegerValue(efNode) : null;
            Object maxScanCntNode = indexOptionsMap.remove("proxima.hnsw.searcher.max_scan_cnt");
            Integer maxScanCnt = maxScanCntNode != null ? XContentMapValues.nodeIntegerValue(maxScanCntNode) : null;

            DocumentMapperParser.checkNoRemainingFields(fieldName, indexOptionsMap, Version.CURRENT);

            // TODO vaild value
            return new HnswIndexOptions(
                maxDocCnt,
                maxScanNum,
                memoryQuota,
                efConstruction,
                maxLevel,
                scalingFactor,
                upperNeighborCnt,
                ef,
                maxScanCnt
            );
        }

        private HnswIndexOptions(
            Integer maxDocCnt,
            Integer maxScanNum,
            Integer memoryQuota,
            Integer efConstruction,
            Integer maxLevel,
            Integer scalingFactor,
            Integer upperNeighborCnt,
            Integer ef,
            Integer maxScanCnt
        ) {
            super(Algorithm.HNSW);
            this.maxDocCnt = maxDocCnt;
            this.maxScanNum = maxScanNum;
            this.memoryQuota = memoryQuota;
            this.efConstruction = efConstruction;
            this.maxLevel = maxLevel;
            this.scalingFactor = scalingFactor;
            this.upperNeighborCnt = upperNeighborCnt;
            this.ef = ef;
            this.maxScanCnt = maxScanCnt;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type.name());
            if (maxDocCnt != null) {
                builder.field("proxima.graph.common.max_doc_cnt", maxDocCnt);
            }
            if (maxScanNum != null) {
                builder.field("proxima.graph.common.max_scan_num", maxScanNum);
            }
            if (memoryQuota != null) {
                builder.field("proxima.general.builder.memory_quota", memoryQuota);
            }
            if (efConstruction != null) {
                builder.field("proxima.hnsw.builder.efconstruction", efConstruction);
            }
            if (maxLevel != null) {
                builder.field("proxima.hnsw.builder.max_level", maxLevel);
            }
            if (scalingFactor != null) {
                builder.field("proxima.hnsw.builder.scaling_factor", scalingFactor);
            }
            if (upperNeighborCnt != null) {
                builder.field("proxima.hnsw.builder.upper_neighbor_cnt", upperNeighborCnt);
            }
            if (ef != null) {
                builder.field("proxima.hnsw.searcher.ef", ef);
            }
            if (maxScanCnt != null) {
                builder.field("proxima.hnsw.searcher.max_scan_cnt", maxScanCnt);
            }
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
            HnswIndexOptions that = (HnswIndexOptions) o;
            return Objects.equals(maxDocCnt, that.maxDocCnt)
                && Objects.equals(maxScanNum, that.maxScanNum)
                && Objects.equals(efConstruction, that.efConstruction)
                && Objects.equals(maxLevel, that.maxLevel)
                && Objects.equals(scalingFactor, that.scalingFactor)
                && Objects.equals(upperNeighborCnt, that.upperNeighborCnt)
                && Objects.equals(ef, that.ef)
                && Objects.equals(maxScanCnt, that.maxScanCnt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxDocCnt, maxScanNum, efConstruction, maxLevel, scalingFactor, upperNeighborCnt, ef, maxScanCnt);
        }

        @Override
        public String toString() {
            return "HnswIndexOptions{"
                + "maxDocCnt="
                + maxDocCnt
                + ", maxScanNum="
                + maxScanNum
                + ", efConstruction="
                + efConstruction
                + ", maxLevel="
                + maxLevel
                + ", scalingFactor="
                + scalingFactor
                + ", upperNeighborCnt="
                + upperNeighborCnt
                + ", ef="
                + ef
                + ", maxScanCnt="
                + maxScanCnt
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
        if (type == Algorithm.HC) {
            return HCIndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        } else if (type == Algorithm.HNSW) {
            return HnswIndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        } else {
            return new IndexOptions(type);
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

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        String simpleName = simpleName();
        String fieldName = name();

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
        }
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
