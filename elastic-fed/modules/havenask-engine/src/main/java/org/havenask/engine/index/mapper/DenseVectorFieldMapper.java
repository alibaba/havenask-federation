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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.havenask.common.Explicit;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.support.XContentMapValues;
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

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper)in;
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
                    String.format("Unable to parse [dims] from provided value [%s] for vector [%s]", o, name)
                );
            }
            if (value <= 0) {
                throw new IllegalArgumentException(
                    String.format("Dimension value must be greater than 0 for vector: %s", name));
            }
            return value;
        }, m -> toType(m).dims);

        private final Parameter<String> similarity = Parameter.stringParam(
            "similarity",
            false,
            m -> toType(m).similarity.name(),
            "l2_norm"
        );

        private final Parameter<String> algorithm = Parameter.stringParam(
            "algorithm",
            false,
            m -> toType(m).algorithm.name(),
            "hnsw"
        );

        private final Parameter<IndexOptions> indexOptions = new Parameter<>(
            "index_options",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : parseIndexOptions(n, o),
            m -> toType(m).indexOptions
        );

        protected final Parameter<Map<String, String>> meta = Parameter.metaParam();

        /**
         * Creates a new Builder with a field name
         *
         * @param name
         */
        protected Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(dimension, similarity, algorithm, indexOptions);
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            DenseVectorFieldType fieldType = new DenseVectorFieldType(name, meta.getValue(), dimension.get(),
                Algorithm.fromString(algorithm.get()), Similarity.fromString(similarity.get()), indexOptions.get());
            return new DenseVectorFieldMapper(name, fieldType, multiFieldsBuilder.build(this, context), copyTo.build(),
                new Explicit<>(false, false));
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
        L2_NORM("l2_norm"),
        DOT_PRODUCT("dot_product");

        private final String value;

        Similarity(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
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

    public abstract static class IndexOptions implements ToXContent {
        final String type;

        IndexOptions(String type) {
            this.type = type;
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

        static IndexOptions parseIndexOptions(Map<String, ?> indexOptionsMap) {
            Integer numInLevel1 = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hc.builder.num_in_level_1"));
            Integer numInLevel2 = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hc.builder.num_in_level_2"));
            Integer leafCentroidNum = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hc.common.leaf_centroid_num"));
            Integer trainSampleCount = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.general.builder.train_sample_count"));
            Float trainSampleRatio = XContentMapValues.nodeFloatValue(
                indexOptionsMap.get("proxima.general.builder.train_sample_ratio"));
            Integer scanNumInLevel1 = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hc.searcher.scan_num_in_level_1"));
            Integer scanNumInLevel2 = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hc.searcher.scan_num_in_level_2"));
            Integer maxScanNum = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hc.searcher.max_scan_num"));
            Integer useLinearThreshold = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("use_linear_threshold"));

            // TODO vaild value
            Integer useDynamicParams = XContentMapValues.nodeIntegerValue(indexOptionsMap.get("use_dynamic_params"));
            return new HCIndexOptions(numInLevel1, numInLevel2, leafCentroidNum, trainSampleCount, trainSampleRatio,
                scanNumInLevel1, scanNumInLevel2, maxScanNum, useLinearThreshold, useDynamicParams);
        }

        public HCIndexOptions(Integer numInLevel1, Integer numInLevel2, Integer leafCentroidNum,
            Integer trainSampleCount,
            Float trainSampleRatio, Integer scanNumInLevel1, Integer scanNumInLevel2, Integer maxScanNum,
            Integer useLinearThreshold, Integer useDynamicParams) {
            super("proxima_hc");
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
            builder.field("type", type);
            builder.field("proxima.hc.builder.num_in_level_1", numInLevel1);
            builder.field("proxima.hc.builder.num_in_level_2", numInLevel2);
            builder.field("proxima.hc.common.leaf_centroid_num", leafCentroidNum);
            builder.field("proxima.general.builder.train_sample_count", trainSampleCount);
            builder.field("proxima.general.builder.train_sample_ratio", trainSampleRatio);
            builder.field("proxima.hc.searcher.scan_num_in_level_1", scanNumInLevel1);
            builder.field("proxima.hc.searcher.scan_num_in_level_2", scanNumInLevel2);
            builder.field("proxima.hc.searcher.max_scan_num", maxScanNum);
            builder.field("use_linear_threshold", useLinearThreshold);
            builder.field("use_dynamic_params", useDynamicParams);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {return true;}
            if (o == null || getClass() != o.getClass()) {return false;}
            HCIndexOptions that = (HCIndexOptions)o;
            return Objects.equals(numInLevel1, that.numInLevel1) && Objects.equals(numInLevel2,
                that.numInLevel2) && Objects.equals(leafCentroidNum, that.leafCentroidNum)
                && Objects.equals(trainSampleCount, that.trainSampleCount) && Objects.equals(
                trainSampleRatio, that.trainSampleRatio) && Objects.equals(scanNumInLevel1, that.scanNumInLevel1)
                && Objects.equals(scanNumInLevel2, that.scanNumInLevel2) && Objects.equals(maxScanNum,
                that.maxScanNum) && Objects.equals(useLinearThreshold, that.useLinearThreshold)
                && Objects.equals(useDynamicParams, that.useDynamicParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(numInLevel1, numInLevel2, leafCentroidNum, trainSampleCount, trainSampleRatio,
                scanNumInLevel1,
                scanNumInLevel2, maxScanNum, useLinearThreshold, useDynamicParams);
        }

        @Override
        public String toString() {
            return "HCIndexOptions{" +
                "numInLevel1=" + numInLevel1 +
                ", numInLevel2=" + numInLevel2 +
                ", leafCentroidNum=" + leafCentroidNum +
                ", trainSampleCount=" + trainSampleCount +
                ", trainSampleRatio=" + trainSampleRatio +
                ", scanNumInLevel1=" + scanNumInLevel1 +
                ", scanNumInLevel2=" + scanNumInLevel2 +
                ", maxScanNum=" + maxScanNum +
                ", useLinearThreshold=" + useLinearThreshold +
                ", useDynamicParams=" + useDynamicParams +
                '}';
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

        static IndexOptions parseIndexOptions(Map<String, ?> indexOptionsMap) {
            Integer maxDocCnt = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.graph.common.max_doc_cnt"));
            Integer maxScanNum = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.graph.common.max_scan_num"));
            Integer memoryQuota = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.general.builder.memory_quota"));
            Integer efConstruction = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hnsw.builder.efconstruction"));
            Integer maxLevel = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hnsw.builder.max_level"));
            Integer scalingFactor = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hnsw.builder.scaling_factor"));
            Integer upperNeighborCnt = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hnsw.builder.upper_neighbor_cnt"));
            Integer ef = XContentMapValues.nodeIntegerValue(indexOptionsMap.get("proxima.hnsw.searcher.ef"));
            Integer maxScanCnt = XContentMapValues.nodeIntegerValue(
                indexOptionsMap.get("proxima.hnsw.searcher.max_scan_cnt"));

            // TODO vaild value
            return new HnswIndexOptions(maxDocCnt, maxScanNum, memoryQuota, efConstruction, maxLevel, scalingFactor,
                upperNeighborCnt, ef, maxScanCnt);
        }

        private HnswIndexOptions(Integer maxDocCnt, Integer maxScanNum, Integer memoryQuota, Integer efConstruction,
            Integer maxLevel,
            Integer scalingFactor, Integer upperNeighborCnt, Integer ef, Integer maxScanCnt) {
            super("hnsw");
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
            builder.field("type", type);
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
            if (this == o) {return true;}
            if (o == null || getClass() != o.getClass()) {return false;}
            HnswIndexOptions that = (HnswIndexOptions)o;
            return Objects.equals(maxDocCnt, that.maxDocCnt) && Objects.equals(maxScanNum, that.maxScanNum)
                && Objects.equals(efConstruction, that.efConstruction) && Objects.equals(maxLevel,
                that.maxLevel) && Objects.equals(scalingFactor, that.scalingFactor) && Objects.equals(
                upperNeighborCnt, that.upperNeighborCnt) && Objects.equals(ef, that.ef) && Objects.equals(
                maxScanCnt, that.maxScanCnt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxDocCnt, maxScanNum, efConstruction, maxLevel, scalingFactor, upperNeighborCnt, ef,
                maxScanCnt);
        }

        @Override
        public String toString() {
            return "HnswIndexOptions{" +
                "maxDocCnt=" + maxDocCnt +
                ", maxScanNum=" + maxScanNum +
                ", efConstruction=" + efConstruction +
                ", maxLevel=" + maxLevel +
                ", scalingFactor=" + scalingFactor +
                ", upperNeighborCnt=" + upperNeighborCnt +
                ", ef=" + ef +
                ", maxScanCnt=" + maxScanCnt +
                '}';
        }
    }

    private static IndexOptions parseIndexOptions(String fieldName, Object propNode) {
        @SuppressWarnings("unchecked")
        Map<String, ?> indexOptionsMap = (Map<String, ?>)propNode;
        Object typeNode = indexOptionsMap.remove("type");
        if (typeNode == null) {
            throw new MapperParsingException("[index_options] requires field [type] to be configured");
        }
        String type = XContentMapValues.nodeStringValue(typeNode);
        if (type.equals("hnsw")) {
            return HnswIndexOptions.parseIndexOptions(indexOptionsMap);
        } else if (type.equals("hc")) {
            return HCIndexOptions.parseIndexOptions(indexOptionsMap);
        } else {
            throw new MapperParsingException(
                "Unknown vector index options type [" + type + "] for field [" + fieldName + "]");
        }
    }

    private final int dims;
    private final Algorithm algorithm;
    private final Similarity similarity;
    private final IndexOptions indexOptions;

    private DenseVectorFieldMapper(String simpleName, DenseVectorFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Explicit<Boolean> ignoreMalformed) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.dims = mappedFieldType.dims;
        this.algorithm = mappedFieldType.algorithm;
        this.similarity = mappedFieldType.similarity;
        this.indexOptions = mappedFieldType.indexOptions;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
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
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class DenseVectorFieldType extends MappedFieldType {

        private final int dims;
        private final Algorithm algorithm;
        private final Similarity similarity;
        private final IndexOptions indexOptions;

        public DenseVectorFieldType(String name, Map<String, String> meta, int dims, Algorithm algorithm,
            Similarity similarity, IndexOptions indexOptions) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.dims = dims;
            this.algorithm = algorithm;
            this.similarity = similarity;
            this.indexOptions = indexOptions;
        }

        public int getDims() {
            return dims;
        }

        public Algorithm getAlgorithm() {
            return algorithm;
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
            // add vaildation
            return builder;
        }
    }
}
