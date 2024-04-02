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

package org.havenask.engine.index.config.generator;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.Strings;
import org.havenask.common.io.Streams;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.Analyzers;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.config.Schema.FieldInfo;
import org.havenask.engine.index.config.Schema.VectorIndex;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.Algorithm;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.DenseVectorFieldType;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.HnswIndexOptions;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.IndexOptions;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.LinearIndexOptions;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.QCIndexOptions;
import org.havenask.engine.util.JsonPrettyFormatter;
import org.havenask.index.mapper.IdFieldMapper;
import org.havenask.index.mapper.KeywordFieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.Mapper;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.NumberFieldMapper;
import org.havenask.index.mapper.TextSearchInfo;

public class SchemaGenerator {
    // have deprecated fields or not support now.
    public static final Set<String> ExcludeFields = Set.of("_field_names", "index", "_type", "_uid", "_parent");
    public static final int SEARCH_INDEX_PARAMS_POS = 0;
    public static final int BUILD_INDEX_PARAMS_POS = 1;
    public static final String LINEAR_BUILDER = "LinearBuilder";
    public static final String QC_BUILDER = "QcBuilder";
    public static final String HNSW_BUILDER = "HnswBuilder";
    public static final String LINEAR_SEARCHER = "LinearSearcher";
    public static final String QC_SEARCHER = "QcSearcher";
    public static final String HNSW_SEARCHER = "HnswSearcher";
    public static final Map<String, String> DISTANCE_TYPE_MAP = Map.of("l2_norm", "SquaredEuclidean", "dot_product", "InnerProduct");

    Set<String> analyzers = null;

    private Set<String> getAnalyzers() {
        if (analyzers != null) {
            return analyzers;
        }
        try (InputStream is = getClass().getResourceAsStream("/config/analyzer.json")) {
            String analyzerText = Streams.copyToString(new InputStreamReader(is, StandardCharsets.UTF_8));
            Analyzers analyzers = JsonPrettyFormatter.fromJsonString(analyzerText, Analyzers.class);
            this.analyzers = Collections.unmodifiableSet(analyzers.analyzers.keySet());
            return this.analyzers;
        } catch (IOException e) {
            return Collections.emptySet();
        }
    }

    public static final String DUP_PREFIX = "DUP_";

    Map<String, String> Ha3FieldType = Map.ofEntries(
        Map.entry("keyword", "STRING"),
        Map.entry("text", "TEXT"),
        Map.entry("_routing", "STRING"),
        Map.entry("_source", "STRING"),
        Map.entry("_id", "STRING"),
        Map.entry("_seq_no", "INT64"),
        Map.entry("_primary_term", "INT64"),
        Map.entry("_version", "INT64"),
        Map.entry("long", "INT64"),
        Map.entry("float", "FLOAT"),
        Map.entry("double", "DOUBLE"),
        Map.entry("integer", "INTEGER"),
        Map.entry("short", "INT16"),
        Map.entry("byte", "INT8"),
        Map.entry("boolean", "STRING"),
        Map.entry("date", "UINT64"),
        Map.entry("vector", "STRING")
    );

    Logger logger = LogManager.getLogger(SchemaGenerator.class);

    // generate index schema from mapping
    public Schema getSchema(String table, Settings indexSettings, MapperService mapperService) {
        Schema schema = new Schema();
        schema.table_name = table;
        if (mapperService == null) {
            return defaultSchema(table);
        }

        if (mapperService.hasNested()) {
            throw new UnsupportedOperationException("nested field not support");
        }

        Set<String> addedFields = new HashSet<>();
        Set<String> analyzers = getAnalyzers();

        List<VectorIndex> vectorIndices = new ArrayList<>();

        // to support vector with category, we need to ensure the order of _id, category and vectors;
        // so we generate _id field first and generate vectors field last;
        schema.indexs.add(new Schema.PRIMARYKEYIndex(IdFieldMapper.NAME, IdFieldMapper.NAME));
        MappedFieldType idField = mapperService.fieldType(IdFieldMapper.NAME);
        if (idField != null) {
            generateSchemaField(table, idField, schema, addedFields, vectorIndices, mapperService);
        }
        for (MappedFieldType field : mapperService.fieldTypes()) {
            if (field.name().equals(IdFieldMapper.NAME)) {
                continue;
            }
            generateSchemaField(table, field, schema, addedFields, vectorIndices, mapperService);
        }

        if (schema.getDupFields().size() > 0) {
            schema.getDupFields().forEach((field) -> { schema.fields.add(new FieldInfo(DUP_PREFIX + field, "RAW")); });
        }

        // missing pre-defined fields
        if (!addedFields.contains("_primary_term")) {
            schema.attributes.add("_primary_term");
            schema.fields.add(new Schema.FieldInfo("_primary_term", Ha3FieldType.get("_primary_term")));
        }

        // extra schema process
        Integer floatToLong = EngineSettings.HA3_FLOAT_MUL_BY10.get(indexSettings);
        if (floatToLong != null && floatToLong > 0) {
            schema.floatToLongMul = (long) Math.pow(10, floatToLong);
            schema.maxFloatLong = Long.MAX_VALUE / schema.floatToLongMul;
            schema.minFloatLong = Long.MIN_VALUE / schema.floatToLongMul;
        }

        vectorIndices.forEach(vectorIndex -> { schema.indexs.add(vectorIndex); });

        return schema;
    }

    private void generateSchemaField(
        String table,
        MappedFieldType field,
        Schema schema,
        Set<String> addedFields,
        List<VectorIndex> vectorIndices,
        MapperService mapperService
    ) {
        String haFieldType = Ha3FieldType.get(field.typeName());
        String fieldName = field.name();
        if (haFieldType == null || fieldName.equals("CMD")) {
            if (fieldName.startsWith("_")) {
                logger.debug("{}: no support meta mapping type/name for field {}", table, field.name());
                return;
            } else {
                // logger.warn("{}: no support mapping type/name for field {}", table, field.name());
                throw new UnsupportedOperationException("no support mapping type (" + field.typeName() + ") for field " + field.name());
            }
        }

        // multi field index
        if (fieldName.contains(".") || fieldName.contains("@")) {
            fieldName = Schema.encodeFieldWithDot(fieldName);
        }

        // deal vector index
        if (field instanceof DenseVectorFieldType) {
            DenseVectorFieldType vectorField = (DenseVectorFieldType) field;
            if (!Strings.isNullOrEmpty(vectorField.getCategory())) {
                String categoryField = vectorField.getCategory();
                validateCategory(categoryField, mapperService);
            }
            VectorIndex vectorIndex = indexVectorField(vectorField, fieldName, schema, haFieldType);
            vectorIndices.add(vectorIndex);
            return;
        }

        addedFields.add(fieldName);
        if (ExcludeFields.contains(fieldName)) {
            return;
        }
        if (field.isStored()) {
            schema.summarys.summary_fields.add(fieldName);
        }
        // pkey stored as attribute
        if (field.hasDocValues() || fieldName.equals(IdFieldMapper.NAME)) {
            schema.attributes.add(fieldName);
        }
        // field info
        if (field.isStored() || field.hasDocValues() || field.isSearchable()) {
            Schema.FieldInfo fieldInfo = new Schema.FieldInfo(fieldName, haFieldType);
            // should configured in analyzer.json
            if (haFieldType.equals("TEXT") && field.indexAnalyzer() != null) {
                if (analyzers.contains(field.indexAnalyzer().name())) {
                    fieldInfo.analyzer = field.indexAnalyzer().name();
                } else {
                    logger.warn("analyzer " + field.indexAnalyzer().name() + ", use default");
                    // TODO support es analyzers
                    fieldInfo.analyzer = "simple_analyzer";
                }
            }
            schema.fields.add(fieldInfo);
        }
        // index
        if (field.isSearchable()) {
            Schema.Index index = null;
            String indexName = fieldName;
            if (fieldName.equals(IdFieldMapper.NAME)) {
                // index = new Schema.PRIMARYKEYIndex(indexName, fieldName);
                return;
            } else if (field.typeName().equals("date")) {
                index = new Schema.NormalIndex(indexName, "DATE", fieldName);
            } else if (haFieldType.equals("TEXT")) { // TODO defualt pack index
                index = new Schema.NormalIndex(indexName, "TEXT", fieldName);
                indexOptions(index, field.getTextSearchInfo());
            } else if (haFieldType.equals("STRING")) {
                index = new Schema.NormalIndex(indexName, "STRING", fieldName);
                indexOptions(index, field.getTextSearchInfo());
            } else if (haFieldType.equals("INT8")
                || haFieldType.equals("INT16")
                || haFieldType.equals("INTEGER")
                || haFieldType.equals("INT64")) {
                    index = new Schema.NormalIndex(indexName, "NUMBER", fieldName);
                    indexOptions(index, field.getTextSearchInfo());
                } else if (haFieldType.equals("DOUBLE") || haFieldType.equals("FLOAT")) {
                    // not support
                    return;
                    // float index will re-mapped to int64 range index
                } else {
                    throw new RuntimeException("index type not supported, field:" + field.name());
                }

            schema.indexs.add(index);
        }
    }

    private VectorIndex indexVectorField(DenseVectorFieldType vectorField, String fieldName, Schema schema, String haFieldType) {
        schema.fields.add(new Schema.FieldInfo(fieldName, haFieldType));
        String dupFieldName = DUP_PREFIX + fieldName;
        String categoryName = vectorField.getCategory();
        schema.getDupFields().add(fieldName);

        List<Schema.Field> indexFields = new ArrayList<>();
        indexFields.add(new Schema.Field(IdFieldMapper.NAME));
        if (!Objects.isNull(categoryName) && !categoryName.isEmpty()) {
            indexFields.add(new Schema.Field(categoryName));
        }
        indexFields.add(new Schema.Field(dupFieldName));

        Map<String, String> parameter = new LinkedHashMap<>();
        parameter.put(DenseVectorFieldMapper.DIMENSION, String.valueOf(vectorField.getDims()));
        parameter.put(DenseVectorFieldMapper.ENABLE_RT_BUILD, String.valueOf(true));
        parameter.put(DenseVectorFieldMapper.DISTANCE_TYPE, DISTANCE_TYPE_MAP.get(vectorField.getSimilarity().getValue()));

        IndexOptions indexOptions = vectorField.getIndexOptions();
        if (indexOptions.embeddingDelimiter != null) {
            parameter.put(DenseVectorFieldMapper.EMBEDDING_DELIMITER, indexOptions.embeddingDelimiter);
        }
        if (indexOptions.majorOrder != null) {
            parameter.put(DenseVectorFieldMapper.MAJOR_ORDER, indexOptions.majorOrder.getValue());
        }
        if (indexOptions.ignoreInvalidDoc != null) {
            parameter.put(DenseVectorFieldMapper.IGNORE_INVALID_DOC, String.valueOf(indexOptions.ignoreInvalidDoc));
        }
        if (indexOptions.enableRecallReport != null) {
            parameter.put(DenseVectorFieldMapper.ENABLE_RECALL_REPORT, String.valueOf(indexOptions.enableRecallReport));
        }
        if (indexOptions.isEmbeddingSaved != null) {
            parameter.put(DenseVectorFieldMapper.IS_EMBEDDING_SAVED, String.valueOf(indexOptions.isEmbeddingSaved));
        }
        if (indexOptions.minScanDocCnt != null) {
            parameter.put(DenseVectorFieldMapper.MIN_SCAN_DOC_CNT, String.valueOf(indexOptions.minScanDocCnt));
        }
        if (indexOptions.linearBuildThreshold != null) {
            parameter.put(DenseVectorFieldMapper.LINEAR_BUILD_THRESHOLD, String.valueOf(indexOptions.linearBuildThreshold));
        }

        String rtIndexParams = getRtIndexParams(indexOptions);
        if (!Strings.isNullOrEmpty(rtIndexParams)) {
            parameter.put(DenseVectorFieldMapper.RT_INDEX_PARAMS, rtIndexParams);
        }

        if (indexOptions.type == Algorithm.HNSW) {
            parameter.put(DenseVectorFieldMapper.BUILDER_NAME, HNSW_BUILDER);
            parameter.put(DenseVectorFieldMapper.SEARCHER_NAME, HNSW_SEARCHER);

            HnswIndexOptions hnswIndexOptions = (HnswIndexOptions) indexOptions;
            String[] SearchAndBuildIndexParams = getSearchAndBuildIndexParams(hnswIndexOptions);
            if (SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS] != null) {
                parameter.put(DenseVectorFieldMapper.SEARCH_INDEX_PARAMS, SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS]);
            }
            if (SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS] != null) {
                parameter.put(DenseVectorFieldMapper.BUILD_INDEX_PARAMS, SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS]);
            }
        } else if (indexOptions.type == Algorithm.QC) {
            parameter.put(DenseVectorFieldMapper.BUILDER_NAME, QC_BUILDER);
            parameter.put(DenseVectorFieldMapper.SEARCHER_NAME, QC_SEARCHER);

            QCIndexOptions qcIndexOptions = (QCIndexOptions) indexOptions;
            String[] SearchAndBuildIndexParams = getSearchAndBuildIndexParams(qcIndexOptions);
            if (SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS] != null) {
                parameter.put(DenseVectorFieldMapper.SEARCH_INDEX_PARAMS, SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS]);
            }
            if (SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS] != null) {
                parameter.put(DenseVectorFieldMapper.BUILD_INDEX_PARAMS, SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS]);
            }
        } else if (indexOptions.type == Algorithm.LINEAR) {
            parameter.put(DenseVectorFieldMapper.BUILDER_NAME, LINEAR_BUILDER);
            parameter.put(DenseVectorFieldMapper.SEARCHER_NAME, LINEAR_SEARCHER);

            LinearIndexOptions linearIndexOptions = (LinearIndexOptions) indexOptions;
            String[] SearchAndBuildIndexParams = getSearchAndBuildIndexParams(linearIndexOptions);
            if (SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS] != null) {
                parameter.put(DenseVectorFieldMapper.SEARCH_INDEX_PARAMS, SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS]);
            }
            if (SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS] != null) {
                parameter.put(DenseVectorFieldMapper.BUILD_INDEX_PARAMS, SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS]);
            }
        }

        return new Schema.VectorIndex(fieldName, indexFields, parameter);
    }

    // TODO: understand these flags.
    private void indexOptions(Schema.Index index, TextSearchInfo options) {
        if (options.hasOffsets()) {
            index.doc_payload_flag = 1;
            index.term_frequency_flag = 1;
        } else if (index.index_type.equals("TEXT") && (options.hasPositions())) {
            index.doc_payload_flag = 1;
            index.term_frequency_flag = 1;
            index.position_list_flag = 1;
            index.position_payload_flag = 1;
        }
    }

    /**
     * default schema
     *
     * @param table table name in schema
     * @return schema
     */
    public Schema defaultSchema(String table) {
        Schema schema = new Schema();
        schema.table_name = table;
        schema.attributes = List.of("_seq_no", "_id", "_version", "_primary_term");
        schema.summarys.summary_fields = List.of("_routing", "_source", "_id");
        schema.indexs = List.of(
            new Schema.NormalIndex("_routing", "STRING", "_routing"),
            new Schema.NormalIndex("_seq_no", "NUMBER", "_seq_no"),
            new Schema.PRIMARYKEYIndex("_id", "_id")
        );
        schema.fields = List.of(
            new Schema.FieldInfo("_routing", "STRING"),
            new Schema.FieldInfo("_seq_no", "INT64"),
            new Schema.FieldInfo("_source", "STRING"),
            new Schema.FieldInfo("_id", "STRING"),
            new Schema.FieldInfo("_version", "INT64"),
            new Schema.FieldInfo("_primary_term", "INT64")
        );
        return schema;
    }

    public String getRtIndexParams(IndexOptions indexOptions) {
        StringBuilder rtIndexParamsBuilder = new StringBuilder();
        if (Objects.nonNull(indexOptions.oswgStreamerSegmentSize) || Objects.nonNull(indexOptions.oswgStreamerEfConstruction)) {
            rtIndexParamsBuilder.append("{");
            if (Objects.nonNull(indexOptions.oswgStreamerSegmentSize)) {
                rtIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.OSWG_STREAMER_SEGMENT_SIZE + "\":");
                rtIndexParamsBuilder.append(String.valueOf(indexOptions.oswgStreamerSegmentSize) + ',');
            }
            if (Objects.nonNull(indexOptions.oswgStreamerEfConstruction)) {
                rtIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.OSWG_STREAMER_EFCONSTRUCTION + "\":");
                rtIndexParamsBuilder.append(String.valueOf(indexOptions.oswgStreamerEfConstruction) + ',');
            }
            // 删掉最后一个多余的','
            rtIndexParamsBuilder.deleteCharAt(rtIndexParamsBuilder.length() - 1);
            rtIndexParamsBuilder.append("}");
        }
        return rtIndexParamsBuilder.toString();
    }

    public String[] getSearchAndBuildIndexParams(IndexOptions indexOptions) {
        String[] resStrs = new String[2];
        if (indexOptions.type == Algorithm.HNSW) {
            HnswIndexOptions hnswIndexOptions = (HnswIndexOptions) indexOptions;

            StringBuilder hnswSearchIndexParamsBuilder = new StringBuilder();
            if (hnswIndexOptions.searcherEf != null) {
                hnswSearchIndexParamsBuilder.append("{");
                hnswSearchIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.HNSW_SEARCHER_EF + "\":");
                hnswSearchIndexParamsBuilder.append(hnswIndexOptions.searcherEf);
                hnswSearchIndexParamsBuilder.append("}");
                resStrs[SEARCH_INDEX_PARAMS_POS] = hnswSearchIndexParamsBuilder.toString();
            }

            StringBuilder hnswBuildIndexParamsBuilder = new StringBuilder();
            if (hnswIndexOptions.builderMaxNeighborCnt != null
                || hnswIndexOptions.builderEfConstruction != null
                || hnswIndexOptions.builderThreadCnt != null) {
                hnswBuildIndexParamsBuilder.append("{");
                if (hnswIndexOptions.builderMaxNeighborCnt != null) {
                    hnswBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.HNSW_BUILDER_MAX_NEIGHBOR_COUNT + "\":");
                    hnswBuildIndexParamsBuilder.append(String.valueOf(hnswIndexOptions.builderMaxNeighborCnt) + ',');
                }
                if (hnswIndexOptions.builderEfConstruction != null) {
                    hnswBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.HNSW_BUILDER_EFCONSTRUCTION + "\":");
                    hnswBuildIndexParamsBuilder.append(String.valueOf(hnswIndexOptions.builderEfConstruction) + ',');
                }
                if (hnswIndexOptions.builderThreadCnt != null) {
                    hnswBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.HNSW_BUILDER_THREAD_COUNT + "\":");
                    hnswBuildIndexParamsBuilder.append(String.valueOf(hnswIndexOptions.builderThreadCnt) + ',');
                }
                // 删掉最后一个多余的','
                hnswBuildIndexParamsBuilder.deleteCharAt(hnswBuildIndexParamsBuilder.length() - 1);
                hnswBuildIndexParamsBuilder.append("}");
                resStrs[BUILD_INDEX_PARAMS_POS] = hnswBuildIndexParamsBuilder.toString();
            }

        } else if (indexOptions.type == Algorithm.QC) {
            QCIndexOptions qcIndexOptions = (QCIndexOptions) indexOptions;
            StringBuilder qcSearchIndexParamsBuilder = new StringBuilder();
            if (qcIndexOptions.searcherScanRatio != null
                || qcIndexOptions.searcherOptimizerParams != null
                || qcIndexOptions.searcherBruteForceThreshold != null) {
                if (qcIndexOptions.searcherScanRatio != null) {
                    qcSearchIndexParamsBuilder.append("{");
                    qcSearchIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_SEARCHER_SCAN_RATIO + "\":");
                    qcSearchIndexParamsBuilder.append(qcIndexOptions.searcherScanRatio.toString() + ',');
                }
                if (qcIndexOptions.searcherOptimizerParams != null) {
                    qcSearchIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_SEARCHER_OPTIMIZER_PARAMS + "\":");
                    qcSearchIndexParamsBuilder.append('\"' + qcIndexOptions.searcherOptimizerParams + '\"' + ',');
                }
                if (qcIndexOptions.searcherBruteForceThreshold != null) {
                    qcSearchIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_SEARCHER_BRUTE_FORCE_THRESHOLD + "\":");
                    qcSearchIndexParamsBuilder.append(String.valueOf(qcIndexOptions.searcherBruteForceThreshold) + ',');
                }
                // 删掉最后一个多余的','
                qcSearchIndexParamsBuilder.deleteCharAt(qcSearchIndexParamsBuilder.length() - 1);
                qcSearchIndexParamsBuilder.append("}");
                resStrs[SEARCH_INDEX_PARAMS_POS] = qcSearchIndexParamsBuilder.toString();
            }

            StringBuilder qcBuildIndexParamsBuilder = new StringBuilder();
            if (qcIndexOptions.builderTrainSampleCount != null
                || qcIndexOptions.builderThreadCount != null
                || qcIndexOptions.builderCentroidCount != null
                || qcIndexOptions.builderClusterAutoTuning != null
                || qcIndexOptions.builderOptimizerClass != null
                || qcIndexOptions.builderOptimizerParams != null
                || qcIndexOptions.builderQuantizerClass != null
                || qcIndexOptions.builderQuantizeByCentroid != null
                || qcIndexOptions.builderStoreOriginalFeatures != null
                || qcIndexOptions.builderTrainSampleRatio != null) {
                if (qcIndexOptions.builderTrainSampleCount != null) {
                    qcBuildIndexParamsBuilder.append("{");
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_TRAIN_SAMPLE_COUNT + "\":");
                    qcBuildIndexParamsBuilder.append(String.valueOf(qcIndexOptions.builderTrainSampleCount) + ',');
                }
                if (qcIndexOptions.builderThreadCount != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_THREAD_COUNT + "\":");
                    qcBuildIndexParamsBuilder.append(String.valueOf(qcIndexOptions.builderThreadCount) + ',');
                }
                if (qcIndexOptions.builderCentroidCount != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_CENTROID_COUNT + "\":");
                    qcBuildIndexParamsBuilder.append('\"' + qcIndexOptions.builderCentroidCount + '\"' + ',');
                }
                if (qcIndexOptions.builderClusterAutoTuning != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_CLUSTER_AUTO_TUNING + "\":");
                    qcBuildIndexParamsBuilder.append(String.valueOf(qcIndexOptions.builderClusterAutoTuning) + ',');
                }
                if (qcIndexOptions.builderOptimizerClass != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_OPTIMIZER_CLASS + "\":");
                    qcBuildIndexParamsBuilder.append('\"' + qcIndexOptions.builderOptimizerClass + '\"' + ',');
                }
                if (qcIndexOptions.builderOptimizerParams != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_OPTIMIZER_PARAMS + "\":");
                    qcBuildIndexParamsBuilder.append('\"' + qcIndexOptions.builderOptimizerParams + '\"' + ',');
                }
                if (qcIndexOptions.builderQuantizerClass != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_QUANTIZER_CLASS + "\":");
                    qcBuildIndexParamsBuilder.append('\"' + qcIndexOptions.builderQuantizerClass + '\"' + ',');
                }
                if (qcIndexOptions.builderQuantizeByCentroid != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_QUANTIZE_BY_CENTROID + "\":");
                    qcBuildIndexParamsBuilder.append(String.valueOf(qcIndexOptions.builderQuantizeByCentroid) + ',');
                }
                if (qcIndexOptions.builderStoreOriginalFeatures != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_STORE_ORIGINAL_FEATURES + "\":");
                    qcBuildIndexParamsBuilder.append(String.valueOf(qcIndexOptions.builderStoreOriginalFeatures) + ',');
                }
                if (qcIndexOptions.builderTrainSampleRatio != null) {
                    qcBuildIndexParamsBuilder.append("\"" + DenseVectorFieldMapper.QC_BUILDER_TRAIN_SAMPLE_RATIO + "\":");
                    qcBuildIndexParamsBuilder.append(String.valueOf(qcIndexOptions.builderTrainSampleRatio) + ',');
                }
                // 删掉最后一个多余的','
                qcBuildIndexParamsBuilder.deleteCharAt(qcBuildIndexParamsBuilder.length() - 1);
                qcBuildIndexParamsBuilder.append("}");
                resStrs[BUILD_INDEX_PARAMS_POS] = qcBuildIndexParamsBuilder.toString();
            }

        } else if (indexOptions.type == Algorithm.LINEAR) {
            LinearIndexOptions linearIndexOptions = (LinearIndexOptions) indexOptions;
            if (linearIndexOptions.builderColumnMajorOrder != null) {
                resStrs[BUILD_INDEX_PARAMS_POS] = "{\""
                    + DenseVectorFieldMapper.LINEAR_BUILDER_COLUMN_MAJOR_ORDER
                    + "\":"
                    + linearIndexOptions.builderColumnMajorOrder
                    + '}';
            }
        }
        return resStrs;
    }

    /**
     * Validates that the vector's 'category' field exists and is of an appropriate type.
     * The category must correspond to an existing field defined in the mapping.
     * Its type should be one of the following: keyword, long, integer, short, or byte.
     */
    private void validateCategory(String categoryName, MapperService mapperService) {
        Map<String, String> fieldTypeMap = new HashMap<>();
        for (MappedFieldType fieldType : mapperService.fieldTypes()) {
            if (fieldType.name().contains(".") || fieldType.name().contains("@")) {
                fieldTypeMap.put(Schema.encodeFieldWithDot(fieldType.name()), fieldType.name());
            } else {
                fieldTypeMap.put(fieldType.name(), fieldType.name());
            }
        }

        if (!fieldTypeMap.containsKey(categoryName)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "category [%s] not found", categoryName));
        }

        String categoryFieldName = fieldTypeMap.get(categoryName);
        Mapper categoryMapper = mapperService.documentMapper().mappers().getMapper(categoryFieldName);
        if (categoryMapper instanceof KeywordFieldMapper) {

        } else if (categoryMapper instanceof NumberFieldMapper) {
            NumberFieldMapper numberFieldMapper = (NumberFieldMapper) categoryMapper;
            String fieldType = numberFieldMapper.fieldType().typeName();
            if (!fieldType.equals("long") && !fieldType.equals("integer") && !fieldType.equals("short") && !fieldType.equals("byte")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "category [%s] is not a legal type, "
                            + "category must be keyword or a integer value type"
                            + "(long, integer, short, or byte)",
                        categoryName
                    )
                );
            }
        } else {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "category [%s] is not a legal type, "
                        + "category must be keyword or a integer value type"
                        + "(long, integer, short, or byte)",
                    categoryName
                )
            );
        }
    }
}
