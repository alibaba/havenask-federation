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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.io.Streams;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.Analyzers;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.config.Schema.FieldInfo;
import org.havenask.engine.index.config.Schema.VectorIndex;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.Algorithm;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.DenseVectorFieldType;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.HnswIndexOptions;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.IndexOptions;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.LinearIndexOptions;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.QCIndexOptions;
import org.havenask.engine.util.JsonPrettyFormatter;
import org.havenask.index.mapper.IdFieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperService;
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

        schema.indexs.add(new Schema.PRIMARYKEYIndex(IdFieldMapper.NAME, IdFieldMapper.NAME));

        List<VectorIndex> vectorIndices = new ArrayList<>();
        for (MappedFieldType field : mapperService.fieldTypes()) {
            String haFieldType = Ha3FieldType.get(field.typeName());
            String fieldName = field.name();
            if (haFieldType == null || fieldName.equals("CMD")) {
                if (fieldName.startsWith("_")) {
                    logger.debug("{}: no support meta mapping type/name for field {}", table, field.name());
                    continue;
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
                VectorIndex vectorIndex = indexVectorField(vectorField, fieldName, schema, haFieldType);
                vectorIndices.add(vectorIndex);
                continue;
            }

            addedFields.add(fieldName);
            if (ExcludeFields.contains(fieldName)) {
                continue;
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
                    continue;
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
                        continue;
                        // float index will re-mapped to int64 range index
                    } else {
                        throw new RuntimeException("index type not supported, field:" + field.name());
                    }

                schema.indexs.add(index);
            }
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

    private VectorIndex indexVectorField(DenseVectorFieldType vectorField, String fieldName, Schema schema, String haFieldType) {
        schema.fields.add(new Schema.FieldInfo(fieldName, haFieldType));
        String dupFieldName = DUP_PREFIX + fieldName;
        schema.getDupFields().add(fieldName);
        List<Schema.Field> indexFields = Arrays.asList(new Schema.Field(IdFieldMapper.NAME), new Schema.Field(dupFieldName));
        Map<String, String> parameter = new LinkedHashMap<>();
        parameter.put("dimension", String.valueOf(vectorField.getDims()));
        parameter.put("enable_rt_build", String.valueOf(true));
        parameter.put("distance_type", DISTANCE_TYPE_MAP.get(vectorField.getSimilarity().getValue()));

        IndexOptions indexOptions = vectorField.getIndexOptions();
        if (indexOptions.embeddingDelimiter != null) {
            parameter.put("embedding_delimiter", indexOptions.embeddingDelimiter);
        }
        if (indexOptions.majorOrder != null) {
            parameter.put("major_order", indexOptions.majorOrder.getValue());
        }
        if (indexOptions.ignoreInvalidDoc != null) {
            parameter.put("ignore_invalid_doc", String.valueOf(indexOptions.ignoreInvalidDoc));
        }
        if (indexOptions.enableRecallReport != null) {
            parameter.put("enable_recall_report", String.valueOf(indexOptions.enableRecallReport));
        }
        if (indexOptions.isEmbeddingSaved != null) {
            parameter.put("is_embedding_saved", String.valueOf(indexOptions.isEmbeddingSaved));
        }
        if (indexOptions.minScanDocCnt != null) {
            parameter.put("min_scan_doc_cnt", String.valueOf(indexOptions.minScanDocCnt));
        }
        if (indexOptions.linearBuildThreshold != null) {
            parameter.put("linear_build_threshold", String.valueOf(indexOptions.linearBuildThreshold));
        }

        if (indexOptions.type == Algorithm.HNSW) {
            parameter.put("builder_name", HNSW_BUILDER);
            parameter.put("searcher_name", HNSW_SEARCHER);

            HnswIndexOptions hnswIndexOptions = (HnswIndexOptions) indexOptions;
            String[] SearchAndBuildIndexParams = getSearchAndBuildIndexParams(hnswIndexOptions);
            if (SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS] != null) {
                parameter.put("searcher_index_params", SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS]);
            }
            if (SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS] != null) {
                parameter.put("builder_index_params", SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS]);
            }
        } else if (indexOptions.type == Algorithm.QC) {
            parameter.put("builder_name", QC_BUILDER);
            parameter.put("searcher_name", QC_SEARCHER);

            QCIndexOptions qcIndexOptions = (QCIndexOptions) indexOptions;
            String[] SearchAndBuildIndexParams = getSearchAndBuildIndexParams(qcIndexOptions);
            if (SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS] != null) {
                parameter.put("searcher_index_params", SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS]);
            }
            if (SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS] != null) {
                parameter.put("builder_index_params", SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS]);
            }
        } else if (indexOptions.type == Algorithm.LINEAR) {
            parameter.put("builder_name", LINEAR_BUILDER);
            parameter.put("searcher_name", LINEAR_SEARCHER);

            LinearIndexOptions linearIndexOptions = (LinearIndexOptions) indexOptions;
            String[] SearchAndBuildIndexParams = getSearchAndBuildIndexParams(linearIndexOptions);
            if (SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS] != null) {
                parameter.put("searcher_index_params", SearchAndBuildIndexParams[SEARCH_INDEX_PARAMS_POS]);
            }
            if (SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS] != null) {
                parameter.put("builder_index_params", SearchAndBuildIndexParams[BUILD_INDEX_PARAMS_POS]);
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

    public String[] getSearchAndBuildIndexParams(IndexOptions indexOptions) {
        String[] resStrs = new String[2];
        if (indexOptions.type == Algorithm.HNSW) {
            HnswIndexOptions hnswIndexOptions = (HnswIndexOptions) indexOptions;

            StringBuilder hnswSearchIndexParamsBuilder = new StringBuilder();
            if (hnswIndexOptions.searcherEf != null) {
                hnswSearchIndexParamsBuilder.append("{");
                hnswSearchIndexParamsBuilder.append("\"proxima.hnsw.searcher.ef\":" + hnswIndexOptions.searcherEf);
                hnswSearchIndexParamsBuilder.append("}");
                resStrs[SEARCH_INDEX_PARAMS_POS] = hnswSearchIndexParamsBuilder.toString();
            }

            StringBuilder hnswBuildIndexParamsBuilder = new StringBuilder();
            if (hnswIndexOptions.builderMaxNeighborCnt != null
                || hnswIndexOptions.builderEfConstruction != null
                || hnswIndexOptions.builderThreadCnt != null) {
                hnswBuildIndexParamsBuilder.append("{");
                if (hnswIndexOptions.builderMaxNeighborCnt != null) {
                    hnswBuildIndexParamsBuilder.append(
                        "\"proxima.hnsw.builder.max_neighbor_cnt\":" + hnswIndexOptions.builderMaxNeighborCnt + ','
                    );
                }
                if (hnswIndexOptions.builderEfConstruction != null) {
                    hnswBuildIndexParamsBuilder.append(
                        "\"proxima.hnsw.builder.ef_construction\":" + hnswIndexOptions.builderEfConstruction + ','
                    );
                }
                if (hnswIndexOptions.builderThreadCnt != null) {
                    hnswBuildIndexParamsBuilder.append("\"proxima.hnsw.builder.thread_cnt\":" + hnswIndexOptions.builderThreadCnt + ',');
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
                    qcSearchIndexParamsBuilder.append("\"proxima.qc.searcher.scan_ratio\":" + qcIndexOptions.searcherScanRatio + ',');
                }
                if (qcIndexOptions.searcherOptimizerParams != null) {
                    qcSearchIndexParamsBuilder.append(
                        "\"proxima.qc.searcher.optimizer_params\":" + '\"' + qcIndexOptions.searcherOptimizerParams + '\"' + ','
                    );
                }
                if (qcIndexOptions.searcherBruteForceThreshold != null) {
                    qcSearchIndexParamsBuilder.append(
                        "\"proxima.qc.searcher.brute_force_threshold\":" + qcIndexOptions.searcherBruteForceThreshold + ','
                    );
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
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.train_sample_count\":" + qcIndexOptions.builderTrainSampleCount + ','
                    );
                }
                if (qcIndexOptions.builderThreadCount != null) {
                    qcBuildIndexParamsBuilder.append("\"proxima.qc.builder.thread_count\":" + qcIndexOptions.builderThreadCount + ',');
                }
                if (qcIndexOptions.builderCentroidCount != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.centroid_count\":" + '\"' + qcIndexOptions.builderCentroidCount + '\"' + ','
                    );
                }
                if (qcIndexOptions.builderClusterAutoTuning != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.cluster_auto_tuning\":" + qcIndexOptions.builderClusterAutoTuning + ','
                    );
                }
                if (qcIndexOptions.builderOptimizerClass != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.optimizer_class\":" + '\"' + qcIndexOptions.builderOptimizerClass + '\"' + ','
                    );
                }
                if (qcIndexOptions.builderOptimizerParams != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.optimizer_params\":" + '\"' + qcIndexOptions.builderOptimizerParams + '\"' + ','
                    );
                }
                if (qcIndexOptions.builderQuantizerClass != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.quantizer_class\":" + '\"' + qcIndexOptions.builderQuantizerClass + '\"' + ','
                    );
                }
                if (qcIndexOptions.builderQuantizeByCentroid != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.quantize_by_centroid\":" + qcIndexOptions.builderQuantizeByCentroid + ','
                    );
                }
                if (qcIndexOptions.builderStoreOriginalFeatures != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.store_original_features\":" + qcIndexOptions.builderStoreOriginalFeatures + ','
                    );
                }
                if (qcIndexOptions.builderTrainSampleRatio != null) {
                    qcBuildIndexParamsBuilder.append(
                        "\"proxima.qc.builder.train_sample_ratio\":" + qcIndexOptions.builderTrainSampleRatio + ','
                    );
                }
                // 删掉最后一个多余的','
                qcBuildIndexParamsBuilder.deleteCharAt(qcBuildIndexParamsBuilder.length() - 1);
                qcBuildIndexParamsBuilder.append("}");
                resStrs[BUILD_INDEX_PARAMS_POS] = qcBuildIndexParamsBuilder.toString();
            }

        } else if (indexOptions.type == Algorithm.LINEAR) {
            LinearIndexOptions linearIndexOptions = (LinearIndexOptions) indexOptions;
            if (linearIndexOptions.linearBuildThreshold != null) {
                resStrs[SEARCH_INDEX_PARAMS_POS] = "{\"proxima.hnsw.builder.linear_build_threshold\":"
                    + linearIndexOptions.linearBuildThreshold
                    + '}';
            }
        }
        return resStrs;
    }
}
