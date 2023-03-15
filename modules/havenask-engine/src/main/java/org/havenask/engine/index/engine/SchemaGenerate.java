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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.engine.index.config.Analyzers;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.util.Utils;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.mapper.IdFieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.TextSearchInfo;
import wiremock.com.google.common.collect.ImmutableMap;
import wiremock.com.google.common.collect.ImmutableSet;

public class SchemaGenerate {
    // have deprecated fields or not support now.
    public static final ImmutableSet<String> ExcludeFields = ImmutableSet.of(
        "_field_names", "index", "_type", "_uid", "_parent"
    );

    private String version() {
        return "v3.7";
    }

    Set<String>[] Analyzers = new Set[]{null};

    private Set<String> getAnalyzers() {
        if (Analyzers[0] != null) {
            return Analyzers[0];
        }
        Path analyzerPath = Paths.get(Utils.getJarDir(), version(), "config", "analyzer.json");
        try {
            Utils.doPrivileged(() -> {
                String analyzerText = Files.readString(analyzerPath);
                Analyzers analyzers = JSON.parseObject(analyzerText, Analyzers.class);
                Analyzers[0] = Collections.unmodifiableSet(analyzers.analyzers.keySet());
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("get analyzers error!", e);
        }
        return Analyzers[0];
    }

    ImmutableMap<String,String> Ha3FieldType = new ImmutableMap.Builder<String,String>()
        .put("keyword", "STRING")
        .put("text", "TEXT")
        .put("_routing", "STRING")
        .put("_source", "STRING")
        .put("_id", "STRING")
        .put("_seq_no", "INT64")
        .put("_primary_term", "INT64")
        .put("_version", "INT64")
        .put("long", "INT64")
        .put("float", "FLOAT")
        .put("double", "DOUBLE")
        .put("integer", "INTEGER")
        .put("short", "INT16")
        .put("byte", "INT8")
        .put("boolean", "STRING") //bool-> T,F
        .put("date", "UINT64")
        .build();

    Logger logger = LogManager.getLogger(SchemaGenerate.class);

    public Schema getSchema(EngineConfig engineConfig) {
        return getSchema(engineConfig.getShardId().getIndexName(), engineConfig,
            engineConfig.getCodecService().getMapperService());
    }

    // generate index schema from mapping
    public Schema getSchema(String table, EngineConfig config, MapperService mapperService) {
        Schema schema = new Schema();
        schema.table_name = table;
        if (mapperService == null) {
            return schema;
        }

        Set<String> addedFields = new HashSet<>();
        Set<String> analyzers = getAnalyzers();
        for (MappedFieldType field : mapperService.fieldTypes()) {
            String haFieldType = Ha3FieldType.get(field.typeName());
            String fieldName = field.name();
            if (haFieldType == null || fieldName.equals("CMD")) {
                logger.warn(table + ": invalid mapping type/name for field {}", field.name());
                continue;
            }

            // multi field index
            if (fieldName.contains(".")) {
                String originField = fieldName.substring(0, fieldName.lastIndexOf('.'));
                schema.copyToFields.computeIfAbsent(originField, (k) -> new LinkedList()).add(fieldName);
                // replace '.' in field name
                fieldName = Schema.encodeFieldWithDot(fieldName);
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
                //  should configured in analyzer.json
                if (haFieldType.equals("TEXT") && field.indexAnalyzer() != null) {
                    if (analyzers.contains(field.indexAnalyzer().name())) {
                        fieldInfo.analyzer = field.indexAnalyzer().name();
                    } else {
                        logger.warn("analyzer " + field.indexAnalyzer().name() + ", use default");
                        // TODO support es analyzers
                        fieldInfo.analyzer = "taobao_analyzer";
                    }
                }
                schema.fields.add(fieldInfo);
            }
            // index
            if (field.isSearchable()) {
                Schema.Index index = null;
                String indexName = fieldName;
                if (fieldName.equals(IdFieldMapper.NAME)) {
                    index = new Schema.PRIMARYKEYIndex(indexName, fieldName);
                } else if (field.typeName().equals("date")) {
                    index = new Schema.Index(indexName, "DATE", fieldName);
                } else if (haFieldType.equals("TEXT")) { //TODO defualt pack index
                    index = new Schema.Index(indexName, "TEXT", fieldName);
                    indexOptions(index, field.getTextSearchInfo());
                } else if (haFieldType.equals("STRING")) {
                    index = new Schema.Index(indexName, "STRING", fieldName);
                    indexOptions(index, field.getTextSearchInfo());
                } else if (haFieldType.equals("INT8") ||
                    haFieldType.equals("INT16") ||
                    haFieldType.equals("INTEGER") ||
                    haFieldType.equals("INT64")) {
                    index = new Schema.Index(indexName, "NUMBER", fieldName);
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

        // missing pre-defined fields
        if (!addedFields.contains("_primary_term")) {
            schema.attributes.add("_primary_term");
            schema.fields.add(new Schema.FieldInfo("_primary_term", Ha3FieldType.get("_primary_term")));
        }
        //add local checkpoint field
        if (addedFields.contains("_local_checkpoint")) {
            throw new RuntimeException("_local_checkpoint is built-in field name!");
        }
        addedFields.add("_local_checkpoint");
        schema.attributes.add("_local_checkpoint");
        schema.fields.add(new Schema.FieldInfo("_local_checkpoint", Ha3FieldType.get("_local_checkpoint")));
        schema.summarys.summary_fields.add("_local_checkpoint");

        // extra schema process
        Integer floatToLong = EngineSettings.HA3_FLOAT_MUL_BY10.get(config.getIndexSettings().getSettings());
        if (floatToLong != null && floatToLong > 0) {
            schema.floatToLongMul = (long)Math.pow(10, floatToLong);
            schema.maxFloatLong = Long.MAX_VALUE / schema.floatToLongMul;
            schema.minFloatLong = Long.MIN_VALUE / schema.floatToLongMul;
        }

        return schema;
    }

    // TODO: understand these flags.
    private void indexOptions(Schema.Index index, TextSearchInfo options) {
        if (options.hasOffsets()) {
            index.doc_payload_flag = 1;
            index.term_frequency_flag = 1;
        } else if (index.index_type.equals("TEXT") &&
            (options.hasPositions())) {
            index.doc_payload_flag = 1;
            index.term_frequency_flag = 1;
            index.position_list_flag = 1;
            index.position_payload_flag = 1;
        }
    }
}
