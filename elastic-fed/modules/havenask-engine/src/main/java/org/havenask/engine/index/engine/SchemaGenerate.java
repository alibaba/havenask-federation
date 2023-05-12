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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.io.Streams;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.Analyzers;
import org.havenask.engine.index.config.Schema;
import org.havenask.index.mapper.IdFieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.TextSearchInfo;

public class SchemaGenerate {
    // have deprecated fields or not support now.
    public static final Set<String> ExcludeFields = Set.of("_field_names", "index", "_type", "_uid", "_parent");

    Set<String> analyzers = null;

    private Set<String> getAnalyzers() {
        if (analyzers != null) {
            return analyzers;
        }
        try (InputStream is = getClass().getResourceAsStream("/config/analyzer.json")) {
            String analyzerText = Streams.copyToString(new InputStreamReader(is, StandardCharsets.UTF_8));
            Analyzers analyzers = JSON.parseObject(analyzerText, Analyzers.class);
            this.analyzers = Collections.unmodifiableSet(analyzers.analyzers.keySet());
            return this.analyzers;
        } catch (IOException e) {
            return Collections.emptySet();
        }
    }

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
        Map.entry("date", "UINT64")
    );

    Logger logger = LogManager.getLogger(SchemaGenerate.class);

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
            if (fieldName.contains(".")) {
                String originField = fieldName.substring(0, fieldName.lastIndexOf('.'));
                schema.copyToFields.computeIfAbsent(originField, (k) -> new LinkedList<>()).add(fieldName);
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
                // should configured in analyzer.json
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
                } else if (haFieldType.equals("TEXT")) { // TODO defualt pack index
                    index = new Schema.Index(indexName, "TEXT", fieldName);
                    indexOptions(index, field.getTextSearchInfo());
                } else if (haFieldType.equals("STRING")) {
                    index = new Schema.Index(indexName, "STRING", fieldName);
                    indexOptions(index, field.getTextSearchInfo());
                } else if (haFieldType.equals("INT8")
                    || haFieldType.equals("INT16")
                    || haFieldType.equals("INTEGER")
                    || haFieldType.equals("INT64")) {
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

        // extra schema process
        Integer floatToLong = EngineSettings.HA3_FLOAT_MUL_BY10.get(indexSettings);
        if (floatToLong != null && floatToLong > 0) {
            schema.floatToLongMul = (long) Math.pow(10, floatToLong);
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
            new Schema.Index("_routing", "STRING", "_routing"),
            new Schema.Index("_seq_no", "NUMBER", "_seq_no"),
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
}
