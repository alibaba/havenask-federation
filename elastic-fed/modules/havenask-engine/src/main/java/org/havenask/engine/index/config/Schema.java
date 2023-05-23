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

package org.havenask.engine.index.config;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;

import org.havenask.common.Strings;
import org.havenask.engine.util.JsonPrettyFormatter;

public class Schema {
    public Summarys summarys = new Summarys();
    public List<String> attributes = new LinkedList<>();
    public List<FieldInfo> fields = new LinkedList<>();
    public List<Index> indexs = new LinkedList<>();
    public String table_name;
    public String table_type = "normal";

    private transient List<String> dupFields = new LinkedList<>();

    public transient String rawSchema;
    // origin field process parameters
    // copyTo field will copy the origin field and write the copied ones(such as multiFields, etc.)
    public transient Map<String/*origin*/, List<String>> copyToFields = new HashMap<>();
    // float mapped fields will replace/rewrite origin search query
    // should also write twice as copyToFields do.
    public transient Map<String, String> float_long_field_map = new HashMap<>();
    public transient long floatToLongMul = 1L;
    public transient long maxFloatLong = Long.MAX_VALUE;
    public transient long minFloatLong = Long.MIN_VALUE;
    public static final transient String FLOAT_MULTI_FIELD_NAME = "_f2i";
    public static final transient char FIELD_DOT_REPLACEMENT = '_';

    // HA3 do not allow '.' for field name
    // currently only multi-field index contains '.'
    public static final String encodeFieldWithDot(String field) {
        return field.replace('.', FIELD_DOT_REPLACEMENT);
    }

    public final long floatToLongVal(double d) {
        if (d >= maxFloatLong) {
            d = maxFloatLong;
        } else if (d <= minFloatLong) {
            d = minFloatLong;
        } else {
            d = d * floatToLongMul;
        }
        return (long) d;
    }

    public static class FieldInfo {
        public FieldInfo() {}

        public FieldInfo(String field_name, String field_type) {
            this.field_name = field_name;
            this.field_type = field_type;
        }

        public String field_name;
        public String field_type;
        public boolean binary_field;
        public String analyzer;
    }

    public abstract static class Index {
        public String index_name;
        public String index_type;

        public Index() {}

        public Index(String index_name, String index_type) {
            this.index_name = index_name;
            this.index_type = index_type;
        }

        public Integer doc_payload_flag;
        public Integer term_payload_flag;
        public Integer position_payload_flag;
        public Integer position_list_flag;
        public Integer term_frequency_flag;
    }

    public static class NormalIndex extends Index {
        @JSONField(name = "index_fields")
        public String index_field;

        public NormalIndex() {}

        public NormalIndex(String index_name, String index_type, String index_field) {
            super(index_name, index_type);
            this.index_field = index_field;
        }
    }

    /**
     * {
     * "index_name": "embedding_index",
     * "index_type":"CUSTOMIZED",
     * "index_fields":[
     * {
     * "boost":1,
     * "field_name":"DUP_pk"
     * },
     * {
     * "boost":1,
     * "field_name":"DUP_embedding"
     * }
     * ],
     * "indexer":"aitheta_indexer",
     * "parameters":{
     * "use_linear_threshold":"10000",
     * "build_metric_type":"l2",
     * "search_metric_type":"ip",
     * "use_dynamic_params":"1",
     * "dimension":"1024",
     * "index_type":"hc"
     * }
     * }
     */
    public static class VectorIndex extends Index {
        public String indexer = "aitheta_indexer";

        @JSONField(name = "index_fields")
        public List<Field> index_fields;
        public Map<String, String> parameters;

        public VectorIndex(String index_name, List<Field> index_fields, Map<String, String> parameters) {
            super(index_name, "CUSTOMIZED");
            this.index_fields = index_fields;
            this.parameters = parameters;
        }
    }

    public static class Field {
        public int boost = 1;
        public String field_name;

        public Field(String field_name) {
            this.field_name = field_name;
        }
    }

    public static class PRIMARYKEYIndex extends NormalIndex {
        public PRIMARYKEYIndex() {
            index_type = "PRIMARYKEY64";
        }

        public PRIMARYKEYIndex(String index_name, String index_field) {
            super(index_name, "PRIMARYKEY64", index_field);
        }

        public boolean has_primary_key_attribute = true;
        public boolean is_primary_key_sorted;
        public String pk_storage_type;
    }

    public static class BoostedField {
        public String field_name;
        public int boost = 1;
    }

    public static class PackIndex extends Index {
        public PackIndex() {
            index_type = "PACK";
        }

        public List<BoostedField> index_fields = new LinkedList<>();
    }

    public static class Summarys {
        public List<String> summary_fields = new LinkedList<>();
    }

    public boolean hasRawSchema() {
        return !Strings.isNullOrEmpty(rawSchema);
    }

    public List<String> getDupFields() {
        return dupFields;
    }

    @Override
    public String toString() {
        if (!hasRawSchema()) {
            return JsonPrettyFormatter.toJsonString(this);
        }
        JSONObject rawSchemaObj = JsonPrettyFormatter.fromString(rawSchema);
        // replace table type
        String tableType = rawSchemaObj.getString("table_type");
        if ((!Strings.isNullOrEmpty(tableType)) && (!tableType.equals("normal"))) {
            return rawSchema;
        }
        // add summary
        if (summarys != null) {
            if (!rawSchemaObj.containsKey("summarys")) {
                rawSchemaObj.put("summarys", this.summarys);
            } else {
                JSONObject summarysObj = rawSchemaObj.getJSONObject("summarys");
                if (!summarysObj.containsKey("summary_fields")) {
                    summarysObj.put("summary_fields", summarys.summary_fields);
                } else {
                    JSONArray summaryFieldsArr = summarysObj.getJSONArray("summary_fields");
                    summaryFieldsArr.addAll(summarys.summary_fields);
                    summarysObj.put("summary_fields", summaryFieldsArr);
                }
                rawSchemaObj.put("summarys", summarysObj);
            }
        }
        // add attributes
        if (attributes != null && attributes.size() > 0) {
            if (!rawSchemaObj.containsKey("attributes")) {
                rawSchemaObj.put("attributes", attributes);
            } else {
                JSONArray attributesArr = rawSchemaObj.getJSONArray("attributes");
                attributesArr.addAll(attributes);
                rawSchemaObj.put("attributes", attributesArr);
            }
        }
        // add fields
        if (fields != null && fields.size() > 0) {
            if (!rawSchemaObj.containsKey("fields")) {
                rawSchemaObj.put("fields", fields);
            } else {
                JSONArray fieldsArr = rawSchemaObj.getJSONArray("fields");
                fieldsArr.addAll(fields);
                rawSchemaObj.put("fields", fieldsArr);
            }
        }
        // add indexs
        if (indexs != null && indexs.size() > 0) {
            if (!rawSchemaObj.containsKey("indexs")) {
                rawSchemaObj.put("indexs", indexs);
            } else {
                JSONArray indexsArr = rawSchemaObj.getJSONArray("indexs");
                indexsArr.addAll(indexs);
                rawSchemaObj.put("indexs", indexsArr);
            }
        }
        // replace table name
        if (!Strings.isNullOrEmpty(table_name)) {
            rawSchemaObj.put("table_name", table_name);
        }
        // replace table type
        if (!Strings.isNullOrEmpty(table_type)) {
            rawSchemaObj.put("table_type", table_type);
        }
        return JsonPrettyFormatter.toString(rawSchemaObj);
    }
}
