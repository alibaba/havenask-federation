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

package org.havenask.engine.index.query;

import org.apache.lucene.search.Query;
import org.havenask.common.ParseField;
import org.havenask.common.ParsingException;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.query.AbstractQueryBuilder;
import org.havenask.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class KnnQueryBuilder extends ProximaQueryBuilder<KnnQueryBuilder> {

    public static final ParseField EF_FIELD = new ParseField("ef");
    public static final ParseField MAX_SCAN_NUM_FIELD = new ParseField("max_scan_num");

    public static final String NAME = "knn";

    private final Integer ef;
    private final Integer maxScanNum;

    public KnnQueryBuilder(String fieldName, float[] vector, int size) {
        this(fieldName, vector, size, null, null, null);
    }

    public KnnQueryBuilder(String fieldName, float[] vector, int size, SearchFilter searchFilter, Integer ef, Integer maxScanNum) {
        super(fieldName, vector, size, searchFilter);
        this.ef = ef;
        this.maxScanNum = maxScanNum;
    }

    public KnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.ef = in.readOptionalVInt();
        this.maxScanNum = in.readOptionalVInt();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void innerDoWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(ef);
        out.writeOptionalVInt(maxScanNum);
    }

    @Override
    protected void innerDoXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(EF_FIELD.getPreferredName(), ef);
        builder.field(MAX_SCAN_NUM_FIELD.getPreferredName(), maxScanNum);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new IllegalArgumentException("field: " + fieldName + " is not exist");
        }
        DenseVectorFieldMapper.DenseVectorFieldType mapper = (DenseVectorFieldMapper.DenseVectorFieldType) fieldType;
        // TODO fix it
        return new KnnQuery(fieldName, vector, size, searchFilter, ef, maxScanNum);
    }

    @Override
    protected boolean innerDoEquals(KnnQueryBuilder other) {
        return Objects.equals(ef, other.ef) && Objects.equals(maxScanNum, other.maxScanNum);
    }

    @Override
    protected int innerDoHashCode() {
        return Objects.hash(ef, maxScanNum);
    }

    public Integer getEf() {
        return ef;
    }

    public Integer getMaxScanNum() {
        return maxScanNum;
    }

    public static KnnQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        List<Object> vector = null;
        int size = 0;
        SearchFilter searchFilter = null;
        Integer ef = null;
        Integer maxScanNum = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (VECTOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            vector = new ArrayList<>();
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                vector.add(parseVectorValue(parser));
                            }
                        }
                    } else if (token.isValue() || token == XContentParser.Token.VALUE_NULL) {
                        if (SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            size = parser.intValue();
                        } else if (SEARCH_FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            if (token != XContentParser.Token.VALUE_NULL) {
                                // TODO filter
                            }
                        } else if (EF_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            if (token != XContentParser.Token.VALUE_NULL) {
                                ef = parser.intValue();
                            }
                        } else if (MAX_SCAN_NUM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            if (token != XContentParser.Token.VALUE_NULL) {
                                maxScanNum = parser.intValue();
                            }
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                vector = parser.list();
            }
        }

        if (vector == null || vector.isEmpty()) {
            throw new IllegalArgumentException("vector can not be empty");
        }

        float[] array = new float[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            array[i] = ((Number) vector.get(i)).floatValue();
        }
        // float[] array = vector.toArray(); // TODO: avoid arrayCopy?
        KnnQueryBuilder graphQuery = new KnnQueryBuilder(fieldName, array, size, searchFilter, ef, maxScanNum);
        graphQuery.queryName(queryName);
        graphQuery.boost(boost);

        return graphQuery;
    }
}
