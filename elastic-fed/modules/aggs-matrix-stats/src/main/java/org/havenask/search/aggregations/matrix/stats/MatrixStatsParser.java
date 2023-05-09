/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.search.aggregations.matrix.stats;

import org.havenask.common.ParseField;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.search.MultiValueMode;
import org.havenask.search.aggregations.support.ValueType;
import org.havenask.search.aggregations.support.ValuesSourceType;
import org.havenask.search.aggregations.support.ArrayValuesSourceAggregationBuilder;
import org.havenask.search.aggregations.support.ArrayValuesSourceParser;

import java.io.IOException;
import java.util.Map;

public class MatrixStatsParser extends ArrayValuesSourceParser.NumericValuesSourceParser {

    public MatrixStatsParser() {
        super(true);
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser,
                            Map<ParseField, Object> otherOptions) throws IOException {
        if (ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            if (token == XContentParser.Token.VALUE_STRING) {
                otherOptions.put(ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD, parser.text());
                return true;
            }
        }
        return false;
    }

    @Override
    protected MatrixStatsAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                          ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        MatrixStatsAggregationBuilder builder = new MatrixStatsAggregationBuilder(aggregationName);
        String mode = (String)otherOptions.get(ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD);
        if (mode != null) {
            builder.multiValueMode(MultiValueMode.fromString(mode));
        }
        return builder;
    }
}
