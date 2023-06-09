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

package org.havenask.search.aggregations.matrix.spi;

import org.havenask.common.ParseField;
import org.havenask.common.xcontent.ContextParser;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.plugins.spi.NamedXContentProvider;
import org.havenask.search.aggregations.Aggregation;
import org.havenask.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.havenask.search.aggregations.matrix.stats.ParsedMatrixStats;

import java.util.List;

import static java.util.Collections.singletonList;

public class MatrixStatsNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        ParseField parseField = new ParseField(MatrixStatsAggregationBuilder.NAME);
        ContextParser<Object, Aggregation> contextParser = (p, name) -> ParsedMatrixStats.fromXContent(p, (String) name);
        return singletonList(new NamedXContentRegistry.Entry(Aggregation.class, parseField, contextParser));
    }
}
