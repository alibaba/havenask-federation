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

package org.havenask.search.profile.aggregation;

import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.profile.AbstractProfiler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AggregationProfiler extends AbstractProfiler<AggregationProfileBreakdown, Aggregator> {

    private final Map<List<String>, AggregationProfileBreakdown> profileBreakdownLookup = new HashMap<>();

    public AggregationProfiler() {
        super(new InternalAggregationProfileTree());
    }

    @Override
    public AggregationProfileBreakdown getQueryBreakdown(Aggregator agg) {
        List<String> path = getAggregatorPath(agg);
        AggregationProfileBreakdown aggregationProfileBreakdown = profileBreakdownLookup.get(path);
        if (aggregationProfileBreakdown == null) {
            aggregationProfileBreakdown = super.getQueryBreakdown(agg);
            profileBreakdownLookup.put(path, aggregationProfileBreakdown);
        }
        return aggregationProfileBreakdown;
    }

    public static List<String> getAggregatorPath(Aggregator agg) {
        LinkedList<String> path = new LinkedList<>();
        while (agg != null) {
            path.addFirst(agg.name());
            agg = agg.parent();
        }
        return path;
    }
}
