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

import org.apache.lucene.search.Scorable;
import org.havenask.search.aggregations.LeafBucketCollector;
import org.havenask.search.profile.Timer;

import java.io.IOException;

public class ProfilingLeafBucketCollector extends LeafBucketCollector {

    private LeafBucketCollector delegate;
    private Timer collectTimer;

    public ProfilingLeafBucketCollector(LeafBucketCollector delegate, AggregationProfileBreakdown profileBreakdown) {
        this.delegate = delegate;
        this.collectTimer = profileBreakdown.getTimer(AggregationTimingType.COLLECT);
    }

    @Override
    public void collect(int doc, long bucket) throws IOException {
        collectTimer.start();
        try {
            delegate.collect(doc, bucket);
        } finally {
            collectTimer.stop();
        }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        delegate.setScorer(scorer);
    }

}
