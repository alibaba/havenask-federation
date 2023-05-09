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

package org.havenask.search.aggregations.bucket.terms;

import org.apache.lucene.index.IndexReader;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.BucketOrder;
import org.havenask.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.havenask.search.internal.ContextIndexSearcher;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyList;

abstract class AbstractStringTermsAggregator extends TermsAggregator {

    protected final boolean showTermDocCountError;

    AbstractStringTermsAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
            BucketOrder order, DocValueFormat format, BucketCountThresholds bucketCountThresholds, SubAggCollectionMode subAggCollectMode,
            boolean showTermDocCountError, Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.showTermDocCountError = showTermDocCountError;
    }

    protected StringTerms buildEmptyTermsAggregation() {
        return new StringTerms(name, order, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, 0, emptyList(), 0);
    }

    protected SignificantStringTerms buildEmptySignificantTermsAggregation(long subsetSize, SignificanceHeuristic significanceHeuristic) {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, subsetSize, supersetSize, significanceHeuristic, emptyList());
    }
}
