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

package org.havenask.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.havenask.HavenaskException;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.lucene.ScorerAware;
import org.havenask.index.fielddata.ScriptDocValues;
import org.havenask.search.lookup.LeafSearchLookup;
import org.havenask.search.lookup.SearchLookup;
import org.havenask.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class AggregationScript implements ScorerAware {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggs", Factory.class);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = org.havenask.common.collect.Map.of(
            "doc", value -> {
                deprecationLogger.deprecate("aggregation-script_doc",
                        "Accessing variable [doc] via [params.doc] from within an aggregation-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_doc", value -> {
                deprecationLogger.deprecate("aggregation-script__doc",
                        "Accessing variable [doc] via [params._doc] from within an aggregation-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_source", value -> ((SourceLookup)value).loadSourceIfNeeded()
    );

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /**
     * A leaf lookup for the bound segment this script will operate on.
     */
    private final LeafSearchLookup leafLookup;

    /**
     * A scorer that will return the score for the current document when the script is run.
     */
    protected Scorable scorer;

    private Object value;

    public AggregationScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        this.params = new DynamicMap(new HashMap<>(params), PARAMS_FUNCTIONS);
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        this.params.putAll(leafLookup.asMap());
    }

    protected AggregationScript() {
        params = null;
        leafLookup = null;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * The doc lookup for the Lucene segment this script was created for.
     */
    public Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /**
     * Set the current document to run the script on next.
     */
    public void setDocument(int docid) {
        leafLookup.setDocument(docid);
    }

    @Override
    public void setScorer(Scorable scorer) {
        this.scorer = scorer;
    }

    /**
     * Sets per-document aggregation {@code _value}.
     * <p>
     * The default implementation just calls {@code setNextVar("_value", value)} but
     * some engines might want to handle this differently for better performance.
     * <p>
     * @param value per-document value, typically a String, Long, or Double
     */
    public void setNextAggregationValue(Object value) {
        this.value = value;
    }

    public Number get_score() {
        try {
            return scorer == null ? 0.0 : scorer.score();
        } catch (IOException e) {
            throw new HavenaskException("couldn't lookup score", e);
        }
    }

    public Object get_value() {
        return value;
    }

    /**
     * Return the result as a long. This is used by aggregation scripts over long fields.
     */
    public long runAsLong() {
        return ((Number) execute()).longValue();
    }

    public double runAsDouble() {
        return ((Number) execute()).doubleValue();
    }

    public abstract Object execute();

    /**
     * A factory to construct {@link AggregationScript} instances.
     */
    public interface LeafFactory {
        AggregationScript newInstance(LeafReaderContext ctx) throws IOException;

        /**
         * Return {@code true} if the script needs {@code _score} calculated, or {@code false} otherwise.
         */
        boolean needs_score();
    }

    /**
     * A factory to construct stateful {@link AggregationScript} factories for a specific index.
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
