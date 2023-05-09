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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.search.fetch;

import org.apache.lucene.search.Query;
import org.havenask.index.IndexSettings;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.query.ParsedQuery;
import org.havenask.search.SearchExtBuilder;
import org.havenask.search.fetch.subphase.FetchDocValuesContext;
import org.havenask.search.fetch.subphase.FetchFieldsContext;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.search.fetch.subphase.FieldAndFormat;
import org.havenask.search.fetch.subphase.InnerHitsContext;
import org.havenask.search.fetch.subphase.InnerHitsContext.InnerHitSubContext;
import org.havenask.search.fetch.subphase.ScriptFieldsContext;
import org.havenask.search.fetch.subphase.highlight.SearchHighlightContext;
import org.havenask.search.internal.ContextIndexSearcher;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.lookup.SearchLookup;
import org.havenask.search.lookup.SourceLookup;
import org.havenask.search.rescore.RescoreContext;

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates state required to execute fetch phases
 */
public class FetchContext {

    private final SearchContext searchContext;
    private final SearchLookup searchLookup;

    /**
     * Create a FetchContext based on a SearchContext
     */
    public FetchContext(SearchContext searchContext) {
        this.searchContext = searchContext;
        this.searchLookup = searchContext.getQueryShardContext().newFetchLookup();
    }

    /**
     * The name of the index that documents are being fetched from
     */
    public String getIndexName() {
        return searchContext.indexShard().shardId().getIndexName();
    }

    /**
     * The point-in-time searcher the original query was executed against
     */
    public ContextIndexSearcher searcher() {
        return searchContext.searcher();
    }

    /**
     * The mapper service for the index we are fetching documents from
     */
    public MapperService mapperService() {
        return searchContext.mapperService();
    }

    /**
     * The index settings for the index we are fetching documents from
     */
    public IndexSettings getIndexSettings() {
        return mapperService().getIndexSettings();
    }

    /**
     * The {@code SearchLookup} for the this context
     */
    public SearchLookup searchLookup() {
        return searchLookup;
    }

    /**
     * The original query
     */
    public Query query() {
        return searchContext.query();
    }

    /**
     * The original query with additional filters and named queries
     */
    public ParsedQuery parsedQuery() {
        return searchContext.parsedQuery();
    }

    /**
     * Any post-filters run as part of the search
     */
    public ParsedQuery parsedPostFilter() {
        return searchContext.parsedPostFilter();
    }

    /**
     * Configuration for fetching _source
     */
    public FetchSourceContext fetchSourceContext() {
        return searchContext.fetchSourceContext();
    }

    /**
     * Should the response include `explain` output
     */
    public boolean explain() {
        return searchContext.explain() && searchContext.query() != null;
    }

    /**
     * The rescorers included in the original search, used for explain output
     */
    public List<RescoreContext> rescore() {
        return searchContext.rescore();
    }

    /**
     * Should the response include sequence number and primary term metadata
     */
    public boolean seqNoAndPrimaryTerm() {
        return searchContext.seqNoAndPrimaryTerm();
    }

    /**
     * Configuration for fetching docValues fields
     */
    public FetchDocValuesContext docValuesContext() {
        FetchDocValuesContext dvContext = searchContext.docValuesContext();
        if (searchContext.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = searchContext.collapse().getFieldName();
            if (dvContext == null) {
                return new FetchDocValuesContext(Collections.singletonList(new FieldAndFormat(name, null)));
            } else if (searchContext.docValuesContext().fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                dvContext.fields().add(new FieldAndFormat(name, null));
            }
        }
        return dvContext;
    }

    /**
     * Configuration for highlighting
     */
    public SearchHighlightContext highlight() {
        return searchContext.highlight();
    }

    /**
     * Should the response include scores, even if scores were not calculated in the original query
     */
    public boolean fetchScores() {
        return searchContext.sort() != null && searchContext.trackScores();
    }

    /**
     * Configuration for returning inner hits
     */
    public InnerHitsContext innerHits() {
        return searchContext.innerHits();
    }

    /**
     * Should the response include version metadata
     */
    public boolean version() {
        return searchContext.version();
    }

    /**
     * Configuration for the 'fields' response
     */
    public FetchFieldsContext fetchFieldsContext() {
        return searchContext.fetchFieldsContext();
    }

    /**
     * Configuration for script fields
     */
    public ScriptFieldsContext scriptFields() {
        return searchContext.scriptFields();
    }

    /**
     * Configuration for external fetch phase plugins
     */
    public SearchExtBuilder getSearchExt(String name) {
        return searchContext.getSearchExt(name);
    }

    /**
     * For a hit document that's being processed, return the source lookup representing the
     * root document. This method is used to pass down the root source when processing this
     * document's nested inner hits.
     *
     * @param hitContext The context of the hit that's being processed.
     */
    public SourceLookup getRootSourceLookup(FetchSubPhase.HitContext hitContext) {
        // Usually the root source simply belongs to the hit we're processing. But if
        // there are multiple layers of inner hits and we're in a nested context, then
        // the root source is found on the inner hits context.
        if (searchContext instanceof InnerHitSubContext && hitContext.hit().getNestedIdentity() != null) {
            InnerHitSubContext innerHitsContext = (InnerHitSubContext) searchContext;
            return innerHitsContext.getRootLookup();
        } else {
            return hitContext.sourceLookup();
        }
    }
}
