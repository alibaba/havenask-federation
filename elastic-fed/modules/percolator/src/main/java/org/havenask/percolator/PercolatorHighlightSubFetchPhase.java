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

package org.havenask.percolator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.document.DocumentField;
import org.havenask.common.text.Text;
import org.havenask.search.SearchHit;
import org.havenask.search.fetch.FetchContext;
import org.havenask.search.fetch.FetchSubPhase;
import org.havenask.search.fetch.FetchSubPhaseProcessor;
import org.havenask.search.fetch.subphase.highlight.HighlightField;
import org.havenask.search.fetch.subphase.highlight.HighlightPhase;
import org.havenask.search.fetch.subphase.highlight.Highlighter;
import org.havenask.search.fetch.subphase.highlight.SearchHighlightContext;
import org.havenask.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Highlighting in the case of the percolate query is a bit different, because the PercolateQuery itself doesn't get highlighted,
 * but the source of the PercolateQuery gets highlighted by each hit containing a query.
 */
final class PercolatorHighlightSubFetchPhase implements FetchSubPhase {
    private final HighlightPhase highlightPhase;

    PercolatorHighlightSubFetchPhase(Map<String, Highlighter> highlighters) {
        this.highlightPhase = new HighlightPhase(highlighters);
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        if (fetchContext.highlight() == null) {
            return null;
        }
        List<PercolateQuery> percolateQueries = locatePercolatorQuery(fetchContext.query());
        if (percolateQueries.isEmpty()) {
            return null;
        }
        return new FetchSubPhaseProcessor() {

            LeafReaderContext ctx;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                this.ctx = readerContext;
            }

            @Override
            public void process(HitContext hit) throws IOException {
                boolean singlePercolateQuery = percolateQueries.size() == 1;
                for (PercolateQuery percolateQuery : percolateQueries) {
                    String fieldName = singlePercolateQuery ? PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX :
                        PercolatorMatchedSlotSubFetchPhase.FIELD_NAME_PREFIX + "_" + percolateQuery.getName();
                    IndexSearcher percolatorIndexSearcher = percolateQuery.getPercolatorIndexSearcher();
                    PercolateQuery.QueryStore queryStore = percolateQuery.getQueryStore();

                    LeafReaderContext percolatorLeafReaderContext = percolatorIndexSearcher.getIndexReader().leaves().get(0);
                    final Query query = queryStore.getQueries(ctx).apply(hit.docId());
                    if (query != null) {
                        DocumentField field = hit.hit().field(fieldName);
                        if (field == null) {
                            // It possible that a hit did not match with a particular percolate query,
                            // so then continue highlighting with the next hit.
                            continue;
                        }

                        for (Object matchedSlot : field.getValues()) {
                            int slot = (int) matchedSlot;
                            BytesReference document = percolateQuery.getDocuments().get(slot);
                            HitContext subContext = new HitContext(
                                new SearchHit(
                                    slot,
                                    "unknown",
                                    new Text(hit.hit().getType()),
                                    Collections.emptyMap(),
                                    Collections.emptyMap()
                                ),
                                percolatorLeafReaderContext,
                                slot,
                                new SourceLookup());
                            subContext.sourceLookup().setSource(document);
                            // force source because MemoryIndex does not store fields
                            SearchHighlightContext highlight = new SearchHighlightContext(fetchContext.highlight().fields(), true);
                            FetchSubPhaseProcessor processor = highlightPhase.getProcessor(fetchContext, highlight, query);
                            processor.process(subContext);
                            for (Map.Entry<String, HighlightField> entry : subContext.hit().getHighlightFields().entrySet()) {
                                if (percolateQuery.getDocuments().size() == 1) {
                                    String hlFieldName;
                                    if (singlePercolateQuery) {
                                        hlFieldName = entry.getKey();
                                    } else {
                                        hlFieldName = percolateQuery.getName() + "_" + entry.getKey();
                                    }
                                    hit.hit().getHighlightFields().put(hlFieldName,
                                        new HighlightField(hlFieldName, entry.getValue().fragments()));
                                } else {
                                    // In case multiple documents are being percolated we need to identify to which document
                                    // a highlight belongs to.
                                    String hlFieldName;
                                    if (singlePercolateQuery) {
                                        hlFieldName = slot + "_" + entry.getKey();
                                    } else {
                                        hlFieldName = percolateQuery.getName() + "_" + slot + "_" + entry.getKey();
                                    }
                                    hit.hit().getHighlightFields().put(hlFieldName,
                                        new HighlightField(hlFieldName, entry.getValue().fragments()));
                                }
                            }
                        }
                    }
                }
            }
        };
    }

    static List<PercolateQuery> locatePercolatorQuery(Query query) {
        if (query == null) {
            return Collections.emptyList();
        }
        List<PercolateQuery> queries = new ArrayList<>();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query query) {
                if (query instanceof PercolateQuery) {
                    queries.add((PercolateQuery)query);
                }
            }
        });
        return queries;
    }
}
