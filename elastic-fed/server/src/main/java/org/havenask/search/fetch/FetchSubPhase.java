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

package org.havenask.search.fetch;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.havenask.search.SearchHit;
import org.havenask.search.lookup.SourceLookup;

import java.io.IOException;

/**
 * Sub phase within the fetch phase used to fetch things *about* the documents like highlighting or matched queries.
 */
public interface FetchSubPhase {

    class HitContext {
        private final SearchHit hit;
        private final LeafReaderContext readerContext;
        private final int docId;
        private final SourceLookup sourceLookup;

        public HitContext(
            SearchHit hit,
            LeafReaderContext context,
            int docId,
            SourceLookup sourceLookup
        ) {
            this.hit = hit;
            this.readerContext = context;
            this.docId = docId;
            this.sourceLookup = sourceLookup;
            sourceLookup.setSegmentAndDocument(context, docId);
        }

        public SearchHit hit() {
            return hit;
        }

        public LeafReader reader() {
            return readerContext.reader();
        }

        public LeafReaderContext readerContext() {
            return readerContext;
        }

        /**
         * @return the docId of this hit relative to the leaf reader context
         */
        public int docId() {
            return docId;
        }

        /**
         * This lookup provides access to the source for the given hit document. Note
         * that it should always be set to the correct doc ID and {@link LeafReaderContext}.
         *
         * In most cases, the hit document's source is loaded eagerly at the start of the
         * {@link FetchPhase}. This lookup will contain the preloaded source.
         */
        public SourceLookup sourceLookup() {
            return sourceLookup;
        }

        public IndexReader topLevelReader() {
            return ReaderUtil.getTopLevelContext(readerContext).reader();
        }
    }

    /**
     * Returns a {@link FetchSubPhaseProcessor} for this sub phase.
     *
     * If nothing should be executed for the provided {@code FetchContext}, then the
     * implementation should return {@code null}
     */
    FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException;
}
