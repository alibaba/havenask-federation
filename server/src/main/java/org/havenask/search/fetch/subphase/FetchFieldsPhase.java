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

package org.havenask.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.havenask.common.document.DocumentField;
import org.havenask.index.mapper.IgnoredFieldMapper;
import org.havenask.index.mapper.MapperService;
import org.havenask.search.SearchHit;
import org.havenask.search.fetch.FetchContext;
import org.havenask.search.fetch.FetchSubPhase;
import org.havenask.search.fetch.FetchSubPhaseProcessor;
import org.havenask.search.lookup.SearchLookup;
import org.havenask.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values from _source and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchFieldsContext fetchFieldsContext = fetchContext.fetchFieldsContext();
        if (fetchFieldsContext == null) {
            return null;
        }

        MapperService mapperService = fetchContext.mapperService();
        SearchLookup searchLookup = fetchContext.searchLookup();
        if (fetchContext.mapperService().documentMapper().sourceMapper().enabled() == false) {
            throw new IllegalArgumentException("Unable to retrieve the requested [fields] since _source is disabled " +
                "in the mappings for index [" + fetchContext.getIndexName() + "]");
        }

        FieldFetcher fieldFetcher = FieldFetcher.create(mapperService, searchLookup, fetchFieldsContext.fields());
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                fieldFetcher.setNextReader(readerContext);
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                SourceLookup sourceLookup = hitContext.sourceLookup();

                Set<String> ignoredFields = getIgnoredFields(hit);
                Map<String, DocumentField> documentFields = fieldFetcher.fetch(sourceLookup, ignoredFields);
                for (Map.Entry<String, DocumentField> entry : documentFields.entrySet()) {
                    hit.setDocumentField(entry.getKey(), entry.getValue());
                }
            }
        };
    }

    private Set<String> getIgnoredFields(SearchHit hit) {
        DocumentField field = hit.field(IgnoredFieldMapper.NAME);
        if (field == null) {
            return org.havenask.common.collect.Set.of();
        }

        Set<String> ignoredFields = new HashSet<>();
        for (Object value : field.getValues()) {
            ignoredFields.add((String) value);
        }
        return ignoredFields;
    }
}
