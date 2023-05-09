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
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.index.mapper.DocValueFetcher;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.ValueFetcher;
import org.havenask.search.fetch.FetchContext;
import org.havenask.search.fetch.FetchSubPhase;
import org.havenask.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetch sub phase which pulls data from doc values.
 *
 * Specifying {@code "docvalue_fields": ["field1", "field2"]}
 */
public final class FetchDocValuesPhase implements FetchSubPhase {

    private static final String USE_DEFAULT_FORMAT = "use_field_mapping";
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(FetchDocValuesPhase.class);

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        FetchDocValuesContext dvContext = context.docValuesContext();
        if (dvContext == null) {
            return null;
        }

        if (context.docValuesContext().fields().stream()
                .map(f -> f.format)
                .anyMatch(USE_DEFAULT_FORMAT::equals)) {
            DEPRECATION_LOGGER.deprecate("explicit_default_format",
                    "[" + USE_DEFAULT_FORMAT + "] is a special format that was only used to " +
                    "ease the transition to 7.x. It has become the default and shouldn't be set explicitly anymore.");
        }

        /*
         * Its tempting to swap this to a `Map` but that'd break backwards
         * compatibility because we support fetching the same field multiple
         * times with different configuration. That isn't possible with a `Map`.
         */
        List<DocValueField> fields = new ArrayList<>();
        for (FieldAndFormat fieldAndFormat : context.docValuesContext().fields()) {
            MappedFieldType ft = context.mapperService().fieldType(fieldAndFormat.field);
            if (ft == null) {
                continue;
            }
            String format = USE_DEFAULT_FORMAT.equals(fieldAndFormat.format) ? null : fieldAndFormat.format;
            ValueFetcher fetcher = new DocValueFetcher(
                ft.docValueFormat(format, null),
                context.searchLookup().doc().getForField(ft)
            );
            fields.add(new DocValueField(fieldAndFormat.field, fetcher));
        }

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                for (DocValueField f : fields) {
                    f.fetcher.setNextReader(readerContext);
                }
            }

            @Override
            public void process(HitContext hit) throws IOException {
                for (DocValueField f : fields) {
                    DocumentField hitField = hit.hit().field(f.field);
                    if (hitField == null) {
                        hitField = new DocumentField(f.field, new ArrayList<>(2));
                        // even if we request a doc values of a meta-field (e.g. _routing),
                        // docValues fields will still be document fields, and put under "fields" section of a hit.
                        hit.hit().setDocumentField(f.field, hitField);
                    }
                    hitField.getValues().addAll(f.fetcher.fetchValues(hit.sourceLookup()));
                }
            }
        };
    }

    private static class DocValueField {
        private final String field;
        private final ValueFetcher fetcher;

        DocValueField(String field, ValueFetcher fetcher) {
            this.field = field;
            this.fetcher = fetcher;
        }
    }
}
