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

package org.havenask.search.fetch.subphase.highlight;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

public class SourceSimpleFragmentsBuilder extends SimpleFragmentsBuilder {

    private final SourceLookup sourceLookup;

    public SourceSimpleFragmentsBuilder(MappedFieldType fieldType,
                                        SourceLookup sourceLookup,
                                        String[] preTags,
                                        String[] postTags,
                                        BoundaryScanner boundaryScanner) {
        super(fieldType, preTags, postTags, boundaryScanner);
        this.sourceLookup = sourceLookup;
    }

    public static final Field[] EMPTY_FIELDS = new Field[0];

    @Override
    protected Field[] getFields(IndexReader reader, int docId, String fieldName) throws IOException {
        // we know its low level reader, and matching docId, since that's how we call the highlighter with
        List<Object> values = sourceLookup.extractRawValues(fieldType.name());
        if (values.isEmpty()) {
            return EMPTY_FIELDS;
        }
        Field[] fields = new Field[values.size()];
        for (int i = 0; i < values.size(); i++) {
            fields[i] = new Field(fieldType.name(), values.get(i).toString(), TextField.TYPE_NOT_STORED);
        }
        return fields;
    }

}
