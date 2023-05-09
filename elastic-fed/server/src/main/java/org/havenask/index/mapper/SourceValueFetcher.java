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

package org.havenask.index.mapper;

import org.havenask.common.Nullable;
import org.havenask.search.lookup.SourceLookup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * An implementation of {@link ValueFetcher} that knows how to extract values
 * from the document source. Most standard field mappers will use this class
 * to implement value fetching.
 *
 * Field types that handle arrays directly should instead use {@link ArraySourceValueFetcher}.
 */
public abstract class SourceValueFetcher implements ValueFetcher {
    private final Set<String> sourcePaths;
    private final @Nullable Object nullValue;

    public SourceValueFetcher(String fieldName, MapperService mapperService) {
        this(fieldName, mapperService, null);
    }

    /**
     * @param fieldName The name of the field.
     * @param mapperService A mapper service.
     * @param nullValue A optional substitute value if the _source value is 'null'.
     */
    public SourceValueFetcher(String fieldName, MapperService mapperService, Object nullValue) {
        this.sourcePaths = mapperService.sourcePath(fieldName);
        this.nullValue = nullValue;
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup) {
        List<Object> values = new ArrayList<>();
        for (String path : sourcePaths) {
            Object sourceValue = lookup.extractValue(path, nullValue);
            if (sourceValue == null) {
                return org.havenask.common.collect.List.of();
            }

            // We allow source values to contain multiple levels of arrays, such as `"field": [[1, 2]]`.
            // So we need to unwrap these arrays before passing them on to be parsed.
            Queue<Object> queue = new ArrayDeque<>();
            queue.add(sourceValue);
            while (queue.isEmpty() == false) {
                Object value = queue.poll();
                if (value instanceof List) {
                    queue.addAll((List<?>) value);
                } else {
                    Object parsedValue = parseSourceValue(value);
                    if (parsedValue != null) {
                        values.add(parsedValue);
                    }
                }
            }
        }
        return values;
    }

    /**
     * Given a value that has been extracted from a document's source, parse it into a standard
     * format. This parsing logic should closely mirror the value parsing in
     * {@link FieldMapper#parseCreateField} or {@link FieldMapper#parse}.
     */
    protected abstract Object parseSourceValue(Object value);

    /**
     * Creates a {@link SourceValueFetcher} that passes through source values unmodified.
     */
    public static SourceValueFetcher identity(String fieldName, MapperService mapperService, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + fieldName + "] doesn't support formats.");
        }
        return new SourceValueFetcher(fieldName, mapperService) {
            @Override
            protected Object parseSourceValue(Object value) {
                return value;
            }
        };
    }

    /**
     * Creates a {@link SourceValueFetcher} that converts source values to strings.
     */
    public static SourceValueFetcher toString(String fieldName, MapperService mapperService, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + fieldName + "] doesn't support formats.");
        }
        return new SourceValueFetcher(fieldName, mapperService) {
            @Override
            protected Object parseSourceValue(Object value) {
                return value.toString();
            }
        };
    }
}
