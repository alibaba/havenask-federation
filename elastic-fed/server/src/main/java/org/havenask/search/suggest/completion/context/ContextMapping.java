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

package org.havenask.search.suggest.completion.context;

import org.havenask.HavenaskParseException;
import org.havenask.Version;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParser.Token;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.index.mapper.CompletionFieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.ParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link ContextMapping} defines criteria that can be used to
 * filter and/or boost suggestions at query time for {@link CompletionFieldMapper}.
 *
 * Implementations have to define how contexts are parsed at query/index time
 */
public abstract class ContextMapping<T extends ToXContent> implements ToXContentFragment {

    public static final String FIELD_TYPE = "type";
    public static final String FIELD_NAME = "name";
    protected final Type type;
    protected final String name;

    public enum Type {
        CATEGORY, GEO;

        public static Type fromString(String type) {
            if (type.equalsIgnoreCase("category")) {
                return CATEGORY;
            } else if (type.equalsIgnoreCase("geo")) {
                return GEO;
            } else {
                throw new IllegalArgumentException("No context type for [" + type + "]");
            }
        }
    }

    /**
     * Define a new context mapping of a specific type
     *
     * @param type type of context mapping, either {@link Type#CATEGORY} or {@link Type#GEO}
     * @param name name of context mapping
     */
    protected ContextMapping(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    /**
     * @return the type name of the context
     */
    public Type type() {
        return type;
    }

    /**
     * @return the name/id of the context
     */
    public String name() {
        return name;
    }

    /**
     * Parses a set of index-time contexts.
     */
    public abstract Set<String> parseContext(ParseContext parseContext, XContentParser parser)
            throws IOException, HavenaskParseException;

    /**
     * Retrieves a set of context from a <code>document</code> at index-time.
     */
    protected abstract Set<String> parseContext(ParseContext.Document document);

    /**
     * Prototype for the query context
     */
    protected abstract T fromXContent(XContentParser context) throws IOException;

    /**
     * Parses query contexts for this mapper
     */
    public final List<InternalQueryContext> parseQueryContext(XContentParser parser) throws IOException, HavenaskParseException {
        List<T> queryContexts = new ArrayList<>();
        Token token = parser.nextToken();
        if (token == Token.START_ARRAY) {
            while (parser.nextToken() != Token.END_ARRAY) {
                queryContexts.add(fromXContent(parser));
            }
        } else {
            queryContexts.add(fromXContent(parser));
        }

        return toInternalQueryContexts(queryContexts);
    }

    /**
     * Convert query contexts to common representation
     */
    protected abstract List<InternalQueryContext> toInternalQueryContexts(List<T> queryContexts);

    /**
     * Implementations should add specific configurations
     * that need to be persisted
     */
    protected abstract XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Checks if the current context is consistent with the rest of the fields. For example, the GeoContext
     * should check that the field that it points to has the correct type.
     */
    public void validateReferences(Version indexVersionCreated, Function<String, MappedFieldType> fieldResolver) {
        // No validation is required by default
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_NAME, name);
        builder.field(FIELD_TYPE, type.name());
        toInnerXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContextMapping<?> that = (ContextMapping<?>) o;
        if (type != that.type) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public String toString() {
        try {
            return Strings.toString(toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS));
        } catch (IOException e) {
            return super.toString();
        }
    }

    public static class InternalQueryContext {
        public final String context;
        public final int boost;
        public final boolean isPrefix;

        public InternalQueryContext(String context, int boost, boolean isPrefix) {
            this.context = context;
            this.boost = boost;
            this.isPrefix = isPrefix;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InternalQueryContext that = (InternalQueryContext) o;

            if (boost != that.boost) return false;
            if (isPrefix != that.isPrefix) return false;
            return context != null ? context.equals(that.context) : that.context == null;

        }

        @Override
        public int hashCode() {
            int result = context != null ? context.hashCode() : 0;
            result = 31 * result + boost;
            result = 31 * result + (isPrefix ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "QueryContext{" +
                    "context='" + context + '\'' +
                    ", boost=" + boost +
                    ", isPrefix=" + isPrefix +
                    '}';
        }
    }
}
