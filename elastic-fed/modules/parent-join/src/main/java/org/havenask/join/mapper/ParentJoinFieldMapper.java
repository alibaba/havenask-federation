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

package org.havenask.join.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.havenask.common.lucene.Lucene;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.support.XContentMapValues;
import org.havenask.index.IndexSettings;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.havenask.index.mapper.ContentPath;
import org.havenask.index.mapper.DocumentMapper;
import org.havenask.index.mapper.FieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.Mapper;
import org.havenask.index.mapper.MapperParsingException;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MappingLookup;
import org.havenask.index.mapper.ParseContext;
import org.havenask.index.mapper.SourceValueFetcher;
import org.havenask.index.mapper.StringFieldType;
import org.havenask.index.mapper.TextSearchInfo;
import org.havenask.index.mapper.ValueFetcher;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} that creates hierarchical joins (parent-join) between documents in the same index.
 * Only one parent-join field can be defined per index. The verification of this assumption is done
 * through the {@link MetaJoinFieldMapper} which declares a meta field called "_parent_join".
 * This field is only used to ensure that there is a single parent-join field defined in the mapping and
 * cannot be used to index or query any data.
 */
public final class ParentJoinFieldMapper extends FieldMapper {
    public static final String NAME = "join";
    public static final String CONTENT_TYPE = "join";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * Returns the {@link ParentJoinFieldMapper} associated with the <code>service</code> or null
     * if there is no parent-join field in this mapping.
     */
    public static ParentJoinFieldMapper getMapper(MapperService service) {
        MetaJoinFieldMapper.MetaJoinFieldType fieldType =
            (MetaJoinFieldMapper.MetaJoinFieldType) service.fieldType(MetaJoinFieldMapper.NAME);
        if (fieldType == null) {
            return null;
        }
        DocumentMapper mapper = service.documentMapper();
        String joinField = fieldType.getJoinField();
        MappingLookup fieldMappers = mapper.mappers();
        return (ParentJoinFieldMapper) fieldMappers.getMapper(joinField);
    }

    private static String getParentIdFieldName(String joinFieldName, String parentName) {
        return joinFieldName + "#" + parentName;
    }

    private static void checkIndexCompatibility(IndexSettings settings, String name) {
        if (settings.getIndexMetadata().isRoutingPartitionedIndex()) {
            throw new IllegalStateException("cannot create join field [" + name + "] " +
                "for the partitioned index " + "[" + settings.getIndex().getName() + "]");
        }
    }

    private static void checkObjectOrNested(ContentPath path, String name) {
        if (path.pathAsText(name).contains(".")) {
            throw new IllegalArgumentException("join field [" + path.pathAsText(name) + "] " +
                "cannot be added inside an object or in a multi-field");
        }
    }

    private static void checkParentFields(String name, List<ParentIdFieldMapper> mappers) {
        Set<String> children = new HashSet<>();
        List<String> conflicts = new ArrayList<>();
        for (ParentIdFieldMapper mapper : mappers) {
            for (String child : mapper.getChildren()) {
                if (children.add(child) == false) {
                    conflicts.add("[" + child + "] cannot have multiple parents");
                }
            }
        }
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("invalid definition for join field [" + name + "]:\n" + conflicts.toString());
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {
        final List<ParentIdFieldMapper.Builder> parentIdFieldBuilders = new ArrayList<>();
        boolean eagerGlobalOrdinals = true;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder addParent(String parent, Set<String> children) {
            String parentIdFieldName = getParentIdFieldName(name, parent);
            parentIdFieldBuilders.add(new ParentIdFieldMapper.Builder(parentIdFieldName, parent, children));
            return builder;
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            return builder;
        }

        @Override
        public ParentJoinFieldMapper build(BuilderContext context) {
            checkObjectOrNested(context.path(), name);
            final List<ParentIdFieldMapper> parentIdFields = new ArrayList<>();
            parentIdFieldBuilders.stream()
                .map((parentBuilder) -> {
                    if (eagerGlobalOrdinals) {
                        parentBuilder.eagerGlobalOrdinals(true);
                    }
                    return parentBuilder.build(context);
                })
                .forEach(parentIdFields::add);
            checkParentFields(name(), parentIdFields);
            MetaJoinFieldMapper unique = new MetaJoinFieldMapper.Builder(name).build(context);
            return new ParentJoinFieldMapper(name, fieldType, new JoinFieldType(buildFullName(context), meta),
                unique, Collections.unmodifiableList(parentIdFields), eagerGlobalOrdinals);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            final IndexSettings indexSettings = parserContext.mapperService().getIndexSettings();
            checkIndexCompatibility(indexSettings, name);

            Builder builder = new Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                if ("type".equals(entry.getKey())) {
                    continue;
                }
                if ("eager_global_ordinals".equals(entry.getKey())) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(entry.getValue(), "eager_global_ordinals"));
                    iterator.remove();
                    continue;
                }
                if ("relations".equals(entry.getKey())) {
                    Map<String, Object> relations = XContentMapValues.nodeMapValue(entry.getValue(), "relations");
                    for (Iterator<Map.Entry<String, Object>> relIt = relations.entrySet().iterator(); relIt.hasNext(); ) {
                        Map.Entry<String, Object> relation = relIt.next();
                        final String parent = relation.getKey();
                        Set<String> children;
                        if (XContentMapValues.isArray(relation.getValue())) {
                            children = new HashSet<>(Arrays.asList(XContentMapValues.nodeStringArrayValue(relation.getValue())));
                        } else {
                            children = Collections.singleton(relation.getValue().toString());
                        }
                        builder.addParent(parent, children);
                    }
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class JoinFieldType extends StringFieldType {
        private JoinFieldType(String name, Map<String, String> meta) {
            super(name, true, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), mapperService, format);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }
    }

    // The meta field that ensures that there is no other parent-join in the mapping
    private MetaJoinFieldMapper uniqueFieldMapper;
    private List<ParentIdFieldMapper> parentIdFields;
    private boolean eagerGlobalOrdinals;

    protected ParentJoinFieldMapper(String simpleName,
                                    FieldType fieldType,
                                    MappedFieldType mappedFieldType,
                                    MetaJoinFieldMapper uniqueFieldMapper,
                                    List<ParentIdFieldMapper> parentIdFields,
                                    boolean eagerGlobalOrdinals) {
        super(simpleName, fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        this.parentIdFields = parentIdFields;
        this.uniqueFieldMapper = uniqueFieldMapper;
        this.eagerGlobalOrdinals = eagerGlobalOrdinals;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected ParentJoinFieldMapper clone() {
        return (ParentJoinFieldMapper) super.clone();
    }

    @Override
    public JoinFieldType fieldType() {
        return (JoinFieldType) super.fieldType();
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> mappers = new ArrayList<> (parentIdFields);
        mappers.add(uniqueFieldMapper);
        return mappers.iterator();
    }

    /**
     * Returns true if <code>name</code> is a parent name in the field.
     */
    public boolean hasParent(String name) {
        return parentIdFields.stream().anyMatch((mapper) -> name.equals(mapper.getParentName()));
    }

    /**
     * Returns true if <code>name</code> is a child name in the field.
     */
    public boolean hasChild(String name) {
        return parentIdFields.stream().anyMatch((mapper) -> mapper.getChildren().contains(name));
    }

    /**
     * Returns the parent Id field mapper associated with a parent <code>name</code>
     * if <code>isParent</code> is true and a child <code>name</code> otherwise.
     */
    public ParentIdFieldMapper getParentIdFieldMapper(String name, boolean isParent) {
        for (ParentIdFieldMapper mapper : parentIdFields) {
            if (isParent && name.equals(mapper.getParentName())) {
                return mapper;
            } else if (isParent == false && mapper.getChildren().contains(name)) {
                return mapper;
            }
        }
        return null;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        ParentJoinFieldMapper joinMergeWith = (ParentJoinFieldMapper) other;
        final List<ParentIdFieldMapper> newParentIdFields = new ArrayList<>();
        for (ParentIdFieldMapper mapper : parentIdFields) {
            if (joinMergeWith.getParentIdFieldMapper(mapper.getParentName(), true) == null) {
                conflicts.add("cannot remove parent [" + mapper.getParentName() + "] in join field [" + name() + "]");
            }
        }
        for (ParentIdFieldMapper mergeWithMapper : joinMergeWith.parentIdFields) {
            ParentIdFieldMapper self = getParentIdFieldMapper(mergeWithMapper.getParentName(), true);
            if (self == null) {
                if (getParentIdFieldMapper(mergeWithMapper.getParentName(), false) != null) {
                    // it is forbidden to add a parent to an existing child
                    conflicts.add("cannot create parent [" + mergeWithMapper.getParentName()  + "] from an existing child");
                }
                for (String child : mergeWithMapper.getChildren()) {
                    if (getParentIdFieldMapper(child, true) != null) {
                        // it is forbidden to add a parent to an existing child
                        conflicts.add("cannot create child [" + child  + "] from an existing parent");
                    }
                }
                newParentIdFields.add(mergeWithMapper);
            } else {
                for (String child : self.getChildren()) {
                    if (mergeWithMapper.getChildren().contains(child) == false) {
                        conflicts.add("cannot remove child [" + child + "] in join field [" + name() + "]");
                    }
                }
                ParentIdFieldMapper merged = (ParentIdFieldMapper) self.merge(mergeWithMapper);
                newParentIdFields.add(merged);
            }
        }
        this.eagerGlobalOrdinals = joinMergeWith.eagerGlobalOrdinals;
        this.parentIdFields = Collections.unmodifiableList(newParentIdFields);
        this.uniqueFieldMapper = (MetaJoinFieldMapper) uniqueFieldMapper.merge(joinMergeWith.uniqueFieldMapper);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        context.path().add(simpleName());
        XContentParser.Token token = context.parser().currentToken();
        String name = null;
        String parent = null;
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = context.parser().nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = context.parser().currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("name".equals(currentFieldName)) {
                        name = context.parser().text();
                    } else if ("parent".equals(currentFieldName)) {
                        parent = context.parser().text();
                    } else {
                        throw new IllegalArgumentException("unknown field name [" + currentFieldName + "] in join field [" + name() + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("parent".equals(currentFieldName)) {
                        parent = context.parser().numberValue().toString();
                    } else {
                        throw new IllegalArgumentException("unknown field name [" + currentFieldName + "] in join field [" + name() + "]");
                    }
                }
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            name = context.parser().text();
            parent = null;
        } else {
            throw new IllegalStateException("[" + name  + "] expected START_OBJECT or VALUE_STRING but was: " + token);
        }

        ParentIdFieldMapper parentIdField = getParentIdFieldMapper(name, true);
        ParentIdFieldMapper childParentIdField = getParentIdFieldMapper(name, false);
        if (parentIdField == null && childParentIdField == null) {
            throw new IllegalArgumentException("unknown join name [" + name + "] for field [" + name() + "]");
        }
        if (childParentIdField != null) {
            // Index the document as a child
            if (parent == null) {
                throw new IllegalArgumentException("[parent] is missing for join field [" + name() + "]");
            }
            if (context.sourceToParse().routing() == null) {
                throw new IllegalArgumentException("[routing] is missing for join field [" + name() + "]");
            }
            assert childParentIdField.getChildren().contains(name);
            ParseContext externalContext = context.createExternalValueContext(parent);
            childParentIdField.parse(externalContext);
        }
        if (parentIdField != null) {
            // Index the document as a parent
            assert parentIdField.getParentName().equals(name);
            ParseContext externalContext = context.createExternalValueContext(context.sourceToParse().id());
            parentIdField.parse(externalContext);
        }

        BytesRef binaryValue = new BytesRef(name);
        Field field = new Field(fieldType().name(), binaryValue, fieldType);
        context.doc().add(field);
        context.doc().add(new SortedDocValuesField(fieldType().name(), binaryValue));
        context.path().remove();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        builder.field("eager_global_ordinals", eagerGlobalOrdinals);
        builder.startObject("relations");
        for (ParentIdFieldMapper field : parentIdFields) {
            if (field.getChildren().size() == 1) {
                builder.field(field.getParentName(), field.getChildren().iterator().next());
            } else {
                builder.field(field.getParentName(), field.getChildren());
            }
        }
        builder.endObject();
    }

}
