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

package org.havenask.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.havenask.HavenaskParseException;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.time.DateFormatter;
import org.havenask.common.xcontent.support.XContentMapValues;
import org.havenask.index.similarity.SimilarityProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.havenask.common.xcontent.support.XContentMapValues.isArray;
import static org.havenask.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.havenask.common.xcontent.support.XContentMapValues.nodeStringValue;

public class TypeParsers {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TypeParsers.class);

    public static final String DOC_VALUES = "doc_values";
    public static final String INDEX_OPTIONS_DOCS = "docs";
    public static final String INDEX_OPTIONS_FREQS = "freqs";
    public static final String INDEX_OPTIONS_POSITIONS = "positions";
    public static final String INDEX_OPTIONS_OFFSETS = "offsets";

    public static void checkNull(String propName, Object propNode) {
        if (false == propName.equals("null_value") && propNode == null) {
            /*
             * No properties *except* null_value are allowed to have null. So we catch it here and tell the user something useful rather
             * than send them a null pointer exception later.
             */
            throw new MapperParsingException("[" + propName + "] must not have a [null] value");
        }
    }

    /**
     * Parse the {@code meta} key of the mapping.
     */
    public static Map<String, String> parseMeta(String name, Object metaObject) {
        if (metaObject instanceof Map == false) {
            throw new MapperParsingException("[meta] must be an object, got " + metaObject.getClass().getSimpleName() +
                    "[" + metaObject + "] for field [" + name +"]");
        }
        @SuppressWarnings("unchecked")
        Map<String, ?> meta = (Map<String, ?>) metaObject;
        if (meta.size() > 5) {
            throw new MapperParsingException("[meta] can't have more than 5 entries, but got " + meta.size() + " on field [" +
                    name + "]");
        }
        for (String key : meta.keySet()) {
            if (key.codePointCount(0, key.length()) > 20) {
                throw new MapperParsingException("[meta] keys can't be longer than 20 chars, but got [" + key +
                        "] for field [" + name + "]");
            }
        }
        for (Object value : meta.values()) {
            if (value instanceof String) {
                String sValue = (String) value;
                if (sValue.codePointCount(0, sValue.length()) > 50) {
                    throw new MapperParsingException("[meta] values can't be longer than 50 chars, but got [" + value +
                            "] for field [" + name + "]");
                }
            } else if (value == null) {
                throw new MapperParsingException("[meta] values can't be null (field [" + name + "])");
            } else {
                throw new MapperParsingException("[meta] values can only be strings, but got " +
                        value.getClass().getSimpleName() + "[" + value + "] for field [" + name + "]");
            }
        }
        Map<String, String> sortedMeta = new TreeMap<>();
        for (Map.Entry<String, ?> entry : meta.entrySet()) {
            sortedMeta.put(entry.getKey(), (String) entry.getValue());
        }
        return Collections.unmodifiableMap(sortedMeta);
    }

    /**
     * Parse common field attributes such as {@code doc_values} or {@code store}.
     */
    public static void parseField(FieldMapper.Builder<?> builder, String name, Map<String, Object> fieldNode,
                                  Mapper.TypeParser.ParserContext parserContext) {
        for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            final String propName = entry.getKey();
            final Object propNode = entry.getValue();
            checkNull(propName, propNode);
            if (propName.equals("store")) {
                builder.store(XContentMapValues.nodeBooleanValue(propNode, name + ".store"));
                iterator.remove();
            } else if (propName.equals("meta")) {
                builder.meta(parseMeta(name, propNode));
                iterator.remove();
            } else if (propName.equals("index")) {
                builder.index(XContentMapValues.nodeBooleanValue(propNode, name + ".index"));
                iterator.remove();
            } else if (propName.equals(DOC_VALUES)) {
                builder.docValues(XContentMapValues.nodeBooleanValue(propNode, name + "." + DOC_VALUES));
                iterator.remove();
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
                deprecationLogger.deprecate(
                    "boost",
                    "Parameter [boost] on field [{}] is deprecated and will be removed in 8.0",
                    name);
                iterator.remove();
            } else if (propName.equals("index_options")) {
                builder.indexOptions(nodeIndexOptionValue(propNode));
                iterator.remove();
            } else if (propName.equals("similarity")) {
                deprecationLogger.deprecate("similarity",
                    "The [similarity] parameter has no effect on field [" + name + "] and will be removed in 8.0");
                iterator.remove();
            } else if (parseMultiField(builder::addMultiField, name, parserContext, propName, propNode)) {
                iterator.remove();
            } else if (propName.equals("copy_to")) {
                if (parserContext.isWithinMultiField()) {
                    throw new MapperParsingException("copy_to in multi fields is not allowed. Found the copy_to in field [" + name + "] " +
                        "which is within a multi field.");
                } else {
                    List<String> copyFields = parseCopyFields(propNode);
                    FieldMapper.CopyTo.Builder cpBuilder = new FieldMapper.CopyTo.Builder();
                    copyFields.forEach(cpBuilder::add);
                    builder.copyTo(cpBuilder.build());
                }
                iterator.remove();
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean parseMultiField(Consumer<Mapper.Builder> multiFieldsBuilder, String name,
                                          Mapper.TypeParser.ParserContext parserContext, String propName, Object propNode) {
        if (propName.equals("fields")) {
            if (parserContext.isWithinMultiField()) {
                deprecationLogger.deprecate("multifield_within_multifield", "At least one multi-field, [" + name + "], was " +
                    "encountered that itself contains a multi-field. Defining multi-fields within a multi-field is deprecated and will " +
                    "no longer be supported in 8.0. To resolve the issue, all instances of [fields] that occur within a [fields] block " +
                    "should be removed from the mappings, either by flattening the chained [fields] blocks into a single level, or " +
                    "switching to [copy_to] if appropriate.");
            }

            parserContext = parserContext.createMultiFieldContext(parserContext);

            final Map<String, Object> multiFieldsPropNodes;
            if (propNode instanceof List && ((List<?>) propNode).isEmpty()) {
                multiFieldsPropNodes = Collections.emptyMap();
            } else if (propNode instanceof Map) {
                multiFieldsPropNodes = (Map<String, Object>) propNode;
            } else {
                throw new MapperParsingException("expected map for property [fields] on field [" + propNode + "] or " +
                    "[" + propName + "] but got a " + propNode.getClass());
            }

            for (Map.Entry<String, Object> multiFieldEntry : multiFieldsPropNodes.entrySet()) {
                String multiFieldName = multiFieldEntry.getKey();
                if (multiFieldName.contains(".")) {
                    throw new MapperParsingException("Field name [" + multiFieldName + "] which is a multi field of [" + name + "] cannot" +
                        " contain '.'");
                }
                if (!(multiFieldEntry.getValue() instanceof Map)) {
                    throw new MapperParsingException("illegal field [" + multiFieldName + "], only fields can be specified inside fields");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> multiFieldNodes = (Map<String, Object>) multiFieldEntry.getValue();

                String type;
                Object typeNode = multiFieldNodes.get("type");
                if (typeNode != null) {
                    type = typeNode.toString();
                } else {
                    throw new MapperParsingException("no type specified for property [" + multiFieldName + "]");
                }
                if (type.equals(ObjectMapper.CONTENT_TYPE)
                        || type.equals(ObjectMapper.NESTED_CONTENT_TYPE)
                        || type.equals(FieldAliasMapper.CONTENT_TYPE)) {
                    throw new MapperParsingException("Type [" + type + "] cannot be used in multi field");
                }

                Mapper.TypeParser typeParser = parserContext.typeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("no handler for type [" + type + "] declared on field [" + multiFieldName + "]");
                }
                multiFieldsBuilder.accept(typeParser.parse(multiFieldName, multiFieldNodes, parserContext));
                multiFieldNodes.remove("type");
                DocumentMapperParser.checkNoRemainingFields(propName, multiFieldNodes, parserContext.indexVersionCreated());
            }
            return true;
        }
        return false;
    }

    private static IndexOptions nodeIndexOptionValue(final Object propNode) {
        final String value = propNode.toString();
        if (INDEX_OPTIONS_OFFSETS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else if (INDEX_OPTIONS_POSITIONS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        } else if (INDEX_OPTIONS_FREQS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS_AND_FREQS;
        } else if (INDEX_OPTIONS_DOCS.equalsIgnoreCase(value)) {
            return IndexOptions.DOCS;
        } else {
            throw new HavenaskParseException("failed to parse index option [{}]", value);
        }
    }

    public static DateFormatter parseDateTimeFormatter(Object node) {
        if (node instanceof String) {
            return DateFormatter.forPattern((String) node);
        }
        throw new IllegalArgumentException("Invalid format: [" + node.toString() + "]: expected string value");
    }

    public static List<String> parseCopyFields(Object propNode) {
        List<String> copyFields = new ArrayList<>();
        if (isArray(propNode)) {
            for (Object node : (List<Object>) propNode) {
                copyFields.add(nodeStringValue(node, null));
            }
        } else {
            copyFields.add(nodeStringValue(propNode, null));
        }
        return copyFields;
    }

    public static SimilarityProvider resolveSimilarity(Mapper.TypeParser.ParserContext parserContext, String name, Object value) {
        if (value == null) {
            return null;    // use default
        }
        SimilarityProvider similarityProvider = parserContext.getSimilarity(value.toString());
        if (similarityProvider == null) {
            throw new MapperParsingException("Unknown Similarity type [" + value + "] for field [" + name + "]");
        }
        return similarityProvider;
    }
}
