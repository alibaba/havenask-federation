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

import org.havenask.common.collect.Iterators;
import org.havenask.common.regex.Regex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * An immutable container for looking up {@link MappedFieldType}s by their name.
 */
class FieldTypeLookup implements Iterable<MappedFieldType> {

    private final Map<String, MappedFieldType> fullNameToFieldType = new HashMap<>();
    private final Map<String, String> aliasToConcreteName = new HashMap<>();

    /**
     * A map from field name to all fields whose content has been copied into it
     * through copy_to. A field only be present in the map if some other field
     * has listed it as a target of copy_to.
     *
     * For convenience, the set of copied fields includes the field itself.
     */
    private final Map<String, Set<String>> fieldToCopiedFields = new HashMap<>();
    private final DynamicKeyFieldTypeLookup dynamicKeyLookup;

    FieldTypeLookup() {
        this(Collections.emptyList(), Collections.emptyList());
    }

    FieldTypeLookup(Collection<FieldMapper> fieldMappers,
                    Collection<FieldAliasMapper> fieldAliasMappers) {
        Map<String, DynamicKeyFieldMapper> dynamicKeyMappers = new HashMap<>();

        for (FieldMapper fieldMapper : fieldMappers) {
            String fieldName = fieldMapper.name();
            MappedFieldType fieldType = fieldMapper.fieldType();
            fullNameToFieldType.put(fieldType.name(), fieldType);
            if (fieldMapper instanceof DynamicKeyFieldMapper) {
                dynamicKeyMappers.put(fieldName, (DynamicKeyFieldMapper) fieldMapper);
            }

            for (String targetField : fieldMapper.copyTo().copyToFields()) {
                Set<String> sourcePath = fieldToCopiedFields.get(targetField);
                if (sourcePath == null) {
                    Set<String> copiedFields = new HashSet<>();
                    copiedFields.add(targetField);
                    fieldToCopiedFields.put(targetField, copiedFields);
                }
                fieldToCopiedFields.get(targetField).add(fieldName);
            }
        }

        for (FieldAliasMapper fieldAliasMapper : fieldAliasMappers) {
            String aliasName = fieldAliasMapper.name();
            String path = fieldAliasMapper.path();
            aliasToConcreteName.put(aliasName, path);
        }

        this.dynamicKeyLookup = new DynamicKeyFieldTypeLookup(dynamicKeyMappers, aliasToConcreteName);
    }

    /**
     * Returns the mapped field type for the given field name.
     */
    public MappedFieldType get(String field) {
        String concreteField = aliasToConcreteName.getOrDefault(field, field);
        MappedFieldType fieldType = fullNameToFieldType.get(concreteField);
        if (fieldType != null) {
            return fieldType;
        }

        // If the mapping contains fields that support dynamic sub-key lookup, check
        // if this could correspond to a keyed field of the form 'path_to_field.path_to_key'.
        return dynamicKeyLookup.get(field);
    }

    /**
     * Returns a list of the full names of a simple match regex like pattern against full name and index name.
     */
    public Set<String> simpleMatchToFullName(String pattern) {
        Set<String> fields = new HashSet<>();
        for (MappedFieldType fieldType : this) {
            if (Regex.simpleMatch(pattern, fieldType.name())) {
                fields.add(fieldType.name());
            }
        }
        for (String aliasName : aliasToConcreteName.keySet()) {
            if (Regex.simpleMatch(pattern, aliasName)) {
                fields.add(aliasName);
            }
        }
        return fields;
    }

    /**
     * Given a concrete field name, return its paths in the _source.
     *
     * For most fields, the source path is the same as the field itself. However
     * there are cases where a field's values are found elsewhere in the _source:
     *   - For a multi-field, the source path is the parent field.
     *   - One field's content could have been copied to another through copy_to.
     *
     * @param field The field for which to look up the _source path. Note that the field
     *              should be a concrete field and *not* an alias.
     * @return A set of paths in the _source that contain the field's values.
     */
    public Set<String> sourcePaths(String field) {
        String resolvedField = field;
        int lastDotIndex = field.lastIndexOf('.');
        if (lastDotIndex > 0) {
            String parentField = field.substring(0, lastDotIndex);
            if (fullNameToFieldType.containsKey(parentField)) {
                resolvedField = parentField;
            }
        }

        return fieldToCopiedFields.containsKey(resolvedField)
            ? fieldToCopiedFields.get(resolvedField)
            : org.havenask.common.collect.Set.of(resolvedField);
    }

    @Override
    public Iterator<MappedFieldType> iterator() {
        Iterator<MappedFieldType> concreteFieldTypes = fullNameToFieldType.values().iterator();
        Iterator<MappedFieldType> keyedFieldTypes = dynamicKeyLookup.fieldTypes();
        return Iterators.concat(concreteFieldTypes, keyedFieldTypes);
    }
}
