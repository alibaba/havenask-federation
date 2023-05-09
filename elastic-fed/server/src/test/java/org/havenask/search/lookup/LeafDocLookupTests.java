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

package org.havenask.search.lookup;

import org.havenask.index.fielddata.LeafFieldData;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.ScriptDocValues;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperService;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import static org.havenask.search.lookup.LeafDocLookup.TYPES_DEPRECATION_MESSAGE;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafDocLookupTests extends HavenaskTestCase {
    private ScriptDocValues<?> docValues;
    private LeafDocLookup docLookup;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("field");
        when(fieldType.valueForDisplay(anyObject())).then(returnsFirstArg());

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("_type")).thenReturn(fieldType);
        when(mapperService.fieldType("field")).thenReturn(fieldType);
        when(mapperService.fieldType("alias")).thenReturn(fieldType);

        docValues = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData = createFieldData(docValues);

        docLookup = new LeafDocLookup(mapperService,
            ignored -> fieldData,
            new String[] { "type" },
            null);
    }

    public void testBasicLookup() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("field");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testFieldAliases() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("alias");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testTypesDeprecation() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("_type");
        assertEquals(docValues, fetchedDocValues);
        assertWarnings(TYPES_DEPRECATION_MESSAGE);
    }

    private IndexFieldData<?> createFieldData(ScriptDocValues scriptDocValues) {
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        doReturn(scriptDocValues).when(leafFieldData).getScriptValues();

        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn("field");
        doReturn(leafFieldData).when(fieldData).load(anyObject());

        return fieldData;
    }
}
