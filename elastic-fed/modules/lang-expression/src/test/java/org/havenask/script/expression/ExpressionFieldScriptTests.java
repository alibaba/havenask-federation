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

package org.havenask.script.expression;

import org.havenask.index.fielddata.IndexNumericFieldData;
import org.havenask.index.fielddata.LeafNumericFieldData;
import org.havenask.index.fielddata.SortedNumericDoubleValues;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.NumberFieldMapper;
import org.havenask.script.FieldScript;
import org.havenask.script.ScriptException;
import org.havenask.search.lookup.SearchLookup;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionFieldScriptTests extends HavenaskTestCase {
    private ExpressionScriptEngine service;
    private SearchLookup lookup;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("field")).thenReturn(fieldType);
        when(mapperService.fieldType("alias")).thenReturn(fieldType);

        SortedNumericDoubleValues doubleValues = mock(SortedNumericDoubleValues.class);
        when(doubleValues.advanceExact(anyInt())).thenReturn(true);
        when(doubleValues.nextValue()).thenReturn(2.718);

        LeafNumericFieldData atomicFieldData = mock(LeafNumericFieldData.class);
        when(atomicFieldData.getDoubleValues()).thenReturn(doubleValues);

        IndexNumericFieldData fieldData = mock(IndexNumericFieldData.class);
        when(fieldData.getFieldName()).thenReturn("field");
        when(fieldData.load(anyObject())).thenReturn(atomicFieldData);

        service = new ExpressionScriptEngine();
        lookup = new SearchLookup(mapperService, (ignored, lookup) -> fieldData, null);
    }

    private FieldScript.LeafFactory compile(String expression) {
        FieldScript.Factory factory = service.compile(null, expression, FieldScript.CONTEXT, Collections.emptyMap());
        return factory.newFactory(Collections.emptyMap(), lookup);
    }

    public void testCompileError() {
        ScriptException e = expectThrows(ScriptException.class, () -> {
            compile("doc['field'].value * *@#)(@$*@#$ + 4");
        });
        assertTrue(e.getCause() instanceof ParseException);
    }

    public void testLinkError() {
        ScriptException e = expectThrows(ScriptException.class, () -> {
            compile("doc['nonexistent'].value * 5");
        });
        assertTrue(e.getCause() instanceof ParseException);
    }

    public void testFieldAccess() throws IOException {
        FieldScript script = compile("doc['field'].value").newInstance(null);
        script.setDocument(1);

        Object result = script.execute();
        assertThat(result, equalTo(2.718));
    }

    public void testFieldAccessWithFieldAlias() throws IOException {
        FieldScript script = compile("doc['alias'].value").newInstance(null);
        script.setDocument(1);

        Object result = script.execute();
        assertThat(result, equalTo(2.718));
    }
}
