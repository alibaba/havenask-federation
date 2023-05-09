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

package org.havenask.ingest.common;

import org.havenask.HavenaskException;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentParseException;
import org.havenask.script.IngestScript;
import org.havenask.script.MockScriptEngine;
import org.havenask.script.Script;
import org.havenask.script.ScriptException;
import org.havenask.script.ScriptModule;
import org.havenask.script.ScriptService;
import org.havenask.script.ScriptType;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptProcessorFactoryTests extends HavenaskTestCase {

    private ScriptProcessor.Factory factory;
    private static final Map<String, String> ingestScriptParamToType;
    static {
        Map<String, String> map = new HashMap<>();
        map.put("id", "stored");
        map.put("source", "inline");
        ingestScriptParamToType = Collections.unmodifiableMap(map);
    }

    @Before
    public void init() {
        factory = new ScriptProcessor.Factory(mock(ScriptService.class));
    }

    public void testFactoryValidationWithDefaultLang() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "source");
        configMap.put(randomType, "foo");
        ScriptProcessor processor = factory.create(null, randomAlphaOfLength(10), null, configMap);
        assertThat(processor.getScript().getLang(), equalTo(randomType.equals("id") ? null : Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType().toString(), equalTo(ingestScriptParamToType.get(randomType)));
        assertThat(processor.getScript().getParams(), equalTo(Collections.emptyMap()));
    }

    public void testFactoryValidationWithParams() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "source");
        Map<String, Object> randomParams = Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10));
        configMap.put(randomType, "foo");
        configMap.put("params", randomParams);
        ScriptProcessor processor = factory.create(null, randomAlphaOfLength(10), null, configMap);
        assertThat(processor.getScript().getLang(), equalTo(randomType.equals("id") ? null : Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType().toString(), equalTo(ingestScriptParamToType.get(randomType)));
        assertThat(processor.getScript().getParams(), equalTo(randomParams));
    }

    public void testFactoryValidationForMultipleScriptingTypes() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("id", "foo");
        configMap.put("source", "bar");
        configMap.put("lang", "mockscript");

        XContentParseException exception = expectThrows(XContentParseException.class,
            () -> factory.create(null, randomAlphaOfLength(10), null, configMap));
        assertThat(exception.getMessage(), containsString("[script] failed to parse field [source]"));
    }

    public void testFactoryValidationAtLeastOneScriptingType() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("lang", "mockscript");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> factory.create(null, randomAlphaOfLength(10), null, configMap));

        assertThat(exception.getMessage(), is("must specify either [source] for an inline script or [id] for a stored script"));
    }

    public void testInlineBackcompat() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("inline", "code");

        factory.create(null, randomAlphaOfLength(10), null, configMap);
        assertWarnings("Deprecated field [inline] used, expected [source] instead");
    }

    public void testFactoryInvalidateWithInvalidCompiledScript() throws Exception {
        String randomType = randomFrom("source", "id");
        ScriptService mockedScriptService = mock(ScriptService.class);
        ScriptException thrownException = new ScriptException("compile-time exception", new RuntimeException(),
            Collections.emptyList(), "script", "mockscript");
        when(mockedScriptService.compile(any(), any())).thenThrow(thrownException);
        factory = new ScriptProcessor.Factory(mockedScriptService);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(randomType, "my_script");

        HavenaskException exception = expectThrows(HavenaskException.class,
            () -> factory.create(null, randomAlphaOfLength(10), null, configMap));

        assertThat(exception.getMessage(), is("compile-time exception"));
    }

    public void testInlineIsCompiled() throws Exception {
        String scriptName = "foo";
        ScriptService scriptService = new ScriptService(Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(
                    Script.DEFAULT_SCRIPT_LANG,
                    Collections.singletonMap(scriptName, ctx -> {
                        ctx.put("foo", "bar");
                        return null;
                    }),
                    Collections.emptyMap()
                )
            ), new HashMap<>(ScriptModule.CORE_CONTEXTS));
        factory = new ScriptProcessor.Factory(scriptService);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("source", scriptName);
        ScriptProcessor processor = factory.create(null, null, randomAlphaOfLength(10), configMap);
        assertThat(processor.getScript().getLang(), equalTo(Script.DEFAULT_SCRIPT_LANG));
        assertThat(processor.getScript().getType(), equalTo(ScriptType.INLINE));
        assertThat(processor.getScript().getParams(), equalTo(Collections.emptyMap()));
        assertNotNull(processor.getPrecompiledIngestScript());
        Map<String, Object> ctx = new HashMap<>();
        processor.getPrecompiledIngestScript().execute(ctx);
        assertThat(ctx.get("foo"), equalTo("bar"));
    }

    public void testStoredIsNotCompiled() throws Exception {
        ScriptService mockedScriptService = mock(ScriptService.class);
        when(mockedScriptService.compile(any(), any())).thenReturn(mock(IngestScript.Factory.class));
        factory = new ScriptProcessor.Factory(mockedScriptService);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("id", "script_name");
        ScriptProcessor processor = factory.create(null, null, randomAlphaOfLength(10), configMap);
        assertNull(processor.getScript().getLang());
        assertThat(processor.getScript().getType(), equalTo(ScriptType.STORED));
        assertThat(processor.getScript().getParams(), equalTo(Collections.emptyMap()));
        assertNull(processor.getPrecompiledIngestScript());
    }
}
