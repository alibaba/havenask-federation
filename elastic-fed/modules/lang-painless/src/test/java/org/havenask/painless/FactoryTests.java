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

package org.havenask.painless;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.havenask.common.settings.Settings;
import org.havenask.painless.spi.Whitelist;
import org.havenask.script.ScriptContext;
import org.havenask.script.ScriptException;
import org.havenask.script.ScriptFactory;
import org.havenask.script.TemplateScript;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class FactoryTests extends ScriptTestCase {
    private static PainlessScriptEngine SCRIPT_ENGINE;

    @BeforeClass
    public static void beforeClass() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = newDefaultContexts();
        contexts.put(StatefulFactoryTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(FactoryTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(DeterministicFactoryTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(EmptyTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(TemplateScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(VoidReturnTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(FactoryTestConverterScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(FactoryTestConverterScriptBadDef.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(DocFieldsTestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        SCRIPT_ENGINE = new PainlessScriptEngine(Settings.EMPTY, contexts);
    }

    @AfterClass
    public static void afterClass() {
        SCRIPT_ENGINE = null;
    }

    @Override
    protected PainlessScriptEngine getEngine() {
        return SCRIPT_ENGINE;
    }

    public abstract static class StatefulFactoryTestScript {
        private final int x;
        private final int y;

        public StatefulFactoryTestScript(int x, int y, int a, int b) {
            this.x = x*a;
            this.y = y*b;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y*2;
        }

        public int getC() {
            return -1;
        }

        public int getD() {
            return 2;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract Object execute(int test);

        public abstract boolean needsTest();
        public abstract boolean needsNothing();
        public abstract boolean needsX();
        public abstract boolean needsC();
        public abstract boolean needsD();

        public interface StatefulFactory {
            StatefulFactoryTestScript newInstance(int a, int b);

            boolean needsTest();
            boolean needsNothing();
            boolean needsX();
            boolean needsC();
            boolean needsD();
        }

        public interface Factory {
            StatefulFactory newFactory(int x, int y);

            boolean needsTest();
            boolean needsNothing();
            boolean needsX();
            boolean needsC();
            boolean needsD();
        }

        public static final ScriptContext<StatefulFactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", StatefulFactoryTestScript.Factory.class);
    }

    public void testStatefulFactory() {
        StatefulFactoryTestScript.Factory factory = getEngine().compile(
            "stateful_factory_test", "test + x + y + d", StatefulFactoryTestScript.CONTEXT, Collections.emptyMap());
        StatefulFactoryTestScript.StatefulFactory statefulFactory = factory.newFactory(1, 2);
        StatefulFactoryTestScript script = statefulFactory.newInstance(3, 4);
        assertEquals(24, script.execute(3));
        statefulFactory.newInstance(5, 6);
        assertEquals(28, script.execute(7));
        assertEquals(true, script.needsTest());
        assertEquals(false, script.needsNothing());
        assertEquals(true, script.needsX());
        assertEquals(false, script.needsC());
        assertEquals(true, script.needsD());
        assertEquals(true, statefulFactory.needsTest());
        assertEquals(false, statefulFactory.needsNothing());
        assertEquals(true, statefulFactory.needsX());
        assertEquals(false, statefulFactory.needsC());
        assertEquals(true, statefulFactory.needsD());
        assertEquals(true, factory.needsTest());
        assertEquals(false, factory.needsNothing());
        assertEquals(true, factory.needsX());
        assertEquals(false, factory.needsC());
        assertEquals(true, factory.needsD());
    }

    public abstract static class FactoryTestScript {
        private final Map<String, Object> params;

        public FactoryTestScript(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract Object execute(int test);

        public interface Factory {
            FactoryTestScript newInstance(Map<String, Object> params);

            boolean needsTest();
            boolean needsNothing();
        }

        public static final ScriptContext<FactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", FactoryTestScript.Factory.class);
    }

    public abstract static class DeterministicFactoryTestScript {
        private final Map<String, Object> params;

        public DeterministicFactoryTestScript(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract Object execute(int test);

        public interface Factory extends ScriptFactory{
            FactoryTestScript newInstance(Map<String, Object> params);

            boolean needsTest();
            boolean needsNothing();
        }

        public static final ScriptContext<DeterministicFactoryTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", DeterministicFactoryTestScript.Factory.class);
    }

    public void testFactory() {
        FactoryTestScript.Factory factory =
            getEngine().compile("factory_test", "test + params.get('test')", FactoryTestScript.CONTEXT, Collections.emptyMap());
        FactoryTestScript script = factory.newInstance(Collections.singletonMap("test", 2));
        assertEquals(4, script.execute(2));
        assertEquals(5, script.execute(3));
        // The factory interface doesn't define `docFields` so we don't generate it.
        expectThrows(NoSuchMethodException.class, () -> factory.getClass().getMethod("docFields"));
        script = factory.newInstance(Collections.singletonMap("test", 3));
        assertEquals(5, script.execute(2));
        assertEquals(2, script.execute(-1));
        assertEquals(true, factory.needsTest());
        assertEquals(false, factory.needsNothing());
    }

    public void testDeterministic() {
        DeterministicFactoryTestScript.Factory factory =
            getEngine().compile("deterministic_test", "Integer.parseInt('123')",
                DeterministicFactoryTestScript.CONTEXT, Collections.emptyMap());
        assertTrue(factory.isResultDeterministic());
        assertEquals(123, factory.newInstance(Collections.emptyMap()).execute(0));
    }

    public void testNotDeterministic() {
        DeterministicFactoryTestScript.Factory factory =
            getEngine().compile("not_deterministic_test", "Math.random()",
                DeterministicFactoryTestScript.CONTEXT, Collections.emptyMap());
        assertFalse(factory.isResultDeterministic());
        Double d = (Double)factory.newInstance(Collections.emptyMap()).execute(0);
        assertTrue(d >= 0.0 && d <= 1.0);
    }

    public void testMixedDeterministicIsNotDeterministic() {
        DeterministicFactoryTestScript.Factory factory =
            getEngine().compile("not_deterministic_test", "Integer.parseInt('123') + Math.random()",
                DeterministicFactoryTestScript.CONTEXT, Collections.emptyMap());
        assertFalse(factory.isResultDeterministic());
        Double d = (Double)factory.newInstance(Collections.emptyMap()).execute(0);
        assertTrue(d >= 123.0 && d <= 124.0);
    }

    public abstract static class EmptyTestScript {
        public static final String[] PARAMETERS = {};
        public abstract Object execute();

        public interface Factory {
            EmptyTestScript newInstance();
        }

        public static final ScriptContext<EmptyTestScript.Factory> CONTEXT =
            new ScriptContext<>("test", EmptyTestScript.Factory.class);
    }

    public void testEmpty() {
        EmptyTestScript.Factory factory = getEngine().compile("empty_test", "1", EmptyTestScript.CONTEXT, Collections.emptyMap());
        EmptyTestScript script = factory.newInstance();
        assertEquals(1, script.execute());
        assertEquals(1, script.execute());
        script = factory.newInstance();
        assertEquals(1, script.execute());
        assertEquals(1, script.execute());
    }

    public void testTemplate() {
        TemplateScript.Factory factory =
            getEngine().compile("template_test", "params['test']", TemplateScript.CONTEXT, Collections.emptyMap());
        TemplateScript script = factory.newInstance(Collections.singletonMap("test", "abc"));
        assertEquals("abc", script.execute());
        assertEquals("abc", script.execute());
        script = factory.newInstance(Collections.singletonMap("test", "def"));
        assertEquals("def", script.execute());
        assertEquals("def", script.execute());
    }

    public void testGetterInLambda() {
        FactoryTestScript.Factory factory =
            getEngine().compile("template_test",
                "IntSupplier createLambda(IntSupplier s) { return s; } createLambda(() -> params['x'] + test).getAsInt()",
                FactoryTestScript.CONTEXT, Collections.emptyMap());
        FactoryTestScript script = factory.newInstance(Collections.singletonMap("x", 1));
        assertEquals(2, script.execute(1));
    }

    public abstract static class VoidReturnTestScript {
        public static final String[] PARAMETERS = {"map"};
        public abstract void execute(Map<Object, Object> map);

        public interface Factory {
            VoidReturnTestScript newInstance();
        }

        public static final ScriptContext<VoidReturnTestScript.Factory> CONTEXT =
                new ScriptContext<>("test", VoidReturnTestScript.Factory.class);
    }

    public void testVoidReturn() {
        getEngine().compile("void_return_test", "int x = 1 + 1; return;", VoidReturnTestScript.CONTEXT, Collections.emptyMap());
        IllegalArgumentException iae = expectScriptThrows(IllegalArgumentException.class, () ->
                getEngine().compile("void_return_test", "1 + 1", VoidReturnTestScript.CONTEXT, Collections.emptyMap()));
        assertEquals(iae.getMessage(), "not a statement: result not used from addition operation [+]");
    }

    public abstract static class FactoryTestConverterScript {
        private final Map<String, Object> params;

        public FactoryTestConverterScript(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract long[] execute(int test);

        public interface Factory {
            FactoryTestConverterScript newInstance(Map<String, Object> params);
        }

        public static final ScriptContext<FactoryTestConverterScript.Factory> CONTEXT =
            new ScriptContext<>("test", FactoryTestConverterScript.Factory.class);

        public static long[] convertFromInt(int i) {
            return new long[]{i};
        }

        public static long[] convertFromString(String s) {
            return new long[]{Long.parseLong(s)};
        }

        public static long[] convertFromList(List<?> l) {
            long[] converted = new long[l.size()];
            for (int i=0; i < l.size(); i++) {
                Object o = l.get(i);
                if (o instanceof Long) {
                    converted[i] = (Long) o;
                } else if (o instanceof Integer) {
                    converted[i] = (Integer) o;
                } else if (o instanceof String) {
                    converted[i] = Long.parseLong((String) o);
                }
            }
            return converted;
        }

        public static long[] convertFromDef(Object def) {
            if (def instanceof String) {
                return convertFromString((String)def);
            } else if (def instanceof Integer) {
                return convertFromInt(((Integer) def).intValue());
            } else if (def instanceof List) {
                return convertFromList((List) def);
            } else {
                return (long[]) def;
            }
            //throw new ClassCastException("Cannot convert [" + def + "] to long[]");
        }
    }


    public void testConverterFactory() {
        FactoryTestConverterScript.Factory factory =
            getEngine().compile("converter_test",
                "return test;",
                FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        FactoryTestConverterScript script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{2}, script.execute(2));
        script = factory.newInstance(Collections.singletonMap("test", 3));
        assertArrayEquals(new long[]{3}, script.execute(3));

        factory = getEngine().compile("converter_test",
            "return test + 1;",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1001}, script.execute(1000));

        factory = getEngine().compile("converter_test",
            "return '100';",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{100}, script.execute(1000));

        factory = getEngine().compile("converter_test",
            "long[] a = new long[]{test, 123}; return a;",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1000, 123}, script.execute(1000));

        factory = getEngine().compile("converter_test",
            "return [test, 123];",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1000, 123}, script.execute(1000));

        factory = getEngine().compile("converter_test",
            "ArrayList a = new ArrayList(); a.add(test); a.add(456); a.add('789'); return a;",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{123, 456, 789}, script.execute(123));

        // autoreturn, no converter
        factory = getEngine().compile("converter_test",
            "new long[]{test}",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{123}, script.execute(123));

        // autoreturn, converter
        factory = getEngine().compile("converter_test",
            "test",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{456}, script.execute(456));

        factory = getEngine().compile("converter_test",
            "'1001'",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1001}, script.execute(456));

        // def tests
        factory = getEngine().compile("converter_test",
            "def a = new long[]{test, 123}; return a;",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1000, 123}, script.execute(1000));

        factory = getEngine().compile("converter_test",
            "def l = [test, 123]; l;",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1000, 123}, script.execute(1000));

        factory = getEngine().compile("converter_test",
            "def a = new ArrayList(); a.add(test); a.add(456); a.add('789'); return a;",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{123, 456, 789}, script.execute(123));

        // autoreturn, no converter
        factory = getEngine().compile("converter_test",
            "def a = new long[]{test}; a;",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{123}, script.execute(123));

        // autoreturn, converter
        factory = getEngine().compile("converter_test",
            "def a = '1001'; a",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1001}, script.execute(456));

        factory = getEngine().compile("converter_test",
            "int x = 1",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(null, script.execute(123));

        factory = getEngine().compile("converter_test",
            "short x = 1; return x",
            FactoryTestConverterScript.CONTEXT, Collections.emptyMap());
        script = factory.newInstance(Collections.singletonMap("test", 2));
        assertArrayEquals(new long[]{1}, script.execute(123));

        ClassCastException cce = expectScriptThrows(ClassCastException.class, () ->
            getEngine().compile("converter_test",
                "return true;",
                FactoryTestConverterScript.CONTEXT, Collections.emptyMap()));
        assertEquals(cce.getMessage(), "Cannot cast from [boolean] to [long[]].");
    }

    public abstract static class FactoryTestConverterScriptBadDef {
        private final Map<String, Object> params;

        public FactoryTestConverterScriptBadDef(Map<String, Object> params) {
            this.params = params;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public static final String[] PARAMETERS = new String[] {"test"};
        public abstract long[] execute(int test);

        public interface Factory {
            FactoryTestConverterScriptBadDef newInstance(Map<String, Object> params);
        }

        public static final ScriptContext<FactoryTestConverterScriptBadDef.Factory> CONTEXT =
            new ScriptContext<>("test", FactoryTestConverterScriptBadDef.Factory.class);

        public static long[] convertFromDef(int def) {
            return new long[]{def};
        }
    }

    public void testConverterFactoryBadDef() {
        IllegalStateException ise = null;
        try {
            getEngine().compile("converter_def",
                "return test;",
                FactoryTestConverterScriptBadDef.CONTEXT, Collections.emptyMap());
        } catch (ScriptException e) {
            ise = (IllegalStateException) e.getCause();
        }
        assertNotNull(ise);
        assertEquals("convertFromDef must take a single Object as an argument, not [int]", ise.getMessage());
    }

    public abstract static class DocFieldsTestScript {
        public static final ScriptContext<DocFieldsTestScript.Factory> CONTEXT = new ScriptContext<>(
            "test",
            DocFieldsTestScript.Factory.class
        );

        public interface Factory {
            DocFieldsTestScript newInstance();

            List<String> docFields();
        }

        public static final String[] PARAMETERS = new String[] {};

        public abstract String execute();

        public final Map<String, String> getDoc() {
            Map<String, String> doc = new HashMap<>();
            doc.put("cat", "meow");
            doc.put("dog", "woof");
            return doc;
        }
    }

    public void testDocFields() {
        DocFieldsTestScript.Factory f =
                getEngine().compile("test", "doc['cat'] + doc['dog']", DocFieldsTestScript.CONTEXT, Collections.emptyMap());
        assertThat(f.docFields(), equalTo(Arrays.asList("cat", "dog")));
        assertThat(f.newInstance().execute(), equalTo("meowwoof"));
    }
}
