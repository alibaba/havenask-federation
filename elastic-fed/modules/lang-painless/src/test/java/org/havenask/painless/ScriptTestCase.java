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

import junit.framework.AssertionFailedError;

import org.havenask.common.settings.Settings;
import org.havenask.painless.antlr.Walker;
import org.havenask.painless.spi.Whitelist;
import org.havenask.painless.spi.WhitelistLoader;
import org.havenask.script.ScriptContext;
import org.havenask.script.ScriptException;
import org.havenask.test.HavenaskTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.havenask.painless.action.PainlessExecuteAction.PainlessTestScript;

/**
 * Base test case for scripting unit tests.
 * <p>
 * Typically just asserts the output of {@code exec()}
 */
public abstract class ScriptTestCase extends HavenaskTestCase {
    private static final PainlessScriptEngine SCRIPT_ENGINE = new PainlessScriptEngine(Settings.EMPTY, newDefaultContexts());

    /** Creates a new contexts map with PainlessTextScript = org.havenask.painless.test */
    protected static Map<ScriptContext<?>, List<Whitelist>> newDefaultContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.havenask.painless.test"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    /**
     * Get the script engine to use.
     * If you override this method in order to customize the settings, it is recommended
     * to setup/teardown a class-level fixture and return it directly for performance.
     */
    protected PainlessScriptEngine getEngine() {
        return SCRIPT_ENGINE;
    }

    /**
     * Uses the {@link Debugger} to get the bytecode output for a script and compare
     * it against an expected bytecode passed in as a String.
     */
    public static final void assertBytecodeExists(String script, String bytecode) {
        final String asm = Debugger.toString(script);
        assertTrue("bytecode not found, got: \n" + asm , asm.contains(bytecode));
    }

    /**
     * Uses the {@link Debugger} to get the bytecode output for a script and compare
     * it against an expected bytecode pattern as a regular expression (please try to avoid!)
     */
    public static final void assertBytecodeHasPattern(String script, String pattern) {
        final String asm = Debugger.toString(script);
        assertTrue("bytecode not found, got: \n" + asm , asm.matches(pattern));
    }

    /** Checks a specific exception class is thrown (boxed inside ScriptException) and returns it. */
    public static final <T extends Throwable> T expectScriptThrows(Class<T> expectedType, ThrowingRunnable runnable) {
        return expectScriptThrows(expectedType, true, runnable);
    }

    /** Checks a specific exception class is thrown (boxed inside ScriptException) and returns it. */
    public static final <T extends Throwable> T expectScriptThrows(Class<T> expectedType, boolean shouldHaveScriptStack,
            ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (e instanceof ScriptException) {
                boolean hasEmptyScriptStack = ((ScriptException) e).getScriptStack().isEmpty();
                if (shouldHaveScriptStack && hasEmptyScriptStack) {
                    /* If this fails you *might* be missing -XX:-OmitStackTraceInFastThrow in the test jvm
                     * In Eclipse you can add this by default by going to Preference->Java->Installed JREs,
                     * clicking on the default JRE, clicking edit, and adding the flag to the
                     * "Default VM Arguments". */
                    AssertionFailedError assertion = new AssertionFailedError("ScriptException should have a scriptStack");
                    assertion.initCause(e);
                    throw assertion;
                } else if (false == shouldHaveScriptStack && false == hasEmptyScriptStack) {
                    AssertionFailedError assertion = new AssertionFailedError("ScriptException shouldn't have a scriptStack");
                    assertion.initCause(e);
                    throw assertion;
                }
                e = e.getCause();
                if (expectedType.isInstance(e)) {
                    return expectedType.cast(e);
                }
            } else {
                AssertionFailedError assertion = new AssertionFailedError("Expected boxed ScriptException");
                assertion.initCause(e);
                throw assertion;
            }
            AssertionFailedError assertion = new AssertionFailedError("Unexpected exception type, expected "
                                                                      + expectedType.getSimpleName());
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError("Expected exception " + expectedType.getSimpleName());
    }

    /**
     * Asserts that the script_stack looks right.
     */
    public static final void assertScriptStack(ScriptException e, String... stack) {
        // This particular incantation of assertions makes the error messages more useful
        try {
            assertThat(e.getScriptStack(), hasSize(stack.length));
            for (int i = 0; i < stack.length; i++) {
                assertEquals(stack[i], e.getScriptStack().get(i));
            }
        } catch (AssertionError assertion) {
            assertion.initCause(e);
            throw assertion;
        }
    }

    /** Compiles and returns the result of {@code script} */
    public final Object exec(String script) {
        return exec(script, null, true);
    }

    /** Compiles and returns the result of {@code script} with access to {@code picky} */
    public final Object exec(String script, boolean picky) {
        return exec(script, null, picky);
    }

    /** Compiles and returns the result of {@code script} with access to {@code vars} */
    public final Object exec(String script, Map<String, Object> vars, boolean picky) {
        Map<String,String> compilerSettings = new HashMap<>();
        compilerSettings.put(CompilerSettings.INITIAL_CALL_SITE_DEPTH, random().nextBoolean() ? "0" : "10");
        return exec(script, vars, compilerSettings, picky);
    }

    /** Compiles and returns the result of {@code script} with access to {@code vars} and compile-time parameters */
    public final Object exec(String script, Map<String, Object> vars, Map<String,String> compileParams, boolean picky) {
        // test for ambiguity errors before running the actual script if picky is true
        if (picky) {
            CompilerSettings pickySettings = new CompilerSettings();
            pickySettings.setPicky(true);
            pickySettings.setRegexesEnabled(CompilerSettings.REGEX_ENABLED.get(Settings.EMPTY));
            Walker.buildPainlessTree(getTestName(), script, pickySettings);
        }
        // test actual script execution
        PainlessTestScript.Factory factory = getEngine().compile(null, script, PainlessTestScript.CONTEXT, compileParams);
        PainlessTestScript testScript = factory.newInstance(vars == null ? Collections.emptyMap() : vars);
        return testScript.execute();
    }
}
