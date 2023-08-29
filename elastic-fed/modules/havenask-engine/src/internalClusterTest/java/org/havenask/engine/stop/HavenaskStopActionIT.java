/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.stop;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.havenask.ArpcThreadLeakFilter;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.common.SuppressForbidden;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.HavenaskITTestCase;
import org.havenask.engine.MockNativeProcessControlService;
import org.havenask.engine.stop.action.HavenaskStopAction;
import org.havenask.engine.stop.action.HavenaskStopRequest;
import org.havenask.engine.stop.action.HavenaskStopResponse;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.transport.nio.MockNioTransportPlugin;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.havenask.engine.NativeProcessControlService.checkProcessAlive;

@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class, ArpcThreadLeakFilter.class })
@HavenaskIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, scope = HavenaskIntegTestCase.Scope.TEST)
public class HavenaskStopActionIT extends HavenaskITTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .putList("node.roles", Arrays.asList("master", "data", "ingest"));
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class, MockNioTransportPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    private String startScript = HavenaskStopActionIT.class.getResource("/fake_sap.sh").getPath();
    String stopScript = MockNativeProcessControlService.class.getResource("/stop_fake_sap.sh").getPath();
    private String startSearcherCommand = "sh " + startScript + " sap_server_d roleType=searcher &";
    private String startQrsCommand = "sh " + startScript + " sap_server_d roleType=qrs &";
    private String stopHavenaskCommand = "sh " + stopScript;
    private long maxWaitSeconds = 10;

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        // stop all process
        Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", stopHavenaskCommand });
        // wait for stopping process finished
        process.waitFor(maxWaitSeconds, TimeUnit.SECONDS);
    }

    public void testLocalStartAndStop() throws Exception {
        assertBusy(() -> assertFalse(checkProcessAlive("searcher")), maxWaitSeconds, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse(checkProcessAlive("qrs")), maxWaitSeconds, TimeUnit.SECONDS);
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startSearcherCommand });
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startQrsCommand });
        assertBusy(() -> assertTrue(checkProcessAlive("searcher")), maxWaitSeconds, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(checkProcessAlive("qrs")), maxWaitSeconds, TimeUnit.SECONDS);
        Runtime.getRuntime().exec(new String[] { "sh", "-c", stopHavenaskCommand });
        // wait for stopping process finished
        assertBusy(() -> assertFalse(checkProcessAlive("searcher")), maxWaitSeconds, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse(checkProcessAlive("qrs")), maxWaitSeconds, TimeUnit.SECONDS);
    }

    // test stop searcher process
    public void testSearcherStop() throws Exception {
        String role = "searcher";
        // check searcher process is not running
        assertFalse(checkProcessAlive(role));
        // start searcher
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startSearcherCommand });
        // check searcher process is running
        assertBusy(() -> assertTrue(checkProcessAlive(role)), maxWaitSeconds, TimeUnit.SECONDS);

        HavenaskStopRequest request = new HavenaskStopRequest(role);
        HavenaskStopResponse response = client().execute(HavenaskStopAction.INSTANCE, request).actionGet();
        assertEquals(200, response.getResultCode());
        assertEquals("target stop role: searcher; run stopping searcher command success; ", response.getResult());
        // check searcher process is not running
        assertFalse(checkProcessAlive(role));
    }

    // test stop qrs process
    public void testQrsStop() throws Exception {
        String role = "qrs";
        assertFalse(checkProcessAlive(role));
        // start qrs
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startQrsCommand });
        // check qrs process is running
        assertBusy(() -> assertTrue(checkProcessAlive(role)), maxWaitSeconds, TimeUnit.SECONDS);

        HavenaskStopRequest request = new HavenaskStopRequest(role);
        HavenaskStopResponse response = client().execute(HavenaskStopAction.INSTANCE, request).actionGet();
        assertEquals(200, response.getResultCode());
        assertEquals("target stop role: qrs; run stopping qrs command success; ", response.getResult());
        // check searcher process is not running
        assertFalse(checkProcessAlive(role));
    }

    // test stop all process
    public void testAllStop() throws Exception {
        String role = "all";
        // check searcher and qrs process is not running
        assertFalse(checkProcessAlive("searcher"));
        assertFalse(checkProcessAlive("qrs"));
        // start searcher and qrs
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startSearcherCommand });
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startQrsCommand });
        // check searcher and qrs process is running
        assertBusy(() -> assertTrue(checkProcessAlive("searcher")), maxWaitSeconds, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(checkProcessAlive("qrs")), maxWaitSeconds, TimeUnit.SECONDS);

        HavenaskStopRequest request = new HavenaskStopRequest(role);
        HavenaskStopResponse response = client().execute(HavenaskStopAction.INSTANCE, request).actionGet();
        assertEquals(200, response.getResultCode());
        assertEquals(
            "target stop role: all; run stopping searcher command success; run stopping qrs command success; ",
            response.getResult()
        );
        // check searcher and qrs process is not running
        assertFalse(checkProcessAlive("searcher"));
        assertFalse(checkProcessAlive("qrs"));
    }

    // illegal role
    public void testIllegalRoleStop() throws Exception {
        String role = "illegal";
        HavenaskStopRequest request = new HavenaskStopRequest(role);
        try {
            client().execute(HavenaskStopAction.INSTANCE, request).actionGet();
            fail("should throw exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("role must be \"searcher\", \"qrs\" or \"all\", but get " + role + ";"));
        }
    }

    // test null role
    public void testNullRoleStop() throws Exception {
        String role = null;
        HavenaskStopRequest request = new HavenaskStopRequest(role);
        try {
            client().execute(HavenaskStopAction.INSTANCE, request).actionGet();
            fail("should throw exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("role must be specified;"));
        }
    }
}
