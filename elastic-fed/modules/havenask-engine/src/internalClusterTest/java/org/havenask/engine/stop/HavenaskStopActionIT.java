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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.ArpcThreadLeakFilter;
import org.havenask.OkHttpThreadLeakFilter;
import org.havenask.common.SuppressForbidden;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.HavenaskITTestCase;
import org.havenask.engine.MockNativeProcessControlService;
import org.havenask.engine.search.action.TransportHavenaskSqlAction;
import org.havenask.engine.stop.action.HavenaskStopAction;
import org.havenask.engine.stop.action.HavenaskStopRequest;
import org.havenask.engine.stop.action.HavenaskStopResponse;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.transport.nio.MockNioTransportPlugin;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { OkHttpThreadLeakFilter.class, ArpcThreadLeakFilter.class })
@HavenaskIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, scope = HavenaskIntegTestCase.Scope.TEST)
public class HavenaskStopActionIT extends HavenaskITTestCase {

    private static final Logger logger = LogManager.getLogger(TransportHavenaskSqlAction.class);

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
    private long stopHavenaskTimeout = 3;
    private static final String CHECK_HAVENASK_ALIVE_COMMAND =
        "ps aux | grep sap_server_d | grep 'roleType=%s' | grep -v grep | awk '{print $2}'";

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        // stop all process
        Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", stopHavenaskCommand });
        // wait for stopping process finished
        process.waitFor(stopHavenaskTimeout, TimeUnit.SECONDS);
    }

    public void testLocalStartAndStop() throws Exception {
        assertFalse(checkProcessAlive("searcher"));
        assertFalse(checkProcessAlive("qrs"));
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startSearcherCommand });
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startQrsCommand });
        assertTrue(checkProcessAlive("searcher"));
        assertTrue(checkProcessAlive("qrs"));
        Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", stopHavenaskCommand });
        // wait for stopping process finished
        process.waitFor(stopHavenaskTimeout, TimeUnit.SECONDS);
        assertFalse(checkProcessAlive("searcher"));
        assertFalse(checkProcessAlive("qrs"));
    }

    // test stop searcher process
    public void testSearcherStop() throws Exception {
        String role = "searcher";
        // check searcher process is not running
        assertFalse(checkProcessAlive(role));
        // start searcher
        Runtime.getRuntime().exec(new String[] { "sh", "-c", startSearcherCommand });
        // check searcher process is running
        assertTrue(checkProcessAlive(role));

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
        assertTrue(checkProcessAlive(role));

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
        assertTrue(checkProcessAlive("searcher"));
        assertTrue(checkProcessAlive("qrs"));

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
            System.out.println(e.getMessage());
            assertTrue(e.getMessage().contains("role must be \"searcher\", \"qrs\" or \"all\", but get " + role + ";"));
        }
    }

    /**
     * 检测进程是否存活
     *
     * @param role 进程角色: searcher 或者 qrs
     * @return 返回进程存活状态
     */
    boolean checkProcessAlive(String role) {
        Process process = null;
        String command = String.format(Locale.ROOT, CHECK_HAVENASK_ALIVE_COMMAND, role);
        System.out.println(command);
        try {
            process = AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("run check command error, command [{}]", command), e);
                    return null;
                }
            });
            if (process == null) {
                logger.warn("run check command error, the process is null, don't know the process [{}] status", role);
                return true;
            }

            try (InputStream inputStream = process.getInputStream()) {
                byte[] bytes = inputStream.readAllBytes();
                String result = new String(bytes, StandardCharsets.UTF_8);
                if (result.trim().equals("")) {
                    logger.info("[{}] pid not found, the process is not alive", role);
                    return false;
                }

                try {
                    if (Integer.valueOf(result.trim()) > 0) {
                        return true;
                    } else {
                        logger.warn("check command get the process [{}] pid error, check result is [{}]", role, result);
                        return false;
                    }
                } catch (NumberFormatException e) {
                    logger.warn("check command get the process [{}] result format error, check result is [{}]", role, result);
                    return false;
                }
            }
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("check command get the process [{}] input error", role), e);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return true;
    }
}
