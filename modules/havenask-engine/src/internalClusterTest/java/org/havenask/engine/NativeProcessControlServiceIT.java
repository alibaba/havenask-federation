/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Havenask Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.havenask.engine;

import java.util.Arrays;
import java.util.Collection;

import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskSingleNodeTestCase;

public class NativeProcessControlServiceIT extends HavenaskSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(
            HavenaskEnginePlugin.class
        );
    }

    public void testStartProcess() {
        NativeProcessControlService nativeProcessControlService = getInstanceFromNode(NativeProcessControlService.class);
        nativeProcessControlService.doStop();
    }
}
