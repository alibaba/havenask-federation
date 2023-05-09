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

package org.havenask.engine;

import org.havenask.cluster.service.ClusterService;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.threadpool.ThreadPool;

public class MockNativeProcessControlService extends NativeProcessControlService {
    public MockNativeProcessControlService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        HavenaskEngineEnvironment havenaskEngineEnvironment
    ) {
        super(clusterService, threadPool, environment, nodeEnvironment, havenaskEngineEnvironment);
        String startScript = MockNativeProcessControlService.class.getResource("/fake_sap.sh").getPath();
        String stopScript = MockNativeProcessControlService.class.getResource("/stop_fake_sap.sh").getPath();
        this.startSearcherCommand = "sh " + startScript + " sap_server_d roleType=searcher &";
        this.startQrsCommand = "sh " + startScript + " sap_server_d roleType=qrs &";
        this.stopHavenaskCommand = "sh " + stopScript;
    }
}
