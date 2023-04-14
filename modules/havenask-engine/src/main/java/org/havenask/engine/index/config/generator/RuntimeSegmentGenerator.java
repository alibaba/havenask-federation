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

package org.havenask.engine.index.config.generator;

import java.nio.file.Files;

import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.index.config.RealtimeInfo;
import org.havenask.engine.index.engine.EngineSettings;

/**
 * TODO 后续将该流程调整在shard目录创建\删除的流程中
 */
public class RuntimeSegmentGenerator {

    private final NativeProcessControlService nativeProcessControlService;
    private final HavenaskEngineEnvironment havenaskEngineEnvironment;
    private final String indexName;

    public RuntimeSegmentGenerator(
        HavenaskEngineEnvironment havenaskEngineEnvironment,
        NativeProcessControlService nativeProcessControlService,
        String indexName
    ) {
        this.havenaskEngineEnvironment = havenaskEngineEnvironment;
        this.indexName = indexName;
        this.nativeProcessControlService = nativeProcessControlService;
    }

    public void generate(Settings settings) {
        if (false == Files.exists(havenaskEngineEnvironment.getRuntimedataPath().resolve(indexName))) {
            boolean realTime = EngineSettings.HAVENASK_REALTIME_ENABLE.get(settings);
            if (realTime) {
                String topic = EngineSettings.HAVENASK_REALTIME_TOPIC_NAME.get(settings);
                String bootstrapServers = EngineSettings.HAVENASK_REALTIME_BOOTSTRAP_SERVERS.get(settings);
                RealtimeInfo realtimeInfo = new RealtimeInfo(indexName, topic, bootstrapServers);

                nativeProcessControlService.startBsJob(indexName, realtimeInfo.toString());
            } else {
                nativeProcessControlService.startBsJob(indexName);
            }
        }
    }

    public static void generateRuntimeSegment(
        HavenaskEngineEnvironment havenaskEngineEnvironment,
        NativeProcessControlService nativeProcessControlService,
        String indexName,
        Settings settings
    ) {
        RuntimeSegmentGenerator runtimeSegmentGenerator = new RuntimeSegmentGenerator(
            havenaskEngineEnvironment,
            nativeProcessControlService,
            indexName
        );
        runtimeSegmentGenerator.generate(settings);
    }
}
