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

package org.havenask.engine.index.config;

import org.havenask.test.HavenaskTestCase;

public class RealtimeInfoTests extends HavenaskTestCase {
    // test for RealtimeInfo
    public void testRealtimeInfo() {
        RealtimeInfo realtimeInfo = new RealtimeInfo("in0", "quickstart-events", "localhost:9092", 1000);
        assertEquals(
            realtimeInfo.toString(),
            "{\n"
                + "\t\"bootstrap.servers\":\"localhost:9092\",\n"
                + "\t\"data_table\":\"in0\",\n"
                + "\t\"kafka_start_timestamp\":1000,\n"
                + "\t\"module_name\":\"kafka\",\n"
                + "\t\"module_path\":\"libbs_raw_doc_reader_plugin.so\",\n"
                + "\t\"realtime_mode\":\"realtime_service_rawdoc_rt_build_mode\",\n"
                + "\t\"realtime_process_rawdoc\":\"true\",\n"
                + "\t\"src_signature\":\"16113477091138423397\",\n"
                + "\t\"topic_name\":\"quickstart-events\",\n"
                + "\t\"type\":\"plugin\"\n"
                + "}"
        );
    }
}
