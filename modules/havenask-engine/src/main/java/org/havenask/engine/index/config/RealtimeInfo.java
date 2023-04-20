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

import com.alibaba.fastjson.annotation.JSONField;

import org.havenask.engine.util.JsonPrettyFormatter;

/**
 * {
 *     "realtime_mode":"realtime_service_rawdoc_rt_build_mode",
 *     "data_table" : "in0",
 *     "type":"plugin",
 *     "module_name":"kafka",
 *     "module_path":"libbs_raw_doc_reader_plugin.so",
 *     "topic_name":"quickstart-events",
 *     "bootstrap.servers":"localhost:9092",
 *     "src_signature":"16113477091138423397",
 *     "realtime_process_rawdoc": "true"
 * }
 */
public class RealtimeInfo {
    public String realtime_mode = "realtime_service_rawdoc_rt_build_mode";
    public String data_table;
    public String type = "plugin";
    public String module_name = "kafka";
    public String module_path = "libbs_raw_doc_reader_plugin.so";
    public String topic_name;
    @JSONField(name = "bootstrap.servers")
    public String bootstrap_servers;
    public String src_signature = "16113477091138423397";
    public String realtime_process_rawdoc = "true";
    public long kafka_start_timestamp = 0;

    public RealtimeInfo(String data_table, String topic_name, String bootstrap_servers, long kafka_start_timestamp) {
        this.data_table = data_table;
        this.topic_name = topic_name;
        this.bootstrap_servers = bootstrap_servers;
        this.kafka_start_timestamp = kafka_start_timestamp;
    }

    @Override
    public String toString() {
        return JsonPrettyFormatter.toJsonString(this);
    }
}
