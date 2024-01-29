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

import org.havenask.engine.util.JsonPrettyFormatter;

import java.util.Objects;

public class ClusterJsonMinMustParams {

    public boolean direct_write = true;
    public WalConfig wal_config = new WalConfig();
    public ClusterConfig cluster_config = new ClusterConfig();
    public OnlineIndexConfig online_index_config = new OnlineIndexConfig();

    public static class ClusterConfig {
        public String cluster_name;
        public String table_name;
        public BuilderRuleConfig builder_rule_config = new BuilderRuleConfig();
        public HashMode hash_mode = new HashMode("_id", "HASH");
    }

    public static class BuilderRuleConfig {
        public int partition_count = 1;
    }

    public static class HashMode {
        public String hash_field = "_id";
        public String hash_function = "HASH";

        public HashMode(String hash_field, String hash_function) {
            this.hash_field = hash_field;
            this.hash_function = hash_function;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HashMode hashMode = (HashMode) o;
            return Objects.equals(hash_field, hashMode.hash_field) && Objects.equals(hash_function, hashMode.hash_function);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hash_field, hash_function);
        }
    }

    public static class WalConfig {
        public SinkConfig sink = new SinkConfig();
        public String strategy = "queue";
    }

    public static class SinkConfig {
        public String queue_name;
        public String queue_size = "5000";
    }

    public static class OnlineIndexConfig {
        public BuildConfig build_config = new BuildConfig();
    }

    public static class BuildConfig {
        public int max_doc_count = 10000;
    }

    @Override
    public String toString() {
        return JsonPrettyFormatter.toJsonString(this);
    }
}
