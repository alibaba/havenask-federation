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

import java.util.List;
import java.util.Objects;

import org.havenask.engine.util.JsonPrettyFormatter;

public class BizConfig {
    public BuildOptionConfig build_option_config = new BuildOptionConfig();
    public ClusterConfig cluster_config = new ClusterConfig();
    public OfflineIndexConfig offline_index_config = new OfflineIndexConfig();
    public boolean realtime = false;

    public static class BuildOptionConfig {
        public boolean async_build = true;
        public int async_queue_size = 1000;
        public boolean document_filter = true;
        public int max_recover_time = 30;
        public boolean sort_build = false;
        public List<SortConfig> sort_descriptions = List.of();
        public int sort_queue_mem = 4096;
        public int sort_queue_size = 10000000;
    }

    public static class SortConfig {
        public SortConfig(String sort_field, String sort_pattern) {
            this.sort_field = sort_field;
            this.sort_pattern = sort_pattern;
        }

        public String sort_field;
        public String sort_pattern;
    }

    public static class BuildConfig {
        public int build_total_memory = 5120;
        public int keep_version_count = 40;
    }

    public static class OfflineIndexConfig {
        public BuildConfig build_config = new BuildConfig();
    }

    public static class ClusterConfig {
        public BuilderRuleConfig builder_rule_config = new BuilderRuleConfig();
        public String cluster_name;
        public String table_name;
        public HashMode hash_mode = new HashMode("_id", "HASH");
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

    public static class BuilderRuleConfig {
        public boolean batch_mode = false;
        public int build_parallel_num = 1;
        public int merge_parallel_num = 1;
        public int partition_count = 1;
    }

    @Override
    public String toString() {
        return JsonPrettyFormatter.toJsonString(this);
    }
}
