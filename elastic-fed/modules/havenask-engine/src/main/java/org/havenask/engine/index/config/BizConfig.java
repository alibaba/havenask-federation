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
    public boolean direct_write = true;
    public WalConfig wal_config = new WalConfig();
    public OnlineIndexConfig online_index_config = new OnlineIndexConfig();
    public BackgroundTaskConfig background_task_config = new BackgroundTaskConfig();
    public boolean realtime = true;

    public static class OnlineIndexConfig {
        public boolean on_disk_flush_realtime_index = true;
        public boolean enable_async_dump_segment = true;
        public int max_realtime_dump_interval = 600;
        public BuildConfig build_config = new BuildConfig();
    }

    public static class WalConfig {
        public int timeout_ms = 10000;
        public SinkConfig sink = new SinkConfig();
        public String strategy = "queue";
    }

    public static class SinkConfig {
        public String queue_name;
        public String queue_size = "5000";
    }

    public static class BuildOptionConfig {
        public boolean async_build = true;
        public int async_queue_size = 50000;
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

    public static class MergeConfig {
        public String merge_strategy = "combined";
        public MergeStrategyConfig merge_strategy_params = new MergeStrategyConfig();
    }

    public static class MergeStrategyConfig {
        public String input_limits = "max-segment-size=5120";
        public String strategy_conditions = "priority-feature=valid-doc-count#asc;conflict-segment-count=10;conflict-delete-percent=30";
        public String output_limits = "max-merged-segment-size=13312;max-total-merged-size=15360;"
            + "max-small-segment-count=10;merge-size-upperbound=256;merge-size-lowerbound=64";
    }

    public static class BuildConfig {
        public int max_doc_count = 10000;
        public int building_memory_limit_mb = 1024;
        public int keep_version_count = 2;
        public int keep_version_hour = 1;
    }

    public static class BackgroundTaskConfig {
        public int dump_interval_ms = 60000;
    }

    public static class OfflineIndexConfig {
        public BuildConfig build_config = new BuildConfig();
        public MergeConfig merge_config = new MergeConfig();
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
