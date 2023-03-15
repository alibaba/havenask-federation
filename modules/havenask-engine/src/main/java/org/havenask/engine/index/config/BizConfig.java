package org.havenask.engine.index.config;

public class BizConfig {
    public BuildOptionConfig build_option_config;
    public ClusterConfig cluster_config;
    public OnlineIndexConfig online_index_config;
    public OfflineIndexConfig offline_index_config;
    public boolean realtime;
    public SwiftConfig swift_config;

    public static class BuildOptionConfig {
        public boolean async_build = true;
        public int max_recover_time = 3; //TODO suez will wait to check isRecovered, locator
    }

    public static class BuildConfig {
        public Integer build_total_memory;
        public Integer max_doc_count;
        public Integer dump_thread_count = 8; //TODO from settings
    }

    public static class LoadConfig {
        public String load_strategy; //mmap, cache
    }

    public static class OnlineIndexConfig {
        public BuildConfig build_config;
        public LoadConfig[] load_config;
        public boolean on_disk_flush_realtime_index;
        public boolean load_remain_flush_realtime_index;
        public boolean enable_async_dump_segment;
        public Integer max_realtime_memory_use = 800; //for 8G test, TODO from setting
    }

    public static class OfflineIndexConfig {
        public BuildConfig build_config;
        public LoadConfig[] load_config;
    }

    public static class SwiftConfig {

    }

    public static class ClusterConfig {
        public BuilderRuleConfig builder_rule_config;
        public String cluster_name;
        public String cluster_type;
        public String table_name;
        public boolean build_in_mem;
        public HashMode hash_mode;
        public String swift_topic_name;
        public String swift_zookeeper_root;
    }

    public static class HashMode {
        public String hash_field;
        public String hash_function;

        public HashMode(String hash_field, String hash_function) {
            this.hash_field = hash_field;
            this.hash_function = hash_function;
        }
    }

    public static class ProxyRule {
        public int partition_count;
        public int replica_count;
    }

    public static class BuilderRuleConfig {
        public int partition_count = 1;
    }

}
