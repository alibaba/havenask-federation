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

package org.havenask.engine.rpc;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.annotation.JSONField;

import org.havenask.engine.util.JsonPrettyFormatter;

public class TargetInfo {
    public AppInfo app_info;
    public BizInfo biz_info;
    public CustomAppInfo custom_app_info;
    public ServiceInfo service_info;
    public Map<String, Map<String, TableInfo>> table_info;
    public Boolean clean_disk;
    public Integer target_version;
    public String catalog_address;

    public static class AppInfo {
        public String config_path;
        public Integer keep_count;
    }

    public static class BizInfo {
        public static class Biz {
            public String config_path;
            public CustomBizInfo custom_biz_info;
            public Integer keep_count;
        }

        public static class CustomBizInfo {

        }

        @JSONField(name = "default")
        public Biz default_biz;

        public BizInfo() {

        }

        public BizInfo(Path defaultConfigPath) {
            default_biz = new Biz();
            default_biz.config_path = defaultConfigPath.toString();
        }
    }

    public static class CustomAppInfo {

    }

    public static class ServiceInfo {
        public static class Service {
            public String topo_info;
        }

        public Service cm2;

        public String zone_name;
        public Integer part_id;
        public Integer part_count;
        public Integer version;
        public Map<String, List<Cm2Config>> cm2_config;

        public static class Cm2Config {
            public Integer part_count;
            public String biz_name;
            public String ip;
            public Integer version;
            public Integer part_id;
            public Integer tcp_port;
            public Boolean support_heartbeat;
            public Integer grpc_port;
        }

        public ServiceInfo() {

        }

        public ServiceInfo(String zone_name, int part_id, int part_count, int version) {
            this.zone_name = zone_name;
            this.part_id = part_id;
            this.part_count = part_count;
            this.version = version;
        }

        public ServiceInfo(String zone_name, int part_id, int part_count) {
            this.zone_name = zone_name;
            this.part_id = part_id;
            this.part_count = part_count;
        }
    }

    public static class TableInfo {
        public static class Partition {
            public static class DeployStatus {
                public Integer deploy_status;
                public String local_config_path;
            }

            public String check_index_path;
            public Integer deploy_status;
            public List<List<Object>> deploy_status_map;
            public Integer inc_version;
            public Integer keep_count;
            public String loaded_config_path;
            public String loaded_index_root;
            public String local_index_path;
            public Integer rt_status;
            public Integer schema_version;
            public Integer table_load_type;
            public Integer table_status;
            public Integer table_type;
        }

        public Integer table_mode;
        public Integer table_type;
        public Integer total_partition_count;
        public String config_path;
        public Boolean force_online;
        public String group_name;
        public String index_root;
        public Map<String, Partition> partitions;
        public String raw_index_root;
        public Integer rt_status;
        public Long timestamp_to_skip;

        public TableInfo() {

        }

        public TableInfo(
            Integer tableMode,
            Integer tableType,
            String configPath,
            String indexRoot,
            Integer totalPartitionCount,
            String curPartitionName,
            Partition curPartition
        ) {
            table_mode = tableMode;
            table_type = tableType;
            config_path = configPath;
            index_root = indexRoot;
            total_partition_count = totalPartitionCount;
            partitions = new HashMap<>();
            partitions.put(curPartitionName, curPartition);
        }
    }

    public static class CatalogAddress {
        public String catalog_address;
    }

    public static TargetInfo parse(String json) {
        return JsonPrettyFormatter.fromJsonString(json, TargetInfo.class);
    }

    @Override
    public String toString() {
        return JsonPrettyFormatter.toJsonString(this);
    }
}
