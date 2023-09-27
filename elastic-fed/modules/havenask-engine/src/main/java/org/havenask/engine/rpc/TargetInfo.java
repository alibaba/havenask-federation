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
import java.util.Objects;

import com.alibaba.fastjson.annotation.JSONField;

import org.havenask.engine.util.JsonPrettyFormatter;

public class TargetInfo {
    public AppInfo app_info;
    public BizInfo biz_info;
    public CustomAppInfo custom_app_info;
    public ServiceInfo service_info;
    public Map<String, Map<String, TableInfo>> table_info;
    public Boolean clean_disk;
    public int target_version;
    public String catalog_address;

    public static class AppInfo {
        public String config_path;
        public int keep_count;
    }

    public static class BizInfo {
        public static class Biz {
            public String config_path;
            public CustomBizInfo custom_biz_info;
            public int keep_count;
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
        public int part_id;
        public int part_count;
        public int version;
        public Map<String, List<cm2Config>> cm2_config;

        public static class cm2Config {
            public int part_count;
            public String biz_name;
            public String ip;
            public int version;
            public int part_id;
            public int tcp_port;
            public boolean support_heartbeat;
            public int grpc_port;
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
                public int deploy_status;
                public String local_config_path;
            }

            public String check_index_path;
            public int deploy_status;
            public List<List<Object>> deploy_status_map;
            public int inc_version;
            public int keep_count;
            public String loaded_config_path;
            public String loaded_index_root;
            public String local_index_path;
            public int rt_status;
            public int schema_version;
            public int table_load_type;
            public int table_status;
            public int table_type;
        }

        public int table_mode;
        public int table_type;
        public int total_partition_count;
        public String config_path;
        public boolean force_online;
        public String group_name;
        public String index_root;
        public Map<String, Partition> partitions;
        public String raw_index_root;
        public int rt_status;
        public long timestamp_to_skip;

        public TableInfo() {

        }

        public TableInfo(
            int tableMode,
            int tableType,
            String configPath,
            String indexRoot,
            int totalPartitionCount,
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        TargetInfo that = (TargetInfo)o;
        return target_version == that.target_version && Objects.equals(app_info, that.app_info)
            && Objects.equals(biz_info, that.biz_info) && Objects.equals(custom_app_info,
            that.custom_app_info) && Objects.equals(service_info, that.service_info) && Objects.equals(
            table_info, that.table_info) && Objects.equals(clean_disk, that.clean_disk)
            && Objects.equals(catalog_address, that.catalog_address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(app_info, biz_info, custom_app_info, service_info, table_info, clean_disk, target_version,
            catalog_address);
    }
}
