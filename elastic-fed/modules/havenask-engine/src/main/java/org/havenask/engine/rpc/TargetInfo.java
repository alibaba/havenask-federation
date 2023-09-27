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
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AppInfo appInfo = (AppInfo) o;
            return keep_count == appInfo.keep_count && Objects.equals(config_path, appInfo.config_path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config_path, keep_count);
        }

        public String config_path;
        public int keep_count;
    }

    public static class BizInfo {
        public static class Biz {
            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Biz biz = (Biz) o;
                return keep_count == biz.keep_count
                    && Objects.equals(config_path, biz.config_path)
                    && Objects.equals(custom_biz_info, biz.custom_biz_info);
            }

            @Override
            public int hashCode() {
                return Objects.hash(config_path, custom_biz_info, keep_count);
            }

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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BizInfo bizInfo = (BizInfo) o;
            return Objects.equals(default_biz, bizInfo.default_biz);
        }

        @Override
        public int hashCode() {
            return Objects.hash(default_biz);
        }
    }

    public static class CustomAppInfo {

    }

    public static class ServiceInfo {
        public static class Service {
            public String topo_info;

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Service service = (Service) o;
                return Objects.equals(topo_info, service.topo_info);
            }

            @Override
            public int hashCode() {
                return Objects.hash(topo_info);
            }
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

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                cm2Config cm2Config = (cm2Config) o;
                return part_count == cm2Config.part_count
                    && version == cm2Config.version
                    && part_id == cm2Config.part_id
                    && tcp_port == cm2Config.tcp_port
                    && support_heartbeat == cm2Config.support_heartbeat
                    && grpc_port == cm2Config.grpc_port
                    && Objects.equals(biz_name, cm2Config.biz_name)
                    && Objects.equals(ip, cm2Config.ip);
            }

            @Override
            public int hashCode() {
                return Objects.hash(part_count, biz_name, ip, version, part_id, tcp_port, support_heartbeat, grpc_port);
            }
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ServiceInfo that = (ServiceInfo) o;
            return part_id == that.part_id
                && part_count == that.part_count
                && version == that.version
                && Objects.equals(cm2, that.cm2)
                && Objects.equals(zone_name, that.zone_name)
                && Objects.equals(cm2_config, that.cm2_config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cm2, zone_name, part_id, part_count, version, cm2_config);
        }
    }

    public static class TableInfo {
        public static class Partition {
            public static class DeployStatus {
                public int deploy_status;
                public String local_config_path;

                @Override
                public boolean equals(Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }
                    DeployStatus that = (DeployStatus) o;
                    return deploy_status == that.deploy_status && Objects.equals(local_config_path, that.local_config_path);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(deploy_status, local_config_path);
                }
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

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Partition partition = (Partition) o;
                return deploy_status == partition.deploy_status
                    && inc_version == partition.inc_version
                    && keep_count == partition.keep_count
                    && rt_status == partition.rt_status
                    && schema_version == partition.schema_version
                    && table_load_type == partition.table_load_type
                    && table_status == partition.table_status
                    && table_type == partition.table_type
                    && Objects.equals(check_index_path, partition.check_index_path)
                    && Objects.equals(deploy_status_map, partition.deploy_status_map)
                    && Objects.equals(loaded_config_path, partition.loaded_config_path)
                    && Objects.equals(loaded_index_root, partition.loaded_index_root)
                    && Objects.equals(local_index_path, partition.local_index_path);
            }

            @Override
            public int hashCode() {
                return Objects.hash(
                    check_index_path,
                    deploy_status,
                    deploy_status_map,
                    inc_version,
                    keep_count,
                    loaded_config_path,
                    loaded_index_root,
                    local_index_path,
                    rt_status,
                    schema_version,
                    table_load_type,
                    table_status,
                    table_type
                );
            }
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableInfo tableInfo = (TableInfo) o;
            return table_mode == tableInfo.table_mode
                && table_type == tableInfo.table_type
                && total_partition_count == tableInfo.total_partition_count
                && force_online == tableInfo.force_online
                && rt_status == tableInfo.rt_status
                && timestamp_to_skip == tableInfo.timestamp_to_skip
                && Objects.equals(config_path, tableInfo.config_path)
                && Objects.equals(group_name, tableInfo.group_name)
                && Objects.equals(index_root, tableInfo.index_root)
                && Objects.equals(partitions, tableInfo.partitions)
                && Objects.equals(raw_index_root, tableInfo.raw_index_root);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                table_mode,
                table_type,
                total_partition_count,
                config_path,
                force_online,
                group_name,
                index_root,
                partitions,
                raw_index_root,
                rt_status,
                timestamp_to_skip
            );
        }
    }

    public static class CatalogAddress {
        public String catalog_address;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CatalogAddress that = (CatalogAddress) o;
            return Objects.equals(catalog_address, that.catalog_address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog_address);
        }
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TargetInfo that = (TargetInfo) o;
        return target_version == that.target_version
            && Objects.equals(app_info, that.app_info)
            && Objects.equals(biz_info, that.biz_info)
            && Objects.equals(custom_app_info, that.custom_app_info)
            && Objects.equals(service_info, that.service_info)
            && Objects.equals(table_info, that.table_info)
            && Objects.equals(clean_disk, that.clean_disk)
            && Objects.equals(catalog_address, that.catalog_address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(app_info, biz_info, custom_app_info, service_info, table_info, clean_disk, target_version, catalog_address);
    }
}
