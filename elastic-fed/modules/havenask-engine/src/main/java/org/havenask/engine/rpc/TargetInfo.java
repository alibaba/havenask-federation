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
    }

    public static class CustomAppInfo {

    }

    public static class ServiceInfo {
        public static class Service {
            public String topo_info;
        }

        public Service cm2;
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

        public String config_path;
        public boolean force_online;
        public String group_name;
        public String index_root;
        public Map<String, Partition> partitions;
        public String raw_index_root;
        public int rt_status;
        public long timestamp_to_skip;
    }

    public static TargetInfo parse(String json) {
        return JsonPrettyFormatter.fromJsonString(json, TargetInfo.class);
    }

    @Override
    public String toString() {
        return JsonPrettyFormatter.toJsonString(this);
    }
}
