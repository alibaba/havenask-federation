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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TargetInfo {
    public Map<String, Map<String, TableInfo>> table_info;
    public Map<String, BizInfo> biz_info;
    public ServiceInfo service_info;
    public boolean clean_disk;

    public List<Cm2Config> findExistedBizService(List<Cm2Config> services) {
        if (service_info == null || service_info.cm2_config == null) return null;
        List<Cm2Config> existed = service_info.cm2_config.get("local");
        if (existed == null) return null;
        List<Cm2Config> found = new LinkedList<>();
        Set<String> existedBizs = services.stream().map(cm2Config -> cm2Config.biz_name).collect(Collectors.toSet());
        for (Cm2Config cm : existed) {
            if (existedBizs.contains(cm)) found.add(cm);
        }
        return found;
    }

    public void addBizService(List<Cm2Config> services) {
        if (service_info == null) {
            service_info = new ServiceInfo("qrs");
            service_info.part_count = 0;
        }
        if (service_info.cm2_config == null) {
            service_info.cm2_config = new HashMap<>();
            service_info.cm2_config.put("local", new LinkedList<>());
        }
        List<Cm2Config> existed = service_info.cm2_config.get("local");
        existed.addAll(services);
    }

    public boolean removeBizService(List<Cm2Config> services) {
        if (service_info == null || service_info.cm2_config == null) return false;
        return service_info.cm2_config.get("local").removeAll(services);
    }

    public List<Cm2Config> removeAllBizService() {
        if (service_info == null || service_info.cm2_config == null) return null;
        return service_info.cm2_config.put("local", new LinkedList<>());
    }

    public List<Cm2Config> removeBizByPrefix(String prefix) {
        if (service_info == null || service_info.cm2_config == null) return null;
        List<Cm2Config> current = service_info.cm2_config.get("local");
        if (current == null) return null;
        List<Cm2Config> removed = new LinkedList<>();
        Iterator<Cm2Config> itr = current.iterator();
        while (itr.hasNext()) {
            Cm2Config cm = itr.next();
            if (cm.biz_name.startsWith(prefix)) {
                itr.remove();
                removed.add(cm);
            }
        }
        return removed;
    }

    public static class ServiceInfo {
        public String zone_name;
        public int version = 0;
        public int part_count = 1;
        public int part_id = 0;
        public Map<String, List<Cm2Config>> cm2_config;

        public ServiceInfo() {}

        public ServiceInfo(String zone) {
            zone_name = zone;
        }
    }

    public static class Cm2Config {
        public String biz_name;
        public int part_count;
        public long part_id;
        public int tcp_port;
        public long version;
        public String ip;

        public Cm2Config() {}

        public Cm2Config(String biz_name, int tcp_port, long version, String ip) {
            this.biz_name = biz_name;
            this.tcp_port = tcp_port;
            this.version = version;
            this.ip = ip;
            this.part_count = 1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Cm2Config)) {
                return false;
            }
            Cm2Config cm2Config = (Cm2Config) o;
            return Objects.equals(biz_name, cm2Config.biz_name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(biz_name);
        }
    }

    public static class BizInfo {
        public String config_path;

        public BizInfo() {}

        public BizInfo(String configPath) {
            config_path = configPath;
        }
    }

    public static class TableInfo {
        public String index_root;
        public Map<String, Partition> partitions;
        public String config_path;

        public TableInfo() {}

        public TableInfo(String indexRoot, String configPath) {
            index_root = indexRoot;
            config_path = configPath;
            partitions = new HashMap<>();
            partitions.put("0_65535", new Partition());
        }
    }

    public static class Partition {
        public int inc_version = 1;
    }

    public static TargetInfo createSearchDefault(String zone, String indexRoot, String tableConf, String bizConf) {
        TargetInfo targetInfo = new TargetInfo();

        Map<String, Map<String, TableInfo>> tables = new HashMap<>();
        Map<String, TableInfo> tableInfoMap = new HashMap<>();
        tables.put(zone, tableInfoMap);
        tableInfoMap.put("0", new TableInfo(indexRoot, tableConf));
        targetInfo.table_info = tables;

        targetInfo.biz_info = new HashMap<>();
        targetInfo.biz_info.put("default", new BizInfo(bizConf));

        targetInfo.service_info = new ServiceInfo(zone);

        return targetInfo;
    }

    public static TargetInfo createQrsDefault(String bizConf) {
        TargetInfo targetInfo = new TargetInfo();

        ServiceInfo serviceInfo = new ServiceInfo("qrs");
        serviceInfo.part_count = 0;
        serviceInfo.cm2_config = new HashMap<>();
        serviceInfo.cm2_config.put("local", new LinkedList<>());

        targetInfo.table_info = Collections.emptyMap();
        targetInfo.biz_info = new HashMap<>();
        targetInfo.biz_info.put("default", new BizInfo(bizConf));

        return targetInfo;
    }

}
