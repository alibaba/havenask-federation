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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.havenask.common.ParseField;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParser.Token;

public class TargetInfo implements ToXContentObject {
    public Map<String, Map<String, TableInfo>> table_info;
    public BizInfo biz_info;
    public ServiceInfo service_info;
    public boolean clean_disk;

    private static final ParseField TABLE_INFO_FIELD = new ParseField("table_info");
    private static final ParseField BIZ_INFO_FIELD = new ParseField("biz_info");
    private static final ParseField SERVICE_INFO_FIELD = new ParseField("service_info");
    private static final ParseField CLEAN_DISK_FIELD = new ParseField("clean_disk");

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TargetInfo that = (TargetInfo)o;
        return clean_disk == that.clean_disk
            && table_info.equals(that.table_info)
            && biz_info.equals(that.biz_info)
            && service_info.equals(that.service_info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table_info, biz_info, service_info, clean_disk);
    }

    public static TargetInfo fromXContent(XContentParser parser) throws IOException {
        TargetInfo targetInfo = new TargetInfo();
        String currentFieldName = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (TABLE_INFO_FIELD.getPreferredName().equals(currentFieldName)) {
                Map<String, Map<String, TableInfo>> tableInfos = new TreeMap<>();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                        Map<String, TableInfo> tableMap = new TreeMap<>();
                        tableInfos.put(currentFieldName, tableMap);
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                            TableInfo tableInfo = TableInfo.fromXContent(parser);
                            tableMap.put(currentFieldName, tableInfo);
                        }
                    }
                }
            } else if (BIZ_INFO_FIELD.getPreferredName().equals(currentFieldName)) {
                targetInfo.biz_info = BizInfo.fromXContent(parser);
            } else if (SERVICE_INFO_FIELD.getPreferredName().equals(currentFieldName)) {
                targetInfo.service_info = ServiceInfo.fromXContent(parser);
            } else if (CLEAN_DISK_FIELD.getPreferredName().equals(currentFieldName)) {
                targetInfo.clean_disk = parser.booleanValue();
            }
        }
        return targetInfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(TABLE_INFO_FIELD.getPreferredName());
        for (Entry<String, Map<String, TableInfo>> entry : table_info.entrySet()) {
            String tableName = entry.getKey();
            builder.startObject(tableName);
            Map<String, TableInfo> map = entry.getValue();
            for (Entry<String, TableInfo> e : map.entrySet()) {
                String version = e.getKey();
                TableInfo tableInfo = e.getValue();
                builder.field(version, tableInfo);
            }
            builder.endObject();
        }
        builder.endObject();
        builder.field(BIZ_INFO_FIELD.getPreferredName(), biz_info);
        builder.field(SERVICE_INFO_FIELD.getPreferredName(), service_info);
        builder.field(CLEAN_DISK_FIELD.getPreferredName(), clean_disk);
        builder.endObject();
        return builder;
    }

    public List<Cm2Config> findExistedBizService(List<Cm2Config> services) {
        if (service_info == null || service_info.cm2_config == null) {
            return null;
        }
        List<Cm2Config> existed = service_info.cm2_config.get("local");
        if (existed == null) {
            return null;
        }
        List<Cm2Config> found = new LinkedList<>();
        Set<String> existedBizs = services.stream().map(cm2Config -> cm2Config.biz_name).collect(Collectors.toSet());
        for (Cm2Config cm : existed) {
            if (existedBizs.contains(cm)) {
                found.add(cm);
            }
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
        if (service_info == null || service_info.cm2_config == null) {
            return false;
        }
        return service_info.cm2_config.get("local").removeAll(services);
    }

    public List<Cm2Config> removeAllBizService() {
        if (service_info == null || service_info.cm2_config == null) {
            return null;
        }
        return service_info.cm2_config.put("local", new LinkedList<>());
    }

    public List<Cm2Config> removeBizByPrefix(String prefix) {
        if (service_info == null || service_info.cm2_config == null) {
            return null;
        }
        List<Cm2Config> current = service_info.cm2_config.get("local");
        if (current == null) {
            return null;
        }
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

    public static class ServiceInfo implements ToXContentObject {
        private static final ParseField ZONE_NAME_FIELD = new ParseField("zone_name");
        private static final ParseField VERSION_FIELD = new ParseField("version");
        private static final ParseField PART_COUNT_FIELD = new ParseField("part_count");
        private static final ParseField PART_ID_FIELD = new ParseField("part_id");

        private static final ConstructingObjectParser<ServiceInfo, Void> PARSER = new ConstructingObjectParser<>(
            ServiceInfo.class.getName(),
            true,
            args -> {
                return new ServiceInfo((String)args[0], (int)args[1], (int)args[2], (int)args[3]);
            }
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), ZONE_NAME_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), PART_COUNT_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), PART_ID_FIELD);

        }

        public ServiceInfo() {}

        public ServiceInfo(String zone_name) {
            this.zone_name = zone_name;
        }

        public ServiceInfo(String zone_name, int version, int part_count, int part_id) {
            this.zone_name = zone_name;
            this.version = version;
            this.part_count = part_count;
            this.part_id = part_id;
        }

        public String zone_name;
        public int version = 0;
        public int part_count = 1;
        public int part_id = 0;
        public Map<String, List<Cm2Config>> cm2_config;

        public static ServiceInfo fromXContent(XContentParser parser) throws IOException {
            ServiceInfo serviceInfo = new ServiceInfo();
            String currentFieldName = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (ZONE_NAME_FIELD.getPreferredName().equals(currentFieldName)) {
                    serviceInfo.zone_name = parser.text();
                } else if (VERSION_FIELD.getPreferredName().equals(currentFieldName)) {
                    serviceInfo.version = parser.intValue();
                } else if (PART_COUNT_FIELD.getPreferredName().equals(currentFieldName)) {
                    serviceInfo.part_count = parser.intValue();
                } else if (PART_ID_FIELD.getPreferredName().equals(currentFieldName)) {
                    serviceInfo.part_count = parser.intValue();
                }
            }
            return serviceInfo;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ZONE_NAME_FIELD.getPreferredName(), zone_name);
            builder.field(VERSION_FIELD.getPreferredName(), version);
            builder.field(PART_COUNT_FIELD.getPreferredName(), part_count);
            builder.field(PART_ID_FIELD.getPreferredName(), part_id);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ServiceInfo that = (ServiceInfo)o;
            return version == that.version
                && part_count == that.part_count
                && part_id == that.part_id
                && zone_name.equals(that.zone_name)
                && cm2_config.equals(that.cm2_config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(zone_name, version, part_count, part_id, cm2_config);
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
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Cm2Config cm2Config = (Cm2Config)o;
            return part_count == cm2Config.part_count
                && part_id == cm2Config.part_id
                && tcp_port == cm2Config.tcp_port
                && version == cm2Config.version
                && biz_name.equals(cm2Config.biz_name)
                && ip.equals(cm2Config.ip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(biz_name, part_count, part_id, tcp_port, version, ip);
        }
    }

    public static class BizInfo implements ToXContentObject {
        private static final ParseField DEFAULT_NAME_FIELD = new ParseField("default");
        private static final ParseField CONFIG_PATH_FIELD = new ParseField("config_path");

        public String config_path;

        public BizInfo() {}

        public BizInfo(String configPath) {
            config_path = configPath;
        }

        public static BizInfo fromXContent(XContentParser parser) throws IOException {
            BizInfo bizInfo = new BizInfo();
            String currentFieldName = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (DEFAULT_NAME_FIELD.getPreferredName().equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (CONFIG_PATH_FIELD.getPreferredName().equals(currentFieldName)) {
                            bizInfo.config_path = parser.text();
                        }
                    }
                }
            }
            return bizInfo;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(DEFAULT_NAME_FIELD.getPreferredName());
            builder.field(CONFIG_PATH_FIELD.getPreferredName(), config_path);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {return true;}
            if (o == null || getClass() != o.getClass()) {return false;}
            BizInfo bizInfo = (BizInfo)o;
            return config_path.equals(bizInfo.config_path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config_path);
        }
    }

    public static class TableInfo implements ToXContentObject {
        public String index_root;
        public Partition partition;
        public String config_path;

        private static final ParseField INDEX_ROOT_FIELD = new ParseField("index_root");
        private static final ParseField CONFIG_PATH_FIELD = new ParseField("config_path");
        private static final ParseField PARTITIONS_FIELD = new ParseField("partitions");

        public TableInfo() {}

        public TableInfo(String indexRoot, String configPath) {
            index_root = indexRoot;
            config_path = configPath;
            partition = new Partition();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableInfo tableInfo = (TableInfo)o;
            return index_root.equals(tableInfo.index_root)
                && partition.equals(tableInfo.partition)
                && config_path.equals(tableInfo.config_path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index_root, partition, config_path);
        }

        public static TableInfo fromXContent(XContentParser parser) throws IOException {
            TableInfo tableInfo = new TableInfo();
            String currentFieldName = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (INDEX_ROOT_FIELD.getPreferredName().equals(currentFieldName)) {
                    tableInfo.index_root = parser.text();
                } else if (CONFIG_PATH_FIELD.getPreferredName().equals(currentFieldName)) {
                    tableInfo.config_path = parser.text();
                } else if (PARTITIONS_FIELD.getPreferredName().equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if ("0_65535".equals(currentFieldName)) {
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if ("inc_version".equals(currentFieldName)) {
                                    tableInfo.partition = new Partition(parser.intValue());
                                }
                            }
                        }
                    }
                } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    parser.skipChildren();
                }
            }
            return tableInfo;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_ROOT_FIELD.getPreferredName(), index_root);
            builder.startObject(PARTITIONS_FIELD.getPreferredName());
            builder.startObject("0_65535");
            builder.field("inc_version", partition.inc_version);
            builder.endObject();
            builder.endObject();
            builder.field(CONFIG_PATH_FIELD.getPreferredName(), config_path);
            builder.endObject();
            return builder;
        }
    }

    public static class Partition {
        public int inc_version = 1;

        public Partition() {}

        public Partition(int inc_version) {
            this.inc_version = inc_version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Partition partition = (Partition)o;
            return inc_version == partition.inc_version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(inc_version);
        }
    }

    public static TargetInfo createSearchDefault(String zone, String indexRoot, String tableConf, String bizConf) {
        TargetInfo targetInfo = new TargetInfo();

        Map<String, Map<String, TableInfo>> tables = new HashMap<>();
        Map<String, TableInfo> tableInfoMap = new HashMap<>();
        tables.put(zone, tableInfoMap);
        tableInfoMap.put("0", new TableInfo(indexRoot, tableConf));
        targetInfo.table_info = tables;

        targetInfo.biz_info = new BizInfo(bizConf);

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
        targetInfo.biz_info = new BizInfo(bizConf);

        return targetInfo;
    }

}
