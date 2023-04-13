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

import java.util.Objects;
import java.util.Set;

import org.havenask.engine.index.config.BizConfig.HashMode;
import org.havenask.engine.util.JsonPrettyFormatter;

/**
 * {
 *   "turing_options_config": {
 *     "dependency_table": [
 *       "in0"
 *     ]
 *   },
 *   "cluster_config": {
 *     "hash_mode": {
 *       "hash_field": "docid",
 *       "hash_function": "HASH"
 *     },
 *     "query_config": {
 *       "default_index": "title",
 *       "default_operator": "AND"
 *     },
 *     "table_name": "in0"
 *   }
 * }
 */
public class ZoneBiz {
    public TuringOptionsConfig turing_options_config = new TuringOptionsConfig();
    public ClusterConfig cluster_config = new ClusterConfig();

    public static class ClusterConfig {
        public String table_name = "in0";
        public HashMode hash_mode = new HashMode("_id", "HASH");
        public QueryConfig query_config = new QueryConfig("title", "AND");

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClusterConfig that = (ClusterConfig) o;
            return Objects.equals(table_name, that.table_name)
                && Objects.equals(hash_mode, that.hash_mode)
                && Objects.equals(query_config, that.query_config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(table_name, hash_mode, query_config);
        }
    }

    public static class TuringOptionsConfig {
        public Set<String> dependency_table = Set.of("in0");

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TuringOptionsConfig that = (TuringOptionsConfig) o;
            return Objects.equals(dependency_table, that.dependency_table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dependency_table);
        }
    }

    public static class QueryConfig {
        public String default_index;
        public String default_operator;

        public QueryConfig(String default_index, String default_operator) {
            this.default_index = default_index;
            this.default_operator = default_operator;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QueryConfig that = (QueryConfig) o;
            return Objects.equals(default_index, that.default_index) && Objects.equals(default_operator, that.default_operator);
        }

        @Override
        public int hashCode() {
            return Objects.hash(default_index, default_operator);
        }
    }

    public static ZoneBiz parse(String json) {
        return JsonPrettyFormatter.fromJsonString(json, ZoneBiz.class);
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
        ZoneBiz zoneBiz = (ZoneBiz) o;
        return Objects.equals(turing_options_config, zoneBiz.turing_options_config)
            && Objects.equals(cluster_config, zoneBiz.cluster_config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(turing_options_config, cluster_config);
    }
}
