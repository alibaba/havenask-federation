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

import java.util.Arrays;
import java.util.List;

import org.havenask.engine.index.config.BizConfig.HashMode;
import org.havenask.engine.util.JsonPrettyFormatter;

/**
 * {
 *     "dependency_table": [
 *         "in0"
 *     ],
 *     "cluster_config" : {
 *         "hash_mode" : {
 *             "hash_field" : "_id",
 *             "hash_function" : "HASH"
 *         },
 *         "query_config" : {
 *             "default_index" : "title",
 *             "default_operator" : "AND"
 *         },
 *         "table_name" : "in0"
 *     }
 * }
 */
public class ZoneBiz {
    public List<String> dependency_table = Arrays.asList("in0");
    public ClusterConfig cluster_config = new ClusterConfig();

    public static class ClusterConfig {
        public String table_name = "in0";
        public HashMode hash_mode = new HashMode("_id", "HASH");
        public QueryConfig query_config = new QueryConfig("title", "AND");
    }

    public static class QueryConfig {
        public String default_index;
        public String default_operator;

        public QueryConfig(String default_index, String default_operator) {
            this.default_index = default_index;
            this.default_operator = default_operator;
        }
    }

    @Override
    public String toString() {
        return JsonPrettyFormatter.toJsonString(this);
    }
}
