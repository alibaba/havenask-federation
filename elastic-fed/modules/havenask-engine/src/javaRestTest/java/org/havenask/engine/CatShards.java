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

package org.havenask.engine;

import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class CatShards {
    CatShards(ShardInfo[] shards) {
        this.shards = shards;
    }

    private ShardInfo[] shards;

    public ShardInfo[] getShards() {
        return shards;
    }

    public static class ShardInfo {
        public final String index;
        public final Integer shard;
        public final String prirep;
        public final String state;
        public final String docs;
        public final String store;
        public final String ip;
        public final String node;

        ShardInfo(String index, Integer shard, String prirep, String state, String docs, String store, String ip, String node) {
            this.index = index;
            this.shard = shard;
            this.prirep = prirep;
            this.state = state;
            this.docs = docs;
            this.store = store;
            this.ip = ip;
            this.node = node;
        }
    }

    public static CatShards fromXContent(XContentParser parser) throws IOException {
        List<ShardInfo> shardInfos = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new IOException("Expected data to start with an Array");
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IOException("Expected data to be an Object");
            }
            String index = null;
            Integer shard = null;
            String prirep = null;
            String state = null;
            String docs = null;
            String store = null;
            String ip = null;
            String node = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    switch (fieldName) {
                        case "index":
                            index = parser.text();
                            break;
                        case "shard":
                            shard = parser.intValue();
                            break;
                        case "prirep":
                            prirep = parser.text();
                            break;
                        case "state":
                            state = parser.text();
                            break;
                        case "docs":
                            docs = parser.text();
                            break;
                        case "store":
                            store = parser.text();
                            break;
                        case "ip":
                            ip = parser.text();
                            break;
                        case "node":
                            node = parser.text();
                            break;
                        default:
                            throw new IOException("Unexpected field: " + fieldName);
                    }
                }
            }
            shardInfos.add(new ShardInfo(index, shard, prirep, state, docs, store, ip, node));
        }

        return new CatShards(shardInfos.toArray(new ShardInfo[0]));
    }

    public static CatShards parse(String strResponse) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, strResponse);
        return CatShards.fromXContent(parser);
    }
}
