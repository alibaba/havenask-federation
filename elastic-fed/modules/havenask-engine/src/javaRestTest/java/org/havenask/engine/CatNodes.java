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

class CatNodes {
    CatNodes(NodeInfo[] nodes) {
        this.nodes = nodes;
    }

    private NodeInfo[] nodes;

    public static class NodeInfo {
        public final String ip;
        public final Integer heapPercent;
        public final Integer ramPercent;
        public final Integer cpu;
        public final Double load1m;
        public final Double load5m;
        public final Double load15m;
        public final String nodeRole;
        public final String master;
        public final String name;

        NodeInfo(
            String ip,
            Integer heapPercent,
            Integer ramPercent,
            Integer cpu,
            Double load1m,
            Double load5m,
            Double load15m,
            String nodeRole,
            String master,
            String name
        ) {
            this.ip = ip;
            this.heapPercent = heapPercent;
            this.ramPercent = ramPercent;
            this.cpu = cpu;
            this.load1m = load1m;
            this.load5m = load5m;
            this.load15m = load15m;
            this.nodeRole = nodeRole;
            this.master = master;
            this.name = name;
        }
    }

    public NodeInfo[] getNodes() {
        return nodes;
    }

    public static CatNodes fromXContent(XContentParser parser) throws IOException {
        List<NodeInfo> nodes = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new IOException("Expected data to start with an Array");
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IOException("Expected data to be an Object");
            }
            String ip = null;
            Integer heapPercent = null;
            Integer ramPercent = null;
            Integer cpu = null;
            Double load1m = null;
            Double load5m = null;
            Double load15m = null;
            String nodeRole = null;
            String master = null;
            String name = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    switch (fieldName) {
                        case "ip":
                            ip = parser.text();
                            break;
                        case "heap.percent":
                            heapPercent = parser.intValue();
                            break;
                        case "ram.percent":
                            ramPercent = parser.intValue();
                            break;
                        case "cpu":
                            cpu = parser.intValue();
                            break;
                        case "load_1m":
                            load1m = parser.doubleValue();
                            break;
                        case "load_5m":
                            load5m = parser.doubleValue();
                            break;
                        case "load_15m":
                            load15m = parser.doubleValue();
                            break;
                        case "node.role":
                            nodeRole = parser.text();
                            break;
                        case "master":
                            master = parser.text();
                            break;
                        case "name":
                            name = parser.text();
                            break;
                        default:
                            throw new IOException("Unexpected field: " + fieldName);
                    }
                }
            }
            nodes.add(new NodeInfo(ip, heapPercent, ramPercent, cpu, load1m, load5m, load15m, nodeRole, master, name));
        }
        return new CatNodes(nodes.toArray(new NodeInfo[0]));
    }

    public static CatNodes parse(String strResponse) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, strResponse);
        return CatNodes.fromXContent(parser);
    }

}
