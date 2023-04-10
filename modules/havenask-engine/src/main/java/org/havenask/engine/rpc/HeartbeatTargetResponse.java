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

import java.io.IOException;

import org.havenask.common.ParseField;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParser.Token;
import org.havenask.engine.index.config.TargetInfo;
import org.havenask.engine.index.config.TargetInfo.ServiceInfo;

import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class HeartbeatTargetResponse implements ToXContentObject {
    private static final ParseField SIGNATURE_FIELD = new ParseField("signature");
    private static final ParseField CUSTOM_INFO_FIELD = new ParseField("customInfo");
    private static final ParseField SERVICE_INFO_FIELD = new ParseField("serviceInfo");

    private final TargetInfo customInfo;
    private final ServiceInfo serviceInfo;
    private final TargetInfo signature;

    public HeartbeatTargetResponse(TargetInfo customInfo, ServiceInfo serviceInfo, TargetInfo signature) {
        this.customInfo = customInfo;
        this.serviceInfo = serviceInfo;
        this.signature = signature;
    }

    public TargetInfo getCustomInfo() {
        return customInfo;
    }

    public ServiceInfo getServiceInfo() {
        return serviceInfo;
    }

    public TargetInfo getSignature() {
        return signature;
    }

    public static HeartbeatTargetResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        String currentFieldName = parser.currentName();
        TargetInfo customInfo = null;
        ServiceInfo serviceInfo = null;
        TargetInfo signature = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                if (CUSTOM_INFO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    customInfo = TargetInfo.fromXContent(parser);
                } else if (SERVICE_INFO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    serviceInfo = ServiceInfo.fromXContent(parser);
                } else if (SIGNATURE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    signature = TargetInfo.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new HeartbeatTargetResponse(customInfo, serviceInfo, signature);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CUSTOM_INFO_FIELD.getPreferredName(), customInfo);
        builder.field(SERVICE_INFO_FIELD.getPreferredName(), serviceInfo);
        builder.field(SIGNATURE_FIELD.getPreferredName(), signature);
        builder.endObject();
        return builder;
    }
}
