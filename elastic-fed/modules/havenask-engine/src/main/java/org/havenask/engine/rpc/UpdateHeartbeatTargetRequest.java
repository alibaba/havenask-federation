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
import org.havenask.common.collect.Map;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;

public class UpdateHeartbeatTargetRequest implements ToXContentObject {

    public static final ParseField SIGNATURE_FIELD = new ParseField("signature");
    public static final ParseField CUSTOM_INFO_FIELD = new ParseField("customInfo");
    public static final ParseField GLOBAL_CUSTOM_INFO_FIELD = new ParseField("globalCustomInfo");

    public TargetInfo getTargetInfo() {
        return targetInfo;
    }

    private final TargetInfo targetInfo;

    public UpdateHeartbeatTargetRequest(TargetInfo targetInfo) {
        this.targetInfo = targetInfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SIGNATURE_FIELD.getPreferredName(), targetInfo.toString());
        builder.field(CUSTOM_INFO_FIELD.getPreferredName(), targetInfo.toString());
        builder.field(GLOBAL_CUSTOM_INFO_FIELD.getPreferredName(), Map.of(CUSTOM_INFO_FIELD.getPreferredName(), targetInfo.toString()));
        builder.endObject();
        return builder;
    }
}
