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

import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.index.config.TargetInfo.ServiceInfo;
import org.havenask.test.AbstractXContentTestCase;

public class ServiceInfoTests extends AbstractXContentTestCase<ServiceInfo> {
    @Override
    protected ServiceInfo createTestInstance() {
        return new ServiceInfo(randomAlphaOfLength(5), randomInt(), randomInt(), randomInt());
    }

    @Override
    protected ServiceInfo doParseInstance(XContentParser parser) throws IOException {
        return ServiceInfo.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
