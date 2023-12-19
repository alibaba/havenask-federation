/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.common.xcontent.yaml;

import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContentParser;
import org.havenask.common.xcontent.support.filtering.FilterPath;

import com.fasterxml.jackson.core.JsonParser;

public class YamlXContentParser extends JsonXContentParser {

    public YamlXContentParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler, parser);
    }

    public YamlXContentParser(
            NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler,
            JsonParser parser,
            FilterPath[] includes,
            FilterPath[] excludes
    ) {
        super(xContentRegistry, deprecationHandler, parser, includes, excludes);
    }

    @Override
    public XContentType contentType() {
        return XContentType.YAML;
    }
}
