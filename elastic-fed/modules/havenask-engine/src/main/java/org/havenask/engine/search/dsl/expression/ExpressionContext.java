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

package org.havenask.engine.search.dsl.expression;

import org.havenask.common.Nullable;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.engine.search.internal.HavenaskScroll;

public class ExpressionContext {

    private final NamedXContentRegistry namedXContentRegistry;
    private final HavenaskScroll havenaskScroll;
    private final int shardNum;

    public ExpressionContext(
        @Nullable NamedXContentRegistry namedXContentRegistry,
        @Nullable HavenaskScroll havenaskScroll,
        @Nullable int shardNum
    ) {
        this.namedXContentRegistry = namedXContentRegistry;
        this.havenaskScroll = havenaskScroll;
        this.shardNum = shardNum;
    }

    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }

    public HavenaskScroll getHavenaskScroll() {
        return havenaskScroll;
    }

    public int getShardNum() {
        return shardNum;
    }
}
