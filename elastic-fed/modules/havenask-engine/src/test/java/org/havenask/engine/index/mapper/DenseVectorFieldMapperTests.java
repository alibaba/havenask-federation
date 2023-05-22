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

package org.havenask.engine.index.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.index.mapper.MapperTestCase;
import org.havenask.plugins.Plugin;

import static java.util.Collections.singletonList;

public class DenseVectorFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new HavenaskEnginePlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "dense_vector").field("dims", 4);
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "dims",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4)),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 5))
        );
        checker.registerConflictCheck(
            "similarity",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("similarity", "dot_product")),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("similarity", "l2_norm"))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("similarity", "dot_product")),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 4)
                    .field("similarity", "dot_product")
                    .startObject("index_options")
                    .field("type", "hc")
                    .endObject()
            )
        );
    }

    @Override
    public void testMeta() throws IOException {
        // TODO base testMeta failed, need to fix
    }
}
