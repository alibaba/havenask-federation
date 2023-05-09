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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.common.geo.builders;

import org.havenask.common.geo.GeoShapeType;
import org.havenask.common.geo.parsers.ShapeParser;
import org.havenask.common.io.stream.NamedWriteable;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.Writeable.Reader;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.havenask.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public abstract class AbstractShapeBuilderTestCase<SB extends ShapeBuilder<?,?,?>> extends HavenaskTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        if (namedWriteableRegistry == null) {
            namedWriteableRegistry = new NamedWriteableRegistry(GeoShapeType.getShapeWriteables());
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /**
     * create random shape that is put under test
     */
    protected abstract SB createTestShapeBuilder();

    /**
     * mutate the given shape so the returned shape is different
     */
    protected abstract SB createMutation(SB original) throws IOException;

    /**
     * Test that creates new shape from a random test shape and checks both for equality
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB testShape = createTestShapeBuilder();
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                contentBuilder.prettyPrint();
            }
            XContentBuilder builder = testShape.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);
            try (XContentParser shapeContentParser = createParser(shuffled)) {
                shapeContentParser.nextToken();
                ShapeBuilder<?, ?, ?> parsedShape = ShapeParser.parse(shapeContentParser);
                assertNotSame(testShape, parsedShape);
                assertEquals(testShape, parsedShape);
                assertEquals(testShape.hashCode(), parsedShape.hashCode());
            }
        }
    }

    /**
     * Test serialization and deserialization of the test shape.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB testShape = createTestShapeBuilder();
            SB deserializedShape = copyShape(testShape);
            assertEquals(testShape, deserializedShape);
            assertEquals(testShape.hashCode(), deserializedShape.hashCode());
            assertNotSame(testShape, deserializedShape);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(createTestShapeBuilder(), AbstractShapeBuilderTestCase::copyShape, this::createMutation);
        }
    }

    protected static <T extends NamedWriteable> T copyShape(T original) throws IOException {
        @SuppressWarnings("unchecked")
        Reader<T> reader = (Reader<T>) namedWriteableRegistry.getReader(ShapeBuilder.class, original.getWriteableName());
        return HavenaskTestCase.copyWriteable(original, namedWriteableRegistry, reader);
    }
}
