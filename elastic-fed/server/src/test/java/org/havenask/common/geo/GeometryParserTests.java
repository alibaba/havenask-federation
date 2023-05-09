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

package org.havenask.common.geo;

import org.havenask.HavenaskParseException;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParseException;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.geometry.Geometry;
import org.havenask.geometry.GeometryCollection;
import org.havenask.geometry.Line;
import org.havenask.geometry.LinearRing;
import org.havenask.geometry.Point;
import org.havenask.geometry.Polygon;
import org.havenask.test.HavenaskTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link GeometryParser}
 */
public class GeometryParserTests extends HavenaskTestCase {

    public void testGeoJsonParsing() throws Exception {

        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(100.0).value(0.0).endArray()
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken();
            GeometryFormat format = new GeometryParser(true, randomBoolean(), randomBoolean()).geometryFormat(parser);
            assertEquals(new Point(100, 0), format.fromXContent(parser));
            XContentBuilder newGeoJson = XContentFactory.jsonBuilder();
            format.toXContent(new Point(100, 10), newGeoJson, ToXContent.EMPTY_PARAMS);
            assertEquals("{\"type\":\"Point\",\"coordinates\":[100.0,10.0]}", Strings.toString(newGeoJson));
        }

        XContentBuilder pointGeoJsonWithZ = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Point")
            .startArray("coordinates").value(100.0).value(0.0).value(10.0).endArray()
            .endObject();

        try (XContentParser parser = createParser(pointGeoJsonWithZ)) {
            parser.nextToken();
            assertEquals(new Point(100, 0, 10.0), new GeometryParser(true, randomBoolean(), true).parse(parser));
        }


        try (XContentParser parser = createParser(pointGeoJsonWithZ)) {
            parser.nextToken();
            expectThrows(IllegalArgumentException.class, () -> new GeometryParser(true, randomBoolean(), false).parse(parser));
        }

        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "Polygon")
                    .startArray("coordinates")
                        .startArray()
                            .startArray().value(100.0).value(1.0).endArray()
                            .startArray().value(101.0).value(1.0).endArray()
                            .startArray().value(101.0).value(0.0).endArray()
                            .startArray().value(100.0).value(0.0).endArray()
                        .endArray()
                    .endArray()
                .endObject();

        Polygon p = new Polygon(new LinearRing(new double[]{100d, 101d, 101d, 100d, 100d}, new double[]{1d, 1d, 0d, 0d, 1d}));
        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            // Coerce should automatically close the polygon
            assertEquals(p, new GeometryParser(true, true, randomBoolean()).parse(parser));
        }

        try (XContentParser parser = createParser(polygonGeoJson)) {
            parser.nextToken();
            // No coerce - the polygon parsing should fail
            expectThrows(XContentParseException.class, () -> new GeometryParser(true, false, randomBoolean()).parse(parser));
        }
    }

    public void testWKTParsing() throws Exception {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "Point (100 0)")
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            GeometryFormat format = new GeometryParser(true, randomBoolean(), randomBoolean()).geometryFormat(parser);
            assertEquals(new Point(100, 0), format.fromXContent(parser));
            XContentBuilder newGeoJson = XContentFactory.jsonBuilder().startObject().field("val");
            format.toXContent(new Point(100, 10), newGeoJson, ToXContent.EMPTY_PARAMS);
            newGeoJson.endObject();
            assertEquals("{\"val\":\"POINT (100.0 10.0)\"}", Strings.toString(newGeoJson));
        }

        // Make sure we can parse values outside the normal lat lon boundaries
        XContentBuilder lineGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "LINESTRING (100 0, 200 10)")
            .endObject();

        try (XContentParser parser = createParser(lineGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            assertEquals(new Line(new double[]{100, 200}, new double[]{0, 10}),
                new GeometryParser(true, randomBoolean(), randomBoolean()).parse(parser));
        }
    }

    public void testNullParsing() throws Exception {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .nullField("foo")
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            GeometryFormat format = new GeometryParser(true, randomBoolean(), randomBoolean()).geometryFormat(parser);
            assertNull(format.fromXContent(parser));

            XContentBuilder newGeoJson = XContentFactory.jsonBuilder().startObject().field("val");
            // if we serialize non-null value - it should be serialized as geojson
            format.toXContent(new Point(100, 10), newGeoJson, ToXContent.EMPTY_PARAMS);
            newGeoJson.endObject();
            assertEquals("{\"val\":{\"type\":\"Point\",\"coordinates\":[100.0,10.0]}}", Strings.toString(newGeoJson));

            newGeoJson = XContentFactory.jsonBuilder().startObject().field("val");
            format.toXContent(null, newGeoJson, ToXContent.EMPTY_PARAMS);
            newGeoJson.endObject();
            assertEquals("{\"val\":null}", Strings.toString(newGeoJson));

        }
    }

    public void testUnsupportedValueParsing() throws Exception {
        XContentBuilder pointGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", 42)
            .endObject();

        try (XContentParser parser = createParser(pointGeoJson)) {
            parser.nextToken(); // Start object
            parser.nextToken(); // Field Name
            parser.nextToken(); // Field Value
            HavenaskParseException ex = expectThrows(HavenaskParseException.class,
                () -> new GeometryParser(true, randomBoolean(), randomBoolean()).parse(parser));
            assertEquals("shape must be an object consisting of type and coordinates", ex.getMessage());
        }
    }

    public void testBasics() {
        GeometryParser parser = new GeometryParser(true, randomBoolean(), randomBoolean());
        // point
        Point expectedPoint = new Point(-122.084110, 37.386637);
        testBasics(parser, mapOf("lat", 37.386637, "lon", -122.084110), expectedPoint);
        testBasics(parser, "37.386637, -122.084110", expectedPoint);
        testBasics(parser, "POINT (-122.084110 37.386637)", expectedPoint);
        testBasics(parser, Arrays.asList(-122.084110, 37.386637), expectedPoint);
        testBasics(parser, mapOf("type", "Point", "coordinates", Arrays.asList(-122.084110, 37.386637)), expectedPoint);
        // line
        Line expectedLine = new Line(new double[] { 0, 1 }, new double[] { 0, 1 });
        testBasics(parser, "LINESTRING(0 0, 1 1)", expectedLine);
        testBasics(parser,
            mapOf("type", "LineString", "coordinates", Arrays.asList(Arrays.asList(0, 0), Arrays.asList(1, 1))),
            expectedLine
        );
        // polygon
        Polygon expectedPolygon = new Polygon(new LinearRing(new double[] { 0, 1, 1, 0, 0 }, new double[] { 0, 0, 1, 1, 0 }));
        testBasics(parser, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", expectedPolygon);
        testBasics(parser,
            mapOf(
                "type",
                "Polygon",
                "coordinates",
                Arrays.asList(
                    Arrays.asList(Arrays.asList(0, 0), Arrays.asList(1, 0), Arrays.asList(1, 1), Arrays.asList(0, 1), Arrays.asList(0, 0))
                )
            ),
            expectedPolygon
        );
        // geometry collection
        testBasics(parser,
            Arrays.asList(
                Arrays.asList(-122.084110, 37.386637),
                "37.386637, -122.084110",
                "POINT (-122.084110 37.386637)",
                mapOf("type", "Point", "coordinates", Arrays.asList(-122.084110, 37.386637)),
                mapOf("type", "LineString", "coordinates", Arrays.asList(Arrays.asList(0, 0), Arrays.asList(1, 1))),
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
            ),
            new GeometryCollection<>(
                Arrays.asList(expectedPoint, expectedPoint, expectedPoint, expectedPoint, expectedLine, expectedPolygon)
            )
        );
        expectThrows(HavenaskParseException.class, () -> testBasics(parser, "not a geometry", null));
    }

    private void testBasics(GeometryParser parser, Object value, Geometry expected) {
        Geometry geometry = parser.parseGeometry(value);
        assertEquals(expected, geometry);
    }

    private static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
