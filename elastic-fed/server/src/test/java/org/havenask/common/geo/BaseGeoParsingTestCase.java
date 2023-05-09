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

import org.havenask.common.geo.parsers.ShapeParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.geometry.utils.GeographyValidator;
import org.havenask.index.mapper.GeoShapeIndexer;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.hamcrest.HavenaskGeoAssertions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.havenask.common.geo.builders.ShapeBuilder.SPATIAL_CONTEXT;

/** Base class for all geo parsing tests */
abstract class BaseGeoParsingTestCase extends HavenaskTestCase {
    protected static final GeometryFactory GEOMETRY_FACTORY = SPATIAL_CONTEXT.getGeometryFactory();

    public abstract void testParsePoint() throws IOException, ParseException;
    public abstract void testParseMultiPoint() throws IOException, ParseException;
    public abstract void testParseLineString() throws IOException, ParseException;
    public abstract void testParseMultiLineString() throws IOException, ParseException;
    public abstract void testParsePolygon() throws IOException, ParseException;
    public abstract void testParseMultiPolygon() throws IOException, ParseException;
    public abstract void testParseEnvelope() throws IOException, ParseException;
    public abstract void testParseGeometryCollection() throws IOException, ParseException;

    protected void assertValidException(XContentBuilder builder, Class<?> expectedException) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken();
            HavenaskGeoAssertions.assertValidException(parser, expectedException);
        }
    }

    protected void assertGeometryEquals(Object expected, XContentBuilder geoJson, boolean useJTS) throws IOException, ParseException {
        try (XContentParser parser = createParser(geoJson)) {
            parser.nextToken();
            if (useJTS) {
                HavenaskGeoAssertions.assertEquals(expected, ShapeParser.parse(parser).buildS4J());
            } else {
                GeometryParser geometryParser = new GeometryParser(true, true, true);
                org.havenask.geometry.Geometry shape = geometryParser.parse(parser);
                shape = new GeoShapeIndexer(true, "name").prepareForIndexing(shape);
                HavenaskGeoAssertions.assertEquals(expected, shape);
            }
        }
    }

    protected void assertGeometryEquals(org.havenask.geometry.Geometry expected, XContentBuilder geoJson) throws IOException {
        try (XContentParser parser = createParser(geoJson)) {
            parser.nextToken();
            assertEquals(expected, new GeoJson(true, false, new GeographyValidator(false)).fromXContent(parser));
        }
    }

    protected ShapeCollection<Shape> shapeCollection(Shape... shapes) {
        return new ShapeCollection<>(Arrays.asList(shapes), SPATIAL_CONTEXT);
    }

    protected ShapeCollection<Shape> shapeCollection(Geometry... geoms) {
        List<Shape> shapes = new ArrayList<>(geoms.length);
        for (Geometry geom : geoms) {
            shapes.add(jtsGeom(geom));
        }
        return new ShapeCollection<>(shapes, SPATIAL_CONTEXT);
    }

    protected JtsGeometry jtsGeom(Geometry geom) {
        return new JtsGeometry(geom, SPATIAL_CONTEXT, false, false);
    }

}
