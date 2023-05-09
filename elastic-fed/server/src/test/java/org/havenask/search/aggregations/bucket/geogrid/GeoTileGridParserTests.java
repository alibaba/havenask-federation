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

package org.havenask.search.aggregations.bucket.geogrid;

import org.havenask.ExceptionsHelper;
import org.havenask.common.xcontent.XContentParseException;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.geo.GeometryTestUtils;
import org.havenask.geometry.Rectangle;
import org.havenask.test.HavenaskTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class GeoTileGridParserTests extends HavenaskTestCase {
    public void testParseValidFromInts() throws Exception {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\":" + precision + ", \"size\": 500, \"shard_size\": 550}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid"));
    }

    public void testParseValidFromStrings() throws Exception {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"my_loc\", \"precision\":\"" + precision + "\", \"size\": \"500\", \"shard_size\": \"550\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid"));
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":false}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException e = expectThrows(XContentParseException.class,
                () -> GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid"));
        assertThat(ExceptionsHelper.detailedMessage(e),
                containsString("[geotile_grid] precision doesn't support values of type: VALUE_BOOLEAN"));
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":\"30\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid");
            fail();
        } catch (XContentParseException ex) {
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Invalid geotile_grid precision of 30. Must be between 0 and 29.", ex.getCause().getMessage());
        }
    }

    public void testParseValidBounds() throws Exception {
        Rectangle bbox = GeometryTestUtils.randomRectangle();
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
            "{\"field\":\"my_loc\", \"precision\": 5, \"size\": 500, \"shard_size\": 550," + "\"bounds\": { "
                + "\"top\": " + bbox.getMaxY() + ","
                + "\"bottom\": " + bbox.getMinY() + ","
                + "\"left\": " + bbox.getMinX() + ","
                + "\"right\": " + bbox.getMaxX() + "}"
                + "}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(GeoTileGridAggregationBuilder.PARSER.parse(stParser, "geotile_grid"));
    }
}
