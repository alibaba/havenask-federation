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

package org.havenask.index.mapper;

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.index.mapper.FieldTypeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class GeoShapeFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new GeoShapeFieldMapper.Builder("field").build(context).fieldType();

        Map<String, Object> jsonLineString = org.havenask.common.collect.Map.of(
            "type", "LineString",
            "coordinates", Arrays.asList(Arrays.asList(42.0, 27.1), Arrays.asList(30.0, 50.0)));
        Map<String, Object> jsonPoint = org.havenask.common.collect.Map.of(
            "type", "Point",
            "coordinates", Arrays.asList(14.0, 15.0));
        String wktLineString = "LINESTRING (42.0 27.1, 30.0 50.0)";
        String wktPoint = "POINT (14.0 15.0)";

        // Test a single shape in geojson format.
        Object sourceValue = jsonLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in geojson format.
        sourceValue = Arrays.asList(jsonLineString, jsonPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a single shape in wkt format.
        sourceValue = wktLineString;
        assertEquals(Collections.singletonList(jsonLineString), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Collections.singletonList(wktLineString), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of shapes in wkt format.
        sourceValue = Arrays.asList(wktLineString, wktPoint);
        assertEquals(Arrays.asList(jsonLineString, jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(Arrays.asList(wktLineString, wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }
}
