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

import org.havenask.common.geo.GeometryFormat;
import org.havenask.common.geo.GeometryParser;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.support.MapXContentParser;
import org.havenask.geometry.Geometry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.Collections;

public class GeoShapeParser extends AbstractGeometryFieldMapper.Parser<Geometry> {
    private final GeometryParser geometryParser;

    public GeoShapeParser(GeometryParser geometryParser) {
        this.geometryParser = geometryParser;
    }

    @Override
    public Geometry parse(XContentParser parser) throws IOException, ParseException {
        return geometryParser.parse(parser);
    }

    @Override
    public Object format(Geometry value, String format) {
        return geometryParser.geometryFormat(format).toXContentAsObject(value);
    }

    @Override
    public Object parseAndFormatObject(Object value, String format) {
        try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            Collections.singletonMap("dummy_field", value), XContentType.JSON)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value

            GeometryFormat<Geometry> geometryFormat = geometryParser.geometryFormat(parser);
            if (geometryFormat.name().equals(format)) {
                return value;
            }

            Geometry geometry = geometryFormat.fromXContent(parser);
            return format(geometry, format);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
