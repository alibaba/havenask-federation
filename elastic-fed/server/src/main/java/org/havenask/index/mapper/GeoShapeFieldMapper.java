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

package org.havenask.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.havenask.common.Explicit;
import org.havenask.common.geo.GeometryParser;
import org.havenask.common.geo.ShapeRelation;
import org.havenask.common.geo.builders.ShapeBuilder;
import org.havenask.geometry.Geometry;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.query.VectorGeoShapeQueryProcessor;

import java.util.List;
import java.util.Map;

/**
 * FieldMapper for indexing {@link LatLonShape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.havenask.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 */
public class GeoShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry, Geometry> {
    public static final String CONTENT_TYPE = "geo_shape";
    public static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setDimensions(7, 4, Integer.BYTES);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder<Builder,GeoShapeFieldType> {

        public Builder(String name) {
            super (name, FIELD_TYPE);
            this.hasDocValues = false;
        }

        private GeoShapeFieldType buildFieldType(BuilderContext context) {
            GeoShapeFieldType ft = new GeoShapeFieldType(buildFullName(context), indexed, fieldType.stored(), hasDocValues, meta);
            GeometryParser geometryParser = new GeometryParser(ft.orientation.getAsBoolean(), coerce().value(),
                ignoreZValue().value());
            ft.setGeometryParser(new GeoShapeParser(geometryParser));
            ft.setGeometryIndexer(new GeoShapeIndexer(orientation().value().getAsBoolean(), buildFullName(context)));
            ft.setOrientation(orientation == null ? Defaults.ORIENTATION.value() : orientation);
            return ft;
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            return new GeoShapeFieldMapper(name, fieldType, buildFieldType(context), ignoreMalformed(context), coerce(context),
                ignoreZValue(), orientation(),
                multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class GeoShapeFieldType extends AbstractShapeGeometryFieldType<Geometry, Geometry> implements GeoShapeQueryable {

        private final VectorGeoShapeQueryProcessor queryProcessor;

        public GeoShapeFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, false, meta);
            this.queryProcessor = new VectorGeoShapeQueryProcessor();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            return queryProcessor.geoShapeQuery(shape, fieldName, relation, context);
        }
    }

    public static final class TypeParser extends AbstractShapeGeometryFieldMapper.TypeParser {

        @Override
        protected AbstractShapeGeometryFieldMapper.Builder newBuilder(String name, Map<String, Object> params) {
            if (params.containsKey(DEPRECATED_PARAMETERS_KEY)) {
                return new LegacyGeoShapeFieldMapper.Builder(name,
                    (LegacyGeoShapeFieldMapper.DeprecatedParameters)params.get(DEPRECATED_PARAMETERS_KEY));
            }
            return new GeoShapeFieldMapper.Builder(name);
        }
    }

    public GeoShapeFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               Explicit<Boolean> ignoreZValue, Explicit<ShapeBuilder.Orientation> orientation,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation,
            multiFields, copyTo);
    }

    @Override
    protected void addStoredFields(ParseContext context, Geometry geometry) {
        // noop: we currently do not store geo_shapes
        // @todo store as geojson string?
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void addDocValuesFields(String name, Geometry geometry, List fields, ParseContext context) {
        // we will throw a mapping exception before we get here
    }

    @Override
    protected void addMultiFields(ParseContext context, Geometry geometry) {
        // noop (completion suggester currently not compatible with geo_shape)
    }

    @Override
    protected void mergeGeoOptions(AbstractShapeGeometryFieldMapper<?,?> mergeWith, List<String> conflicts) {
        if (mergeWith instanceof LegacyGeoShapeFieldMapper) {
            LegacyGeoShapeFieldMapper legacy = (LegacyGeoShapeFieldMapper) mergeWith;
            throw new IllegalArgumentException("[" + fieldType().name() + "] with field mapper [" + fieldType().typeName() + "] " +
                "using [BKD] strategy cannot be merged with " + "[" + legacy.fieldType().typeName() + "] with [" +
                legacy.fieldType().strategy() + "] strategy");
        }
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }
}
