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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.havenask.LegacyESVersion;
import org.havenask.HavenaskParseException;
import org.havenask.Version;
import org.havenask.common.Explicit;
import org.havenask.common.ParseField;
import org.havenask.common.geo.GeoUtils;
import org.havenask.common.geo.GeometryParser;
import org.havenask.common.geo.ShapeRelation;
import org.havenask.common.geo.ShapesAvailability;
import org.havenask.common.geo.SpatialStrategy;
import org.havenask.common.geo.builders.ShapeBuilder;
import org.havenask.common.geo.builders.ShapeBuilder.Orientation;
import org.havenask.common.geo.parsers.ShapeParser;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.DistanceUnit;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.support.XContentMapValues;
import org.havenask.geometry.Geometry;
import org.havenask.index.query.LegacyGeoShapeQueryProcessor;
import org.havenask.index.query.QueryShardContext;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * FieldMapper for indexing {@link org.locationtech.spatial4j.shape.Shape}s.
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
 *
 * @deprecated use {@link GeoShapeFieldMapper}
 */
@Deprecated
public class LegacyGeoShapeFieldMapper extends AbstractShapeGeometryFieldMapper<ShapeBuilder<?, ?, ?>, Shape> {

    public static final String CONTENT_TYPE = "geo_shape";

    @Deprecated
    public static class DeprecatedParameters {
        public static class Names {
            public static final ParseField STRATEGY = new ParseField("strategy");
            public static final ParseField TREE = new ParseField("tree");
            public static final ParseField TREE_LEVELS = new ParseField("tree_levels");
            public static final ParseField PRECISION = new ParseField("precision");
            public static final ParseField DISTANCE_ERROR_PCT = new ParseField("distance_error_pct");
            public static final ParseField POINTS_ONLY = new ParseField("points_only");
        }

        public static class PrefixTrees {
            public static final String LEGACY_QUADTREE = "legacyquadtree";
            public static final String QUADTREE = "quadtree";
            public static final String GEOHASH = "geohash";
        }

        public static class Defaults {
            public static final SpatialStrategy STRATEGY = SpatialStrategy.RECURSIVE;
            public static final String TREE = "quadtree";
            public static final String PRECISION = "50m";
            public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision(PRECISION);
            public static final int GEOHASH_TREE_LEVELS = GeoUtils.geoHashLevelsForPrecision(PRECISION);
            public static final boolean POINTS_ONLY = false;
            public static final double DISTANCE_ERROR_PCT = 0.025d;
        }

        public SpatialStrategy strategy = null;
        public String tree = null;
        public Integer treeLevels = null;
        public String precision = null;
        public Boolean pointsOnly = null;
        public Double distanceErrorPct = null;

        public void setSpatialStrategy(SpatialStrategy strategy) {
            this.strategy = strategy;
        }

        public void setTree(String prefixTree) {
            this.tree = prefixTree;
        }

        public void setTreeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
        }

        public void setPrecision(String precision) {
            this.precision = precision;
        }

        public void setPointsOnly(boolean pointsOnly) {
            if (this.strategy == SpatialStrategy.TERM && pointsOnly == false) {
                throw new HavenaskParseException("points_only cannot be set to false for term strategy");
            }
            this.pointsOnly = pointsOnly;
        }

        public void setDistanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
        }

        public static boolean parse(String name, String fieldName, Object fieldNode, DeprecatedParameters deprecatedParameters) {
            if (Names.STRATEGY.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setSpatialStrategy(SpatialStrategy.fromString(fieldNode.toString()));
            } else if (Names.TREE.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setTree(fieldNode.toString());
            } else if (Names.TREE_LEVELS.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setTreeLevels(Integer.parseInt(fieldNode.toString()));
            } else if (Names.PRECISION.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setPrecision(fieldNode.toString());
            } else if (Names.DISTANCE_ERROR_PCT.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setDistanceErrorPct(Double.parseDouble(fieldNode.toString()));
            } else if (Names.POINTS_ONLY.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                checkPrefixTreeSupport(fieldName);
                deprecatedParameters.setPointsOnly(
                    XContentMapValues.nodeBooleanValue(fieldNode, name + "." + DeprecatedParameters.Names.POINTS_ONLY));
            } else {
                return false;
            }
            return true;
        }

        private static void checkPrefixTreeSupport(String fieldName) {
            if (ShapesAvailability.JTS_AVAILABLE == false || ShapesAvailability.SPATIAL4J_AVAILABLE == false) {
                throw new HavenaskParseException("Field parameter [{}] is not supported for [{}] field type",
                    fieldName, CONTENT_TYPE);
            }
            DEPRECATION_LOGGER.deprecate("geo_mapper_field_parameter",
                "Field parameter [{}] is deprecated and will be removed in a future version.", fieldName);
        }
    }

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(LegacyGeoShapeFieldMapper.class);

    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder<Builder,
        LegacyGeoShapeFieldMapper.GeoShapeFieldType> {

        DeprecatedParameters deprecatedParameters;

        public Builder(String name) {
            this(name, new DeprecatedParameters());
        }

        public Builder(String name, DeprecatedParameters deprecatedParameters) {
            super(name, Defaults.FIELD_TYPE);
            this.deprecatedParameters = deprecatedParameters;
        }

        private void setupFieldTypeDeprecatedParameters(BuilderContext context, GeoShapeFieldType ft) {
            if (deprecatedParameters.strategy != null) {
                ft.setStrategy(deprecatedParameters.strategy);
            }
            if (deprecatedParameters.tree != null) {
                ft.setTree(deprecatedParameters.tree);
            } else if (context.indexCreatedVersion().before(LegacyESVersion.V_6_6_0)) {
                ft.setTree(DeprecatedParameters.PrefixTrees.GEOHASH);
            }
            if (deprecatedParameters.treeLevels != null) {
                ft.setTreeLevels(deprecatedParameters.treeLevels);
            }
            if (deprecatedParameters.precision != null) {
                // precision is only set iff: a. treeLevel is not explicitly set, b. its explicitly set
                ft.setPrecisionInMeters(DistanceUnit.parse(deprecatedParameters.precision,
                    DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
            }
            if (deprecatedParameters.distanceErrorPct != null) {
                ft.setDistanceErrorPct(deprecatedParameters.distanceErrorPct);
            }
            if (deprecatedParameters.pointsOnly != null) {
                ft.setPointsOnly(deprecatedParameters.pointsOnly);
            }

            if (ft.treeLevels() == 0 && ft.precisionInMeters() < 0) {
                ft.setDefaultDistanceErrorPct(DeprecatedParameters.Defaults.DISTANCE_ERROR_PCT);
            }
        }

        private void setupPrefixTrees(GeoShapeFieldType ft) {
            SpatialPrefixTree prefixTree;
            if (ft.tree().equals(DeprecatedParameters.PrefixTrees.GEOHASH)) {
                prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), DeprecatedParameters.Defaults.GEOHASH_TREE_LEVELS, true));
            } else if (ft.tree().equals(DeprecatedParameters.PrefixTrees.LEGACY_QUADTREE)) {
                prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), DeprecatedParameters.Defaults.QUADTREE_LEVELS, false));
            } else if (ft.tree().equals(DeprecatedParameters.PrefixTrees.QUADTREE)) {
                prefixTree = new PackedQuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT,
                    getLevels(ft.treeLevels(), ft.precisionInMeters(), DeprecatedParameters.Defaults.QUADTREE_LEVELS, false));
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + ft.tree() + "]");
            }

            // setup prefix trees regardless of strategy (this is used for the QueryBuilder)
            // recursive:
            RecursivePrefixTreeStrategy rpts = new RecursivePrefixTreeStrategy(prefixTree, ft.name());
            rpts.setDistErrPct(ft.distanceErrorPct());
            rpts.setPruneLeafyBranches(false);
            ft.recursiveStrategy = rpts;

            // term:
            TermQueryPrefixTreeStrategy termStrategy = new TermQueryPrefixTreeStrategy(prefixTree, ft.name());
            termStrategy.setDistErrPct(ft.distanceErrorPct());
            ft.termStrategy = termStrategy;

            // set default (based on strategy):
            ft.defaultPrefixTreeStrategy = ft.resolvePrefixTreeStrategy(ft.strategy());
            ft.defaultPrefixTreeStrategy.setPointsOnly(ft.pointsOnly());
        }

        private GeoShapeFieldType buildFieldType(BuilderContext context) {
            GeoShapeFieldType ft = new GeoShapeFieldType(buildFullName(context), indexed, fieldType.stored(), false, meta);
            setupFieldTypeDeprecatedParameters(context, ft);
            setupPrefixTrees(ft);
            ft.setGeometryIndexer(new LegacyGeoShapeIndexer(ft));
            ft.setGeometryParser(new LegacyGeoShapeParser());
            ft.setOrientation(orientation == null ? Defaults.ORIENTATION.value() : orientation);
            return ft;
        }

        private static int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
            if (treeLevels > 0 || precisionInMeters >= 0) {
                return Math.max(treeLevels, precisionInMeters >= 0 ? (geoHash ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                    : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters)) : 0);
            }
            return defaultLevels;
        }

        @Override
        public LegacyGeoShapeFieldMapper build(BuilderContext context) {
            if (name.isEmpty()) {
                // Check for an empty name early so we can throw a consistent error message
                throw new IllegalArgumentException("name cannot be empty string");
            }
            return new LegacyGeoShapeFieldMapper(name, fieldType, buildFieldType(context), ignoreMalformed(context),
                coerce(context), orientation(), ignoreZValue(), context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    private static class LegacyGeoShapeParser extends Parser<ShapeBuilder<?, ?, ?>> {
        /**
         * Note that this parser is only used for formatting values.
         */
        private final GeometryParser geometryParser;

        private LegacyGeoShapeParser() {
            this.geometryParser = new GeometryParser(true, true, true);
        }

        @Override
        public ShapeBuilder<?, ?, ?> parse(XContentParser parser) throws IOException, ParseException {
            return ShapeParser.parse(parser);
        }

        @Override
        public Object format(ShapeBuilder<?, ?, ?> value, String format) {
            Geometry geometry = value.buildGeometry();
            return geometryParser.geometryFormat(format).toXContentAsObject(geometry);
        }
    }

    public static final class GeoShapeFieldType extends AbstractShapeGeometryFieldType<ShapeBuilder<?, ?, ?>, Shape>
        implements GeoShapeQueryable {

        private String tree = DeprecatedParameters.Defaults.TREE;
        private SpatialStrategy strategy = DeprecatedParameters.Defaults.STRATEGY;
        private boolean pointsOnly = DeprecatedParameters.Defaults.POINTS_ONLY;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private Double distanceErrorPct;
        private double defaultDistanceErrorPct = 0.0;

        // these are built when the field type is frozen
        private PrefixTreeStrategy defaultPrefixTreeStrategy;
        private RecursivePrefixTreeStrategy recursiveStrategy;
        private TermQueryPrefixTreeStrategy termStrategy;

        private final LegacyGeoShapeQueryProcessor queryProcessor;

        private GeoShapeFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, false, meta);
            this.queryProcessor = new LegacyGeoShapeQueryProcessor(this);
        }

        public GeoShapeFieldType(String name) {
            this(name, true, false, true, Collections.emptyMap());
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            throw new UnsupportedOperationException("process method should not be called for PrefixTree based geo_shapes");
        }

        @Override
        public Query geoShapeQuery(Geometry shape, String fieldName, SpatialStrategy strategy, ShapeRelation relation,
                            QueryShardContext context) {
            return queryProcessor.geoShapeQuery(shape, fieldName, strategy, relation, context);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String tree() {
            return tree;
        }

        public void setTree(String tree) {
            this.tree = tree;
        }

        public SpatialStrategy strategy() {
            return strategy;
        }

        public void setStrategy(SpatialStrategy strategy) {
            this.strategy = strategy;
            if (this.strategy.equals(SpatialStrategy.TERM)) {
                this.pointsOnly = true;
            }
        }

        public boolean pointsOnly() {
            return pointsOnly;
        }

        public void setPointsOnly(boolean pointsOnly) {
            this.pointsOnly = pointsOnly;
        }
        public int treeLevels() {
            return treeLevels;
        }

        public void setTreeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
        }

        public double precisionInMeters() {
            return precisionInMeters;
        }

        public void setPrecisionInMeters(double precisionInMeters) {
            this.precisionInMeters = precisionInMeters;
        }

        public double distanceErrorPct() {
            return distanceErrorPct == null ? defaultDistanceErrorPct : distanceErrorPct;
        }

        public void setDistanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
        }

        public void setDefaultDistanceErrorPct(double defaultDistanceErrorPct) {
            this.defaultDistanceErrorPct = defaultDistanceErrorPct;
        }

        public PrefixTreeStrategy defaultPrefixTreeStrategy() {
            return this.defaultPrefixTreeStrategy;
        }

        public PrefixTreeStrategy resolvePrefixTreeStrategy(SpatialStrategy strategy) {
            return resolvePrefixTreeStrategy(strategy.getStrategyName());
        }

        public PrefixTreeStrategy resolvePrefixTreeStrategy(String strategyName) {
            if (SpatialStrategy.RECURSIVE.getStrategyName().equals(strategyName)) {
                return recursiveStrategy;
            }
            if (SpatialStrategy.TERM.getStrategyName().equals(strategyName)) {
                return termStrategy;
            }
            throw new IllegalArgumentException("Unknown prefix tree strategy [" + strategyName + "]");
        }
    }

    private final Version indexCreatedVersion;

    public LegacyGeoShapeFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                     Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, Explicit<Orientation> orientation,
                                     Explicit<Boolean> ignoreZValue, Settings indexSettings,
                                     MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation,
            multiFields, copyTo);
        this.indexCreatedVersion = Version.indexCreated(indexSettings);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    @Override
    protected void addStoredFields(ParseContext context, Shape geometry) {
        // noop: we do not store geo_shapes; and will not store legacy geo_shape types
    }

    @Override
    protected void addDocValuesFields(String name, Shape geometry, List<IndexableField> fields, ParseContext context) {
        // doc values are not supported
    }

    @Override
    protected void addMultiFields(ParseContext context, Shape geometry) {
        // noop (completion suggester currently not compatible with geo_shape)
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults
            || (fieldType().tree().equals(indexCreatedVersion.onOrAfter(LegacyESVersion.V_6_6_0) ?
                    DeprecatedParameters.Defaults.TREE : DeprecatedParameters.PrefixTrees.GEOHASH)) == false) {
            builder.field(DeprecatedParameters.Names.TREE.getPreferredName(), fieldType().tree());
        }

        if (fieldType().treeLevels() != 0) {
            builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(), fieldType().treeLevels());
        } else if(includeDefaults && fieldType().precisionInMeters() == -1) { // defaults only make sense if precision is not specified
            if (DeprecatedParameters.PrefixTrees.GEOHASH.equals(fieldType().tree())) {
                builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(),
                    DeprecatedParameters.Defaults.GEOHASH_TREE_LEVELS);
            } else if (DeprecatedParameters.PrefixTrees.LEGACY_QUADTREE.equals(fieldType().tree())) {
                builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(),
                    DeprecatedParameters.Defaults.QUADTREE_LEVELS);
            } else if (DeprecatedParameters.PrefixTrees.QUADTREE.equals(fieldType().tree())) {
                builder.field(DeprecatedParameters.Names.TREE_LEVELS.getPreferredName(),
                    DeprecatedParameters.Defaults.QUADTREE_LEVELS);
            } else {
                throw new IllegalArgumentException("Unknown prefix tree type [" + fieldType().tree() + "]");
            }
        }
        if (fieldType().precisionInMeters() != -1) {
            builder.field(DeprecatedParameters.Names.PRECISION.getPreferredName(),
                DistanceUnit.METERS.toString(fieldType().precisionInMeters()));
        } else if (includeDefaults && fieldType().treeLevels() == 0) { // defaults only make sense if tree levels are not specified
            builder.field(DeprecatedParameters.Names.PRECISION.getPreferredName(),
                DistanceUnit.METERS.toString(50));
        }

        if (indexCreatedVersion.onOrAfter(LegacyESVersion.V_7_0_0)) {
            builder.field(DeprecatedParameters.Names.STRATEGY.getPreferredName(), fieldType().strategy().getStrategyName());
        }

        if (includeDefaults || fieldType().distanceErrorPct() != fieldType().defaultDistanceErrorPct) {
            builder.field(DeprecatedParameters.Names.DISTANCE_ERROR_PCT.getPreferredName(), fieldType().distanceErrorPct());
        }
        if (fieldType().strategy() == SpatialStrategy.TERM) {
            // For TERMs strategy the defaults for points only change to true
            if (includeDefaults || fieldType().pointsOnly() != true) {
                builder.field(DeprecatedParameters.Names.POINTS_ONLY.getPreferredName(), fieldType().pointsOnly());
            }
        } else {
            if (includeDefaults || fieldType().pointsOnly() != DeprecatedParameters.Defaults.POINTS_ONLY) {
                builder.field(DeprecatedParameters.Names.POINTS_ONLY.getPreferredName(), fieldType().pointsOnly());
            }
        }
    }

    @Override
    protected void mergeGeoOptions(AbstractShapeGeometryFieldMapper<?,?> mergeWith, List<String> conflicts) {

        if (mergeWith instanceof GeoShapeFieldMapper) {
            GeoShapeFieldMapper fieldMapper = (GeoShapeFieldMapper) mergeWith;
            throw new IllegalArgumentException("[" + fieldType().name() + "] with field mapper [" + fieldType().typeName() + "] " +
                "using [" + fieldType().strategy() + "] strategy cannot be merged with " + "[" + fieldMapper.typeName() +
                "] with [BKD] strategy");
        }

        GeoShapeFieldType g = (GeoShapeFieldType)mergeWith.fieldType();
        // prevent user from changing strategies
        if (fieldType().strategy() != g.strategy()) {
            conflicts.add("mapper [" + name() + "] has different [strategy]");
        }

        // prevent user from changing trees (changes encoding)
        if (fieldType().tree().equals(g.tree()) == false) {
            conflicts.add("mapper [" + name() + "] has different [tree]");
        }

        if (fieldType().pointsOnly() != g.pointsOnly()) {
            conflicts.add("mapper [" + name() + "] has different points_only");
        }

        // TODO we should allow this, but at the moment levels is used to build bookkeeping variables
        // in lucene's SpatialPrefixTree implementations, need a patch to correct that first
        if (fieldType().treeLevels() != g.treeLevels()) {
            conflicts.add("mapper [" + name() + "] has different [tree_levels]");
        }
        if (fieldType().precisionInMeters() != g.precisionInMeters()) {
            conflicts.add("mapper [" + name() + "] has different [precision]");
        }

        this.orientation = mergeWith.orientation;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
