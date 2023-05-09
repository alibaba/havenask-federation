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

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.havenask.HavenaskParseException;
import org.havenask.common.geo.GeoUtils.EffectivePoint;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.geometry.Geometry;
import org.havenask.geometry.Point;
import org.havenask.geometry.Rectangle;
import org.havenask.geometry.ShapeType;
import org.havenask.geometry.utils.GeographyValidator;
import org.havenask.geometry.utils.Geohash;
import org.havenask.geometry.utils.WellKnownText;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import static org.havenask.index.mapper.AbstractPointGeometryFieldMapper.Names.IGNORE_Z_VALUE;

public class GeoPoint implements ToXContentFragment {

    protected double lat;
    protected double lon;

    public GeoPoint() {
    }

    /**
     * Create a new Geopoint from a string. This String must either be a geohash
     * or a lat-lon tuple.
     *
     * @param value String to create the point from
     */
    public GeoPoint(String value) {
        this.resetFromString(value);
    }

    public GeoPoint(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public GeoPoint(GeoPoint template) {
        this(template.getLat(), template.getLon());
    }

    public GeoPoint reset(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    public GeoPoint resetLat(double lat) {
        this.lat = lat;
        return this;
    }

    public GeoPoint resetLon(double lon) {
        this.lon = lon;
        return this;
    }

    public GeoPoint resetFromString(String value) {
        return resetFromString(value, false, EffectivePoint.BOTTOM_LEFT);
    }

    public GeoPoint resetFromString(String value, final boolean ignoreZValue, EffectivePoint effectivePoint) {
        if (value.toLowerCase(Locale.ROOT).contains("point")) {
            return resetFromWKT(value, ignoreZValue);
        } else if (value.contains(",")) {
            return resetFromCoordinates(value, ignoreZValue);
        }
        return parseGeoHash(value, effectivePoint);
    }


    public GeoPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        String[] vals = value.split(",");
        if (vals.length > 3) {
            throw new HavenaskParseException("failed to parse [{}], expected 2 or 3 coordinates "
                + "but found: [{}]", vals.length);
        }
        final double lat;
        final double lon;
        try {
            lat = Double.parseDouble(vals[0].trim());
         } catch (NumberFormatException ex) {
            throw new HavenaskParseException("latitude must be a number");
        }
        try {
            lon = Double.parseDouble(vals[1].trim());
        } catch (NumberFormatException ex) {
            throw new HavenaskParseException("longitude must be a number");
        }
        if (vals.length > 2) {
            GeoPoint.assertZValue(ignoreZValue, Double.parseDouble(vals[2].trim()));
        }
        return reset(lat, lon);
    }

    private GeoPoint resetFromWKT(String value, boolean ignoreZValue) {
        Geometry geometry;
        try {
            geometry = new WellKnownText(false, new GeographyValidator(ignoreZValue))
                .fromWKT(value);
        } catch (Exception e) {
            throw new HavenaskParseException("Invalid WKT format", e);
        }
        if (geometry.type() != ShapeType.POINT) {
            throw new HavenaskParseException("[geo_point] supports only POINT among WKT primitives, " +
                "but found " + geometry.type());
        }
        Point point = (Point) geometry;
        return reset(point.getY(), point.getX());
    }

    GeoPoint parseGeoHash(String geohash, EffectivePoint effectivePoint) {
        if (effectivePoint == EffectivePoint.BOTTOM_LEFT) {
            return resetFromGeoHash(geohash);
        } else {
            Rectangle rectangle = Geohash.toBoundingBox(geohash);
            switch (effectivePoint) {
                case TOP_LEFT:
                    return reset(rectangle.getMaxY(), rectangle.getMinX());
                case TOP_RIGHT:
                    return reset(rectangle.getMaxY(), rectangle.getMaxX());
                case BOTTOM_RIGHT:
                    return reset(rectangle.getMinY(), rectangle.getMaxX());
                default:
                    throw new IllegalArgumentException("Unsupported effective point " + effectivePoint);
            }
        }
    }

    public GeoPoint resetFromIndexHash(long hash) {
        lon = Geohash.decodeLongitude(hash);
        lat = Geohash.decodeLatitude(hash);
        return this;
    }

    // todo this is a crutch because LatLonPoint doesn't have a helper for returning .stringValue()
    // todo remove with next release of lucene
    public GeoPoint resetFromIndexableField(IndexableField field) {
        if (field instanceof LatLonPoint) {
            BytesRef br = field.binaryValue();
            byte[] bytes = Arrays.copyOfRange(br.bytes, br.offset, br.length);
            return this.reset(
                GeoEncodingUtils.decodeLatitude(bytes, 0),
                GeoEncodingUtils.decodeLongitude(bytes, Integer.BYTES));
        } else if (field instanceof LatLonDocValuesField) {
            long encoded = (long)(field.numericValue());
            return this.reset(
                GeoEncodingUtils.decodeLatitude((int)(encoded >>> 32)),
                GeoEncodingUtils.decodeLongitude((int)encoded));
        }
        return resetFromIndexHash(Long.parseLong(field.stringValue()));
    }

    public GeoPoint resetFromGeoHash(String geohash) {
        final long hash;
        try {
            hash = Geohash.mortonEncode(geohash);
        } catch (IllegalArgumentException ex) {
            throw new HavenaskParseException(ex.getMessage(), ex);
        }
        return this.reset(Geohash.decodeLatitude(hash), Geohash.decodeLongitude(hash));
    }

    public GeoPoint resetFromGeoHash(long geohashLong) {
        final int level = (int)(12 - (geohashLong&15));
        return this.resetFromIndexHash(BitUtil.flipFlop((geohashLong >>> 4) << ((level * 5) + 2)));
    }

    public double lat() {
        return this.lat;
    }

    public double getLat() {
        return this.lat;
    }

    public double lon() {
        return this.lon;
    }

    public double getLon() {
        return this.lon;
    }

    public String geohash() {
        return Geohash.stringEncode(lon, lat);
    }

    public String getGeohash() {
        return Geohash.stringEncode(lon, lat);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoPoint geoPoint = (GeoPoint) o;

        if (Double.compare(geoPoint.lat, lat) != 0) return false;
        if (Double.compare(geoPoint.lon, lon) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = lat != +0.0d ? Double.doubleToLongBits(lat) : 0L;
        result = Long.hashCode(temp);
        temp = lon != +0.0d ? Double.doubleToLongBits(lon) : 0L;
        result = 31 * result + Long.hashCode(temp);
        return result;
    }

    @Override
    public String toString() {
        return lat + ", " + lon;
    }

    public static GeoPoint fromGeohash(String geohash) {
        return new GeoPoint().resetFromGeoHash(geohash);
    }

    public static GeoPoint fromGeohash(long geohashLong) {
        return new GeoPoint().resetFromGeoHash(geohashLong);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.latlon(lat, lon);
    }

    public static double assertZValue(final boolean ignoreZValue, double zValue) {
        if (ignoreZValue == false) {
            throw new HavenaskParseException("Exception parsing coordinates: found Z value [{}] but [{}] "
                + "parameter is [{}]", zValue, IGNORE_Z_VALUE, ignoreZValue);
        }
        return zValue;
    }
}
