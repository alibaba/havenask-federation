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

package org.havenask.search.aggregations.support;

import org.havenask.script.AggregationScript;
import org.havenask.search.DocValueFormat;

import java.time.ZoneId;
import java.util.function.LongSupplier;

/**
=======
 * {@link ValuesSourceType} represents a collection of fields that share a common set of operations, for example all numeric fields.
 * Aggregations declare their support for a given ValuesSourceType (via {@link ValuesSourceRegistry.Builder#register}),
 * and should then not need to care about the fields which use that ValuesSourceType.
 *
 * ValuesSourceTypes provide a set of methods to instantiate concrete {@link ValuesSource} instances, based on the actual source of the
 * data for the aggregations.  In general, aggregations should not call these methods, but rather rely on {@link ValuesSourceConfig} to have
 * selected the correct implementation.
 *
 * ValuesSourceTypes should be stateless.  We recommend that plugins define an enum for their ValuesSourceTypes, even if the plugin only
 * intends to define one ValuesSourceType.  ValuesSourceTypes are not serialized as part of the aggregations framework.
 *
 * Prefer reusing an existing ValuesSourceType (ideally from {@link CoreValuesSourceType}) over creating a new type.  There are some cases
 * where creating a new type is necessary however.  In particular, consider a new ValuesSourceType if the field has custom encoding/decoding
 * requirements; if the field needs to expose additional information to the aggregation (e.g. {@link ValuesSource.Range#rangeType()}); or
 * if logically the type needs a more restricted use (e.g. even though dates are stored as numbers, it doesn't make sense to pass them to
 * a sum aggregation).  When adding a new ValuesSourceType, new aggregators should be added and registered at the same time, to add support
 * for the new type to existing aggregations, as appropriate.
 */
public interface ValuesSourceType {
    /**
     * Called when an aggregation is operating over a known empty set (usually because the field isn't specified), this method allows for
     * returning a no-op implementation.  All {@link ValuesSource}s should implement this method.
     * @return - Empty specialization of the base {@link ValuesSource}
     */
    ValuesSource getEmpty();

    /**
     * Returns the type-specific sub class for a script data source.  {@link ValuesSource}s that do not support scripts should throw
     * {@link org.havenask.search.aggregations.AggregationExecutionException}.  Note that this method is called when a script is
     * operating without an underlying field.  Scripts operating over fields are handled by the script argument to getField below.
     *
     * @param script - The script being wrapped
     * @param scriptValueType - The expected output type of the script
     * @return - Script specialization of the base {@link ValuesSource}
     */
    ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType);

    /**
     * Return a {@link ValuesSource} wrapping a field for the given type.  All {@link ValuesSource}s must implement this method.
     *
     * @param fieldContext - The field being wrapped
     * @param script - Optional script that might be applied over the field
     * @return - Field specialization of the base {@link ValuesSource}
     */
    ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script);

    /**
     * Apply the given missing value to an already-constructed {@link ValuesSource}.  Types which do not support missing values should throw
     * {@link org.havenask.search.aggregations.AggregationExecutionException}
     *
     * @param valuesSource - The original {@link ValuesSource}
     * @param rawMissing - The missing value we got from the parser, typically a string or number
     * @param docValueFormat - The format to use for further parsing the user supplied value, e.g. a date format
     * @param now - Used in conjunction with the formatter, should return the current time in milliseconds
     * @return - Wrapper over the provided {@link ValuesSource} to apply the given missing value
     */
    ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat,
                                LongSupplier now);

    /**
     * This method provides a hook for specifying a type-specific formatter.  When {@link ValuesSourceConfig} can resolve a
     * {@link org.havenask.index.mapper.MappedFieldType}, it prefers to get the formatter from there.  Only when a field can't be
     * resolved (which is to say script cases and unmapped field cases), it will fall back to calling this method on whatever
     * {@link ValuesSourceType} it was able to resolve to.
     *
     * @param format - User supplied format string (Optional)
     * @param tz - User supplied time zone (Optional)
     * @return - A formatter object, configured with the passed in settings if appropriate.
     */
    default DocValueFormat getFormatter(String format, ZoneId tz) {
        return DocValueFormat.RAW;
    }

    /**
     * Returns the name of the Values Source Type for stats purposes
     * @return the name of the Values Source Type
     */
    String typeName();
}
