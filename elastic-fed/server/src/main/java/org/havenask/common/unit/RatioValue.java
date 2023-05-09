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

package org.havenask.common.unit;

import org.havenask.HavenaskParseException;

/**
 * Utility class to represent ratio and percentage values between 0 and 100
 */
public class RatioValue {
    private final double percent;

    public RatioValue(double percent) {
        this.percent = percent;
    }

    public double getAsRatio() {
        return this.percent / 100.0;
    }

    public double getAsPercent() {
        return this.percent;
    }

    @Override
    public String toString() {
        return this.percent + "%";
    }

    /**
     * Parses the provided string as a {@link RatioValue}, the string can
     * either be in percentage format (eg. 73.5%), or a floating-point ratio
     * format (eg. 0.735)
     */
    public static RatioValue parseRatioValue(String sValue) {
        if (sValue.endsWith("%")) {
            final String percentAsString = sValue.substring(0, sValue.length() - 1);
            try {
                final double percent = Double.parseDouble(percentAsString);
                if (percent < 0 || percent > 100) {
                    throw new HavenaskParseException("Percentage should be in [0-100], got [{}]", percentAsString);
                }
                return new RatioValue(Math.abs(percent));
            } catch (NumberFormatException e) {
                throw new HavenaskParseException("Failed to parse [{}] as a double", e, percentAsString);
            }
        } else {
            try {
                double ratio = Double.parseDouble(sValue);
                if (ratio < 0 || ratio > 1.0) {
                    throw new HavenaskParseException("Ratio should be in [0-1.0], got [{}]", ratio);
                }
                return new RatioValue(100.0 * Math.abs(ratio));
            } catch (NumberFormatException e) {
                throw new HavenaskParseException("Invalid ratio or percentage [{}]", sValue);
            }

        }
    }
}
