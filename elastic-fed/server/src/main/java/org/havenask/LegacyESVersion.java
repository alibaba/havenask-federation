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

package org.havenask;

import org.havenask.common.Strings;
import org.havenask.common.collect.ImmutableOpenIntMap;
import org.havenask.common.collect.ImmutableOpenMap;

import java.lang.reflect.Field;

/**
 * The Contents of this file were originally moved from {@link Version}.
 *
 * This class keeps all the supported Havenask predecessor versions for
 * backward compatibility purpose.
 */
public class LegacyESVersion extends Version {

    public static final LegacyESVersion V_6_0_0_alpha1 =
        new LegacyESVersion(6000001, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_alpha2 =
        new LegacyESVersion(6000002, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_beta1 =
        new LegacyESVersion(6000026, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_beta2 =
        new LegacyESVersion(6000027, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_rc1 =
        new LegacyESVersion(6000051, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_rc2 =
        new LegacyESVersion(6000052, org.apache.lucene.util.Version.LUCENE_7_0_1);
    public static final LegacyESVersion V_6_0_0 =
        new LegacyESVersion(6000099, org.apache.lucene.util.Version.LUCENE_7_0_1);
    public static final LegacyESVersion V_6_0_1 =
        new LegacyESVersion(6000199, org.apache.lucene.util.Version.LUCENE_7_0_1);
    public static final LegacyESVersion V_6_1_0 = new LegacyESVersion(6010099, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_1 = new LegacyESVersion(6010199, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_2 = new LegacyESVersion(6010299, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_3 = new LegacyESVersion(6010399, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_4 = new LegacyESVersion(6010499, org.apache.lucene.util.Version.LUCENE_7_1_0);
    // The below version is missing from the 7.3 JAR
    private static final org.apache.lucene.util.Version LUCENE_7_2_1 = org.apache.lucene.util.Version.fromBits(7, 2, 1);
    public static final LegacyESVersion V_6_2_0 = new LegacyESVersion(6020099, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_1 = new LegacyESVersion(6020199, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_2 = new LegacyESVersion(6020299, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_3 = new LegacyESVersion(6020399, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_4 = new LegacyESVersion(6020499, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_3_0 = new LegacyESVersion(6030099, org.apache.lucene.util.Version.LUCENE_7_3_1);
    public static final LegacyESVersion V_6_3_1 = new LegacyESVersion(6030199, org.apache.lucene.util.Version.LUCENE_7_3_1);
    public static final LegacyESVersion V_6_3_2 = new LegacyESVersion(6030299, org.apache.lucene.util.Version.LUCENE_7_3_1);
    public static final LegacyESVersion V_6_4_0 = new LegacyESVersion(6040099, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_4_1 = new LegacyESVersion(6040199, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_4_2 = new LegacyESVersion(6040299, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_4_3 = new LegacyESVersion(6040399, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_5_0 = new LegacyESVersion(6050099, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_1 = new LegacyESVersion(6050199, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_2 = new LegacyESVersion(6050299, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_3 = new LegacyESVersion(6050399, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_4 = new LegacyESVersion(6050499, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_6_0 = new LegacyESVersion(6060099, org.apache.lucene.util.Version.LUCENE_7_6_0);
    public static final LegacyESVersion V_6_6_1 = new LegacyESVersion(6060199, org.apache.lucene.util.Version.LUCENE_7_6_0);
    public static final LegacyESVersion V_6_6_2 = new LegacyESVersion(6060299, org.apache.lucene.util.Version.LUCENE_7_6_0);
    public static final LegacyESVersion V_6_7_0 = new LegacyESVersion(6070099, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_7_1 = new LegacyESVersion(6070199, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_7_2 = new LegacyESVersion(6070299, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_0 = new LegacyESVersion(6080099, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_1 = new LegacyESVersion(6080199, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_2 = new LegacyESVersion(6080299, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_3 = new LegacyESVersion(6080399, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_4 = new LegacyESVersion(6080499, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_5 = new LegacyESVersion(6080599, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_6 = new LegacyESVersion(6080699, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_7 = new LegacyESVersion(6080799, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_8 = new LegacyESVersion(6080899, org.apache.lucene.util.Version.LUCENE_7_7_2);
    // Version constant for Lucene 7.7.3 release with index corruption bug fix
    private static final org.apache.lucene.util.Version LUCENE_7_7_3 = org.apache.lucene.util.Version.fromBits(7, 7, 3);
    public static final LegacyESVersion V_6_8_9 = new LegacyESVersion(6080999, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_10 = new LegacyESVersion(6081099, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_11 = new LegacyESVersion(6081199, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_12 = new LegacyESVersion(6081299, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_13 = new LegacyESVersion(6081399, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_14 = new LegacyESVersion(6081499, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_15 = new LegacyESVersion(6081599, org.apache.lucene.util.Version.LUCENE_7_7_3);
    public static final LegacyESVersion V_7_0_0 = new LegacyESVersion(7000099, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_0_1 = new LegacyESVersion(7000199, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_1_0 = new LegacyESVersion(7010099, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_1_1 = new LegacyESVersion(7010199, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_2_0 = new LegacyESVersion(7020099, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_2_1 = new LegacyESVersion(7020199, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_3_0 = new LegacyESVersion(7030099, org.apache.lucene.util.Version.LUCENE_8_1_0);
    public static final LegacyESVersion V_7_3_1 = new LegacyESVersion(7030199, org.apache.lucene.util.Version.LUCENE_8_1_0);
    public static final LegacyESVersion V_7_3_2 = new LegacyESVersion(7030299, org.apache.lucene.util.Version.LUCENE_8_1_0);
    public static final LegacyESVersion V_7_4_0 = new LegacyESVersion(7040099, org.apache.lucene.util.Version.LUCENE_8_2_0);
    public static final LegacyESVersion V_7_4_1 = new LegacyESVersion(7040199, org.apache.lucene.util.Version.LUCENE_8_2_0);
    public static final LegacyESVersion V_7_4_2 = new LegacyESVersion(7040299, org.apache.lucene.util.Version.LUCENE_8_2_0);
    public static final LegacyESVersion V_7_5_0 = new LegacyESVersion(7050099, org.apache.lucene.util.Version.LUCENE_8_3_0);
    public static final LegacyESVersion V_7_5_1 = new LegacyESVersion(7050199, org.apache.lucene.util.Version.LUCENE_8_3_0);
    public static final LegacyESVersion V_7_5_2 = new LegacyESVersion(7050299, org.apache.lucene.util.Version.LUCENE_8_3_0);
    public static final LegacyESVersion V_7_6_0 = new LegacyESVersion(7060099, org.apache.lucene.util.Version.LUCENE_8_4_0);
    public static final LegacyESVersion V_7_6_1 = new LegacyESVersion(7060199, org.apache.lucene.util.Version.LUCENE_8_4_0);
    public static final LegacyESVersion V_7_6_2 = new LegacyESVersion(7060299, org.apache.lucene.util.Version.LUCENE_8_4_0);
    public static final LegacyESVersion V_7_7_0 = new LegacyESVersion(7070099, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_7_1 = new LegacyESVersion(7070199, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_8_0 = new LegacyESVersion(7080099, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_8_1 = new LegacyESVersion(7080199, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_9_0 = new LegacyESVersion(7090099, org.apache.lucene.util.Version.LUCENE_8_6_0);
    public static final LegacyESVersion V_7_9_1 = new LegacyESVersion(7090199, org.apache.lucene.util.Version.LUCENE_8_6_2);
    public static final LegacyESVersion V_7_9_2 = new LegacyESVersion(7090299, org.apache.lucene.util.Version.LUCENE_8_6_2);
    public static final LegacyESVersion V_7_9_3 = new LegacyESVersion(7090399, org.apache.lucene.util.Version.LUCENE_8_6_2);
    public static final LegacyESVersion V_7_10_0 = new LegacyESVersion(7100099, org.apache.lucene.util.Version.LUCENE_8_7_0);
    public static final LegacyESVersion V_7_10_1 = new LegacyESVersion(7100199, org.apache.lucene.util.Version.LUCENE_8_7_0);
    public static final LegacyESVersion V_7_10_2 = new LegacyESVersion(7100299, org.apache.lucene.util.Version.LUCENE_8_7_0);

    // todo move back to Version.java if retiring legacy version support
    protected static final ImmutableOpenIntMap<Version> idToVersion;
    protected static final ImmutableOpenMap<String, Version> stringToVersion;
    static {
        final ImmutableOpenIntMap.Builder<Version> builder = ImmutableOpenIntMap.builder();
        final ImmutableOpenMap.Builder<String, Version> builderByString = ImmutableOpenMap.builder();

        for (final Field declaredField : LegacyESVersion.class.getFields()) {
            if (declaredField.getType().equals(Version.class) || declaredField.getType().equals(LegacyESVersion.class)) {
                final String fieldName = declaredField.getName();
                if (fieldName.equals("CURRENT") || fieldName.equals("V_EMPTY")) {
                    continue;
                }
                assert fieldName.matches("V_\\d+_\\d+_\\d+(_alpha[1,2]|_beta[1,2]|_rc[1,2])?")
                    : "expected Version field [" + fieldName + "] to match V_\\d+_\\d+_\\d+";
                try {
                    final Version version = (Version) declaredField.get(null);
                    if (Assertions.ENABLED) {
                        final String[] fields = fieldName.split("_");
                        if (fields.length == 5) {
                            assert (fields[1].equals("1") || fields[1].equals("6")) && fields[2].equals("0") :
                                "field " + fieldName + " should not have a build qualifier";
                        } else {
                            final int major = Integer.valueOf(fields[1]) * 1000000;
                            final int minor = Integer.valueOf(fields[2]) * 10000;
                            final int revision = Integer.valueOf(fields[3]) * 100;
                            final int expectedId;
                            if (fields[1].equals("1")) {
                                expectedId = 0x08000000 ^ (major + minor + revision + 99);
                            } else {
                                expectedId = (major + minor + revision + 99);
                            }
                            assert version.id == expectedId :
                                "expected version [" + fieldName + "] to have id [" + expectedId + "] but was [" + version.id + "]";
                        }
                    }
                    final Version maybePrevious = builder.put(version.id, version);
                    builderByString.put(version.toString(), version);
                    assert maybePrevious == null :
                        "expected [" + version.id + "] to be uniquely mapped but saw [" + maybePrevious + "] and [" + version + "]";
                } catch (final IllegalAccessException e) {
                    assert false : "Version field [" + fieldName + "] should be public";
                } catch (final RuntimeException e) {
                    assert false : "Version field [" + fieldName + "] threw [" + e + "] during initialization";
                }
            }
        }
        assert CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) : "Version must be upgraded to ["
            + org.apache.lucene.util.Version.LATEST + "] is still set to [" + CURRENT.luceneVersion + "]";

        builder.put(V_EMPTY_ID, V_EMPTY);
        builderByString.put(V_EMPTY.toString(), V_EMPTY);
        idToVersion = builder.build();
        stringToVersion = builderByString.build();
    }

    protected LegacyESVersion(int id, org.apache.lucene.util.Version luceneVersion) {
        // flip the 28th bit of the version id
        // this will be flipped back in the parent class to correctly identify as a legacy version;
        // giving backwards compatibility with legacy systems
        super(id ^ 0x08000000, luceneVersion);
    }

    @Override
    public boolean isBeta() {
        return major < 5 ? build < 50 : build >= 25 && build < 50;
    }

    /**
     * Returns true iff this version is an alpha version
     * Note: This has been introduced in version 5 of the Havenask predecessor. Previous versions will never
     * have an alpha version.
     */
    @Override
    public boolean isAlpha() {
        return major < 5 ? false :  build < 25;
    }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static Version fromString(String version) {
        if (!Strings.hasLength(version)) {
            return Version.CURRENT;
        }
        final Version cached = stringToVersion.get(version);
        if (cached != null) {
            return cached;
        }
        return fromStringSlow(version);
    }

    private static Version fromStringSlow(String version) {
        final boolean snapshot; // this is some BWC for 2.x and before indices
        if (snapshot = version.endsWith("-SNAPSHOT")) {
            version = version.substring(0, version.length() - 9);
        }
        String[] parts = version.split("[.-]");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException(
                "the version needs to contain major, minor, and revision, and optionally the build: " + version);
        }

        try {
            final int rawMajor = Integer.parseInt(parts[0]);
            if (rawMajor >= 5 && snapshot) { // we don't support snapshot as part of the version here anymore
                throw new IllegalArgumentException("illegal version format - snapshots are only supported until version 2.x");
            }
            if (rawMajor >=7 && parts.length == 4) { // we don't support qualifier as part of the version anymore
                throw new IllegalArgumentException("illegal version format - qualifiers are only supported until version 6.x");
            }
            final int betaOffset = rawMajor < 5 ? 0 : 25;
            //we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            final int major = rawMajor * 1000000;
            final int minor = Integer.parseInt(parts[1]) * 10000;
            final int revision = Integer.parseInt(parts[2]) * 100;


            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("alpha")) {
                    assert rawMajor >= 5 : "major must be >= 5 but was " + major;
                    build = Integer.parseInt(buildStr.substring(5));
                    assert build < 25 : "expected a alpha build but " + build + " >= 25";
                } else if (buildStr.startsWith("Beta") || buildStr.startsWith("beta")) {
                    build = betaOffset + Integer.parseInt(buildStr.substring(4));
                    assert build < 50 : "expected a beta build but " + build + " >= 50";
                } else if (buildStr.startsWith("RC") || buildStr.startsWith("rc")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                } else {
                    throw new IllegalArgumentException("unable to parse version " + version);
                }
            }

            return Version.fromId(major + minor + revision + build);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    /**
     * this is used to ensure the version id for new versions of Havenask are always less than the predecessor versions
     */
    protected int maskId(final int id) {
        return id;
    }
}
