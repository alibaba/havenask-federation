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

package org.havenask.gradle;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encapsulates comparison and printing logic for an x.y.z version.
 */
public final class Version implements Comparable<Version> {
    private final int major;
    private final int minor;
    private final int revision;
    private final int id;
    // used to identify rebase to Havenask 1.0.0
    public static final int MASK = 0x08000000;

    /**
     * Specifies how a version string should be parsed.
     */
    public enum Mode {
        /**
         * Strict parsing only allows known suffixes after the patch number: "-alpha", "-beta" or "-rc". The
         * suffix "-SNAPSHOT" is also allowed, either after the patch number, or after the other suffices.
         */
        STRICT,

        /**
         * Relaxed parsing allows any alphanumeric suffix after the patch number.
         */
        RELAXED
    }

    private static final Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(-alpha\\d+|-beta\\d+|-rc\\d+)?(-SNAPSHOT)?");

    private static final Pattern relaxedPattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(-[a-zA-Z0-9_]+|\\+[a-zA-Z0-9_]+)*?");

    public Version(int major, int minor, int revision) {
        Objects.requireNonNull(major, "major version can't be null");
        Objects.requireNonNull(minor, "minor version can't be null");
        Objects.requireNonNull(revision, "revision version can't be null");
        this.major = major;
        this.minor = minor;
        this.revision = revision;

        // currently snapshot is not taken into account
        int id = major * 10000000 + minor * 100000 + revision * 1000;
        // identify if new Havenask version 1
        this.id = major == 1 ? id ^ MASK : id;
    }

    private static int parseSuffixNumber(String substring) {
        if (substring.isEmpty()) {
            throw new IllegalArgumentException("Invalid suffix, must contain a number e.x. alpha2");
        }
        return Integer.parseInt(substring);
    }

    public static Version fromString(final String s) {
        return fromString(s, Mode.STRICT);
    }

    public static Version fromString(final String s, final Mode mode) {
        Objects.requireNonNull(s);
        Matcher matcher = mode == Mode.STRICT ? pattern.matcher(s) : relaxedPattern.matcher(s);
        if (matcher.matches() == false) {
            String expected = mode == Mode.STRICT
                ? "major.minor.revision[-(alpha|beta|rc)Number][-SNAPSHOT]"
                : "major.minor.revision[-extra]";
            throw new IllegalArgumentException("Invalid version format: '" + s + "'. Should be " + expected);
        }

        return new Version(Integer.parseInt(matcher.group(1)), parseSuffixNumber(matcher.group(2)), parseSuffixNumber(matcher.group(3)));
    }

    @Override
    public String toString() {
        return String.valueOf(getMajor()) + "." + String.valueOf(getMinor()) + "." + String.valueOf(getRevision());
    }

    public boolean before(Version compareTo) {
        return id < compareTo.getId();
    }

    public boolean before(String compareTo) {
        return before(fromString(compareTo));
    }

    public boolean onOrBefore(Version compareTo) {
        return id <= compareTo.getId();
    }

    public boolean onOrBefore(String compareTo) {
        return onOrBefore(fromString(compareTo));
    }

    public boolean onOrAfter(Version compareTo) {
        return id >= compareTo.getId();
    }

    public boolean onOrAfter(String compareTo) {
        return onOrAfter(fromString(compareTo));
    }

    public boolean after(Version compareTo) {
        return id > compareTo.getId();
    }

    public boolean after(String compareTo) {
        return after(fromString(compareTo));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Version version = (Version) o;
        return major == version.major && minor == version.minor && revision == version.revision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, revision, id);
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getRevision() {
        return revision;
    }

    protected int getId() {
        return id;
    }

    @Override
    public int compareTo(Version other) {
        return Integer.compare(getId(), other.getId());
    }

}
