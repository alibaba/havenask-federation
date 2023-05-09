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

package org.havenask.packaging.util;

import java.nio.file.Path;
import java.util.Locale;

public class Distribution {

    public final Path path;
    public final Packaging packaging;
    public final Platform platform;
    public final boolean hasJdk;
    public final String version;

    public Distribution(Path path) {
        this.path = path;
        String filename = path.getFileName().toString();

        if (filename.endsWith(".gz")) {
            this.packaging = Packaging.TAR;
        } else if (filename.endsWith(".docker.tar")) {
            this.packaging = Packaging.DOCKER;
        } else {
            int lastDot = filename.lastIndexOf('.');
            this.packaging = Packaging.valueOf(filename.substring(lastDot + 1).toUpperCase(Locale.ROOT));
        }

        this.platform = filename.contains("windows") ? Platform.WINDOWS : Platform.LINUX;
        this.hasJdk = filename.contains("no-jdk") == false;
        String version = filename.split("-", 3)[1];
        if (filename.contains("-SNAPSHOT")) {
            version += "-SNAPSHOT";
        }
        this.version = version;
    }

    public boolean isArchive() {
        return packaging == Packaging.TAR || packaging == Packaging.ZIP;
    }

    public boolean isPackage() {
        return packaging == Packaging.RPM || packaging == Packaging.DEB;
    }

    public boolean isDocker() {
        return packaging == Packaging.DOCKER;
    }

    public enum Packaging {

        TAR(".tar.gz", Platforms.LINUX || Platforms.DARWIN),
        ZIP(".zip", Platforms.WINDOWS),
        DEB(".deb", Platforms.isDPKG()),
        RPM(".rpm", Platforms.isRPM()),
        DOCKER(".docker.tar", Platforms.isDocker());

        /** The extension of this distribution's file */
        public final String extension;

        /** Whether the distribution is intended for use on the platform the current JVM is running on */
        public final boolean compatible;

        Packaging(String extension, boolean compatible) {
            this.extension = extension;
            this.compatible = compatible;
        }
    }

    public enum Platform {
        LINUX,
        WINDOWS,
        DARWIN;

        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
