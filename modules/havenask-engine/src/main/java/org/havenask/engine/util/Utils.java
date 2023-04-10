/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.havenask.SpecialPermission;

public class Utils {
    public static <T> T doPrivileged(PrivilegedExceptionAction<T> operation) throws Exception {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) operation::run);
        } catch (PrivilegedActionException e) {
            throw e.getException();
        }
    }

    public static <T> T doPrivilegedIgnore(PrivilegedExceptionAction<T> operation) {
        try {
            return doPrivileged(operation);
        } catch (Exception ignore) {
            return null;
        }
    }

    private static String JarDir = null;

    /**
     * get thr path that contain current jar file.
     */
    public static String getJarDir() {
        if (JarDir != null) {
            return JarDir;
        }
        Path file = getFile();
        if (file == null) {
            throw new RuntimeException("jar file dir get failed!");
        }
        if (Files.isDirectory(file)) {
            return file.toAbsolutePath().toString();
        }
        return JarDir = file.getParent().toAbsolutePath().toString();
    }

    private static Path getFile() {
        String path = Utils.class.getProtectionDomain().getCodeSource().getLocation().getFile();
        try {
            path = java.net.URLDecoder.decode(path, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            return null;
        }
        return Path.of(path);
    }
}
