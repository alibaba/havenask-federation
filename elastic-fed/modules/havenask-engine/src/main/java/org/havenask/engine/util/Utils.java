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
import java.util.Objects;
import java.util.stream.Stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static Logger logger = LogManager.getLogger(Utils.class);
    public static final String DEFAULT_INDEX_UP_PATH = "/usr/share/havenask/data_havenask/runtimedata";
    public static final String DEFAULT_INDEX_SUB_PATH = "generation_0/partition_0_65535";

    /**
     * return the timestamp in the max version file under the certain index directory
     */
    public static String getIndexCheckpoint(String indexName, String upPath, String subPath) {
        Path versionFilePath = Path.of(upPath, indexName, subPath);
        String maxIndexVersionFile = getIndexMaxVersion(versionFilePath);
        // no version file or directory not exists
        if (Objects.equals(maxIndexVersionFile, null)) return null;
        if (Objects.equals(maxIndexVersionFile, "")) {
            logger.error("directory [{}] has no file ", versionFilePath);
            return null;
        }

        Path filePath = Path.of(upPath, indexName, subPath, maxIndexVersionFile);
        return getIndexTimestamp(filePath);
    }

    public static String getIndexCheckpoint(String indexName) {
        return getIndexCheckpoint(indexName, DEFAULT_INDEX_UP_PATH, DEFAULT_INDEX_SUB_PATH);
    }

    /**
     * return the max version file name under the certain index directory
     */
    private static String getIndexMaxVersion(Path versionFilePath) {
        try (Stream<Path> stream = Files.list(versionFilePath)) {
            String maxVersionFile =  stream.map(path1 -> path1.getFileName().toString())
                    .filter(s -> s.matches("version\\.\\d+"))
                    .map(s -> Long.parseLong(s.substring(s.indexOf('.') + 1)))
                    .max(Long::compare)
                    .map(max -> "version." + max)
                    .orElse("");
            return maxVersionFile;
        } catch (Exception e) {
            logger.error("directory [{}] does not exist or the version num is too big", versionFilePath, e);
            return null;
        }
    }

    /**
     * return the timestamp in the version file
     */
    private static String getIndexTimestamp(Path jsonPath) {
        try {
            String content = Files.readString(jsonPath);
            JSONObject jsonObject = JSON.parseObject(content);
            return jsonObject.getString("timestamp");
        } catch (Exception e) {
            logger.error("file [{}] get index timestamp failed ", jsonPath, e);
            return null;
        }
    }
}
