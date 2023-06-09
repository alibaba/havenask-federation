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

import java.nio.ByteBuffer;
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

    private static final Logger logger = LogManager.getLogger(Utils.class);
    public static final String INDEX_SUB_PATH = "generation_0/partition_0_65535";

    /**
     * return the locator timestamp in the max version file under the certain index directory
     */
    public static Long getIndexCheckpoint(Path indexPath) {
        Path versionFilePath = indexPath.resolve(INDEX_SUB_PATH);
        String maxIndexVersionFile = getIndexMaxVersion(versionFilePath);
        // no version file or directory not exists
        if (Objects.equals(maxIndexVersionFile, null)) return null;
        if (Objects.equals(maxIndexVersionFile, "")) {
            logger.error("directory [{}] has no version file ", versionFilePath);
            return null;
        }

        Path filePath = indexPath.resolve(INDEX_SUB_PATH).resolve(maxIndexVersionFile);
        String locator = getIndexLocator(filePath);

        return getLocatorCheckpoint(locator);
    }

    /**
     * return the max version file name under the certain index directory
     */
    private static String getIndexMaxVersion(Path versionFilePath) {
        try (Stream<Path> stream = Files.list(versionFilePath)) {
            String maxVersionFile = stream.map(path1 -> path1.getFileName().toString())
                .filter(s -> s.matches("version\\.\\d+"))
                .map(s -> Long.parseLong(s.substring(s.indexOf('.') + 1)))
                .max(Long::compare)
                .map(max -> "version." + max)
                .orElse("");
            return maxVersionFile;
        } catch (Exception e) {
            logger.error("directory [{}] does not exist or the version num is too big", versionFilePath);
            return null;
        }
    }

    /**
     * return the loactor in the version file
     */
    private static String getIndexLocator(Path jsonPath) {
        try {
            String content = Files.readString(jsonPath);
            JSONObject jsonObject = JSON.parseObject(content);
            return jsonObject.getString("locator");
        } catch (Exception e) {
            logger.error("get index locator failed in file [{}]", jsonPath);
            return null;
        }
    }

    /**
     * return the checkpoint in the locator string
     */
    public static Long getLocatorCheckpoint(String locator) {
        if (Objects.equals(locator, null)) return null;

        if (locator.length() < 80) {
            logger.debug("locator has no timestamp");
            return null;
        }

        // if (locator.length() > 80) {
        // logger.warn("locator in file [{}] has more than 2 progress, but we only return the timestamp in the first progress", filePath);
        // }

        long progressNum = 0;
        try {
            progressNum = getLongLittleEndian(locator, 32, 48);
        } catch (Exception e) {
            logger.error("illegal form locator [{}]", locator);
            return null;
        }

        if (16 + 16 + 16 + progressNum * 32 != locator.length()) {
            logger.error("illegal form locator with progressNum: [{}] and locator length: [{}]", progressNum, locator.length());
            return null;
        }

        try {
            return getLongLittleEndian(locator, 48, 64);
        } catch (Exception e) {
            logger.error("illegal form locator [{}]", locator);
            return null;
        }
    }

    /**
     * return the long value for the little endian string
     */
    public static Long getLongLittleEndian(String littleEndianHex, int start, int end) {
        int len = end - start;
        if (len != 16) throw new IllegalArgumentException("hex string length must be 16 for long type");
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len / 2; i++) {
            char byteHighPos = littleEndianHex.charAt(start + len - i * 2 - 2);
            char byteLowPos = littleEndianHex.charAt(start + len - i * 2 - 1);
            bytes[i] = (byte) (hexCharToInt(byteHighPos) << 4 | hexCharToInt(byteLowPos));
        }
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * hex to int, thorw exception if the char is not a hex char
     */
    public static int hexCharToInt(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        throw new NumberFormatException("invalid hex char: " + c);
    }
}
