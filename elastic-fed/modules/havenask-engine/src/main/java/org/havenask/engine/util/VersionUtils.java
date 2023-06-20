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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VersionUtils {

    private static Logger logger = LogManager.getLogger(VersionUtils.class);

    public synchronized static long getMaxVersionAndExpireOldVersion(Path path, long defaultVersion) throws IOException {
        try (Stream<Path> stream = Files.list(path)) {
            // 删除最早的目录,只保存10个目录
            long count = stream.count();
            if (count > 10) {
                stream.map(path1 -> path1.getFileName().toString())
                    .filter(StringUtils::isNumeric)
                    .map(Long::valueOf)
                    .sorted(Long::compare)
                    .limit(count - 10)
                    .forEach(version -> {
                        try {
                            Files.delete(path.resolve(String.valueOf(version)));
                            logger.info("path [{}] delete old version: {}", path, version);
                        } catch (IOException e) {
                            logger.error("delete old version error", e);
                        }
                    });
            }
        }

        try (Stream<Path> stream = Files.list(path)) {
            long maxVersion = stream.map(path1 -> path1.getFileName().toString())
                .filter(StringUtils::isNumeric)
                .map(Long::valueOf)
                .max(Long::compare)
                .orElse(defaultVersion);
            logger.debug("path[{}] get max version: {}", path, maxVersion);
            return maxVersion;
        }
    }

    public synchronized static void copyVersionDir(Path sourceDir, Path targetDir) throws IOException {
        Files.walk(sourceDir).forEach(sourcePath -> {
            Path targetPath = targetDir.resolve(sourceDir.relativize(sourcePath));
            try {
                Files.copy(sourcePath, targetPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
