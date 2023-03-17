package org.havenask.engine.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VersionUtils {

    private static Logger logger = LogManager.getLogger(VersionUtils.class);

    public static long getMaxVersion(Path path, long defaultVersion) throws IOException {
        try (Stream<Path> stream = Files.list(path)) {
            long maxVersion = stream.map(path1 -> path1.getFileName().toString())
                .filter(StringUtils::isNumeric)
                .map(Long::valueOf)
                .max(Long::compare)
                .orElse(defaultVersion);
            logger.info("path[{}] get max version: {}", path, maxVersion);
            return maxVersion;
        }
    }

}
