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

package org.havenask.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilders;
import org.havenask.common.Booleans;
import org.havenask.common.Strings;
import org.havenask.common.logging.HavenaskLogMessage;
import org.havenask.common.logging.Loggers;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.index.engine.Engine;
import org.havenask.index.mapper.ParsedDocument;
import org.havenask.index.shard.IndexingOperationListener;
import org.havenask.index.shard.ShardId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class IndexingSlowLog implements IndexingOperationListener {
    public static final String INDEX_INDEXING_SLOWLOG_PREFIX = "index.indexing.slowlog";
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.warn", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.info", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.debug", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING =
        Setting.timeSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".threshold.index.trace", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<Boolean> INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING =
        Setting.boolSetting(INDEX_INDEXING_SLOWLOG_PREFIX +".reformat", true, Property.Dynamic, Property.IndexScope);
    public static final Setting<SlowLogLevel> INDEX_INDEXING_SLOWLOG_LEVEL_SETTING =
        new Setting<>(INDEX_INDEXING_SLOWLOG_PREFIX +".level", SlowLogLevel.TRACE.name(), SlowLogLevel::parse, Property.Dynamic,
            Property.IndexScope);

    private final Logger indexLogger;
    private final Index index;

    private boolean reformat;
    private long indexWarnThreshold;
    private long indexInfoThreshold;
    private long indexDebugThreshold;
    private long indexTraceThreshold;
    /*
     * How much of the source to log in the slowlog - 0 means log none and anything greater than 0 means log at least that many
     * <em>characters</em> of the source.
     */
    private int maxSourceCharsToLog;
    private SlowLogLevel level;

    /**
     * Reads how much of the source to log. The user can specify any value they
     * like and numbers are interpreted the maximum number of characters to log
     * and everything else is interpreted as Havenask interprets booleans
     * which is then converted to 0 for false and Integer.MAX_VALUE for true.
     */
    public static final Setting<Integer> INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING =
            new Setting<>(INDEX_INDEXING_SLOWLOG_PREFIX + ".source", "1000", (value) -> {
                try {
                    return Integer.parseInt(value, 10);
                } catch (NumberFormatException e) {
                    return Booleans.parseBoolean(value, true) ? Integer.MAX_VALUE : 0;
                }
            }, Property.Dynamic, Property.IndexScope);

    IndexingSlowLog(IndexSettings indexSettings) {
        this.indexLogger = LogManager.getLogger(INDEX_INDEXING_SLOWLOG_PREFIX + ".index");
        Loggers.setLevel(this.indexLogger, SlowLogLevel.TRACE.name());
        this.index = indexSettings.getIndex();

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING, this::setReformat);
        this.reformat = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING);
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING, this::setWarnThreshold);
        this.indexWarnThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING).nanos();
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING, this::setInfoThreshold);
        this.indexInfoThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING).nanos();
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING, this::setDebugThreshold);
        this.indexDebugThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING).nanos();
        indexSettings.getScopedSettings()
                .addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING, this::setTraceThreshold);
        this.indexTraceThreshold = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_LEVEL_SETTING, this::setLevel);
        setLevel(indexSettings.getValue(INDEX_INDEXING_SLOWLOG_LEVEL_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING,
                this::setMaxSourceCharsToLog);
        this.maxSourceCharsToLog = indexSettings.getValue(INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING);
    }

    private void setMaxSourceCharsToLog(int maxSourceCharsToLog) {
        this.maxSourceCharsToLog = maxSourceCharsToLog;
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
    }

    private void setWarnThreshold(TimeValue warnThreshold) {
        this.indexWarnThreshold = warnThreshold.nanos();
    }

    private void setInfoThreshold(TimeValue infoThreshold) {
        this.indexInfoThreshold = infoThreshold.nanos();
    }

    private void setDebugThreshold(TimeValue debugThreshold) {
        this.indexDebugThreshold = debugThreshold.nanos();
    }

    private void setTraceThreshold(TimeValue traceThreshold) {
        this.indexTraceThreshold = traceThreshold.nanos();
    }

    private void setReformat(boolean reformat) {
        this.reformat = reformat;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index indexOperation, Engine.IndexResult result) {
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            final ParsedDocument doc = indexOperation.parsedDoc();
            final long tookInNanos = result.getTook();
            // when logger level is more specific than WARN AND event is within threshold it should be logged
            if (indexWarnThreshold >= 0 && tookInNanos > indexWarnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
                indexLogger.warn( new IndexingSlowLogMessage(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexInfoThreshold >= 0 && tookInNanos > indexInfoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
                indexLogger.info(new IndexingSlowLogMessage(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexDebugThreshold >= 0 && tookInNanos > indexDebugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
                indexLogger.debug(new IndexingSlowLogMessage(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            } else if (indexTraceThreshold >= 0 && tookInNanos > indexTraceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
                indexLogger.trace( new IndexingSlowLogMessage(index, doc, tookInNanos, reformat, maxSourceCharsToLog));
            }
        }
    }

    static final class IndexingSlowLogMessage extends HavenaskLogMessage {

        IndexingSlowLogMessage(Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog) {
            super(prepareMap(index,doc,tookInNanos,reformat,maxSourceCharsToLog),
                message(index,doc,tookInNanos,reformat,maxSourceCharsToLog));
        }

        private static Map<String, Object> prepareMap(Index index, ParsedDocument doc, long tookInNanos, boolean reformat,
                                                      int maxSourceCharsToLog) {
            Map<String,Object> map = new HashMap<>();
            map.put("message", index);
            map.put("took", TimeValue.timeValueNanos(tookInNanos));
            map.put("took_millis", ""+TimeUnit.NANOSECONDS.toMillis(tookInNanos));
            map.put("doc_type", doc.type());
            map.put("id", doc.id());
            map.put("routing", doc.routing());

            if (maxSourceCharsToLog == 0 || doc.source() == null || doc.source().length() == 0) {
                return map;
            }
            try {
                String source = XContentHelper.convertToJson(doc.source(), reformat, doc.getXContentType());
                String trim = Strings.cleanTruncate(source, maxSourceCharsToLog).trim();
                StringBuilder sb  = new StringBuilder(trim);
                StringBuilders.escapeJson(sb,0);
                map.put("source", sb.toString());
            } catch (IOException e) {
                StringBuilder sb  = new StringBuilder("_failed_to_convert_[" + e.getMessage()+"]");
                StringBuilders.escapeJson(sb,0);
                map.put("source", sb.toString());
                /*
                 * We choose to fail to write to the slow log and instead let this percolate up to the post index listener loop where this
                 * will be logged at the warn level.
                 */
                final String message = String.format(Locale.ROOT, "failed to convert source for slow log entry [%s]", map.toString());
                throw new UncheckedIOException(message, e);
            }
            return map;
        }

        private static String message(Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog) {
            StringBuilder sb = new StringBuilder();
            sb.append(index).append(" ");
            sb.append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], ");
            sb.append("took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ");
            sb.append("type[").append(doc.type()).append("], ");
            sb.append("id[").append(doc.id()).append("], ");
            if (doc.routing() == null) {
                sb.append("routing[]");
            } else {
                sb.append("routing[").append(doc.routing()).append("]");
            }

            if (maxSourceCharsToLog == 0 || doc.source() == null || doc.source().length() == 0) {
                return sb.toString();
            }
            try {
                String source = XContentHelper.convertToJson(doc.source(), reformat, doc.getXContentType());
                sb.append(", source[").append(Strings.cleanTruncate(source, maxSourceCharsToLog).trim()).append("]");
            } catch (IOException e) {
                sb.append(", source[_failed_to_convert_[").append(e.getMessage()).append("]]");
                /*
                 * We choose to fail to write to the slow log and instead let this percolate up to the post index listener loop where this
                 * will be logged at the warn level.
                 */
                final String message = String.format(Locale.ROOT, "failed to convert source for slow log entry [%s]", sb.toString());
                throw new UncheckedIOException(message, e);
            }
            return sb.toString();
        }
    }

    boolean isReformat() {
        return reformat;
    }

    long getIndexWarnThreshold() {
        return indexWarnThreshold;
    }

    long getIndexInfoThreshold() {
        return indexInfoThreshold;
    }

    long getIndexTraceThreshold() {
        return indexTraceThreshold;
    }

    long getIndexDebugThreshold() {
        return indexDebugThreshold;
    }

    int getMaxSourceCharsToLog() {
        return maxSourceCharsToLog;
    }

    SlowLogLevel getLevel() {
        return level;
    }

}
