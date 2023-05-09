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

package org.havenask.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.util.StringBuilders;
import org.havenask.common.Strings;

/**
 * Pattern converter to populate HavenaskMessageField in a pattern.
 * It will only populate these if the event have message of type <code>HavenaskLogMessage</code>.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "HavenaskMessageField")
@ConverterKeys({"HavenaskMessageField"})
public final class HavenaskMessageFieldConverter extends LogEventPatternConverter {

    private String key;

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static HavenaskMessageFieldConverter newInstance(final Configuration config, final String[] options) {
        final String key = options[0];

        return new HavenaskMessageFieldConverter(key);
    }

    public HavenaskMessageFieldConverter(String key) {
        super("HavenaskMessageField", "HavenaskMessageField");
        this.key = key;
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (event.getMessage() instanceof HavenaskLogMessage) {
            HavenaskLogMessage logMessage = (HavenaskLogMessage) event.getMessage();
            final String value = logMessage.getValueFor(key);
            if (Strings.isNullOrEmpty(value) == false) {
                StringBuilders.appendValue(toAppendTo, value);
                return;
            }
        }
        StringBuilders.appendValue(toAppendTo, "");
    }
}
