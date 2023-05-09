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

package org.havenask.ingest.common;

import org.havenask.dissect.DissectParser;
import org.havenask.ingest.AbstractProcessor;
import org.havenask.ingest.ConfigurationUtils;
import org.havenask.ingest.IngestDocument;
import org.havenask.ingest.Processor;

import java.util.Map;

public final class DissectProcessor extends AbstractProcessor {

    public static final String TYPE = "dissect";
    //package private members for testing
    final String field;
    final boolean ignoreMissing;
    final String pattern;
    final String appendSeparator;
    final DissectParser dissectParser;

    DissectProcessor(String tag, String description, String field, String pattern, String appendSeparator, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
        this.pattern = pattern;
        this.appendSeparator = appendSeparator;
        this.dissectParser = new DissectParser(pattern, appendSeparator);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        String input = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (input == null && ignoreMissing) {
            return ingestDocument;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        dissectParser.parse(input).forEach(ingestDocument::setFieldValue);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public DissectProcessor create(Map<String, Processor.Factory> registry, String processorTag, String description,
                                       Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String pattern = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "pattern");
            String appendSeparator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "append_separator", "");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new DissectProcessor(processorTag, description, field, pattern, appendSeparator, ignoreMissing);
        }
    }
}
