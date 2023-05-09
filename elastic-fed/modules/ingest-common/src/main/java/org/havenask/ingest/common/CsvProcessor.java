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

import org.havenask.ingest.AbstractProcessor;
import org.havenask.ingest.ConfigurationUtils;
import org.havenask.ingest.IngestDocument;
import org.havenask.ingest.Processor;

import java.util.List;
import java.util.Map;

import static org.havenask.ingest.ConfigurationUtils.newConfigurationException;

/**
 * A processor that breaks line from CSV file into separate fields.
 * If there's more fields requested than there is in the CSV, extra field will not be present in the document after processing.
 * In the same way this processor will skip any field that is empty in CSV.
 * <p>
 * By default it uses rules according to <a href="https://tools.ietf.org/html/rfc4180">RCF 4180</a> with one exception: whitespaces are
 * allowed before or after quoted field. Processor can be tweaked with following parameters:
 * <p>
 * quote: set custom quote character (defaults to ")
 * separator: set custom separator (defaults to ,)
 * trim: trim leading and trailing whitespaces in unquoted fields
 * empty_value: sets custom value to use for empty fields (field is skipped if null)
 */
public final class CsvProcessor extends AbstractProcessor {

    public static final String TYPE = "csv";

    //visible for testing
    final String field;
    final String[] headers;
    final boolean trim;
    final char quote;
    final char separator;
    final boolean ignoreMissing;
    final Object emptyValue;

    CsvProcessor(String tag, String description, String field, String[] headers, boolean trim, char separator, char quote,
                 boolean ignoreMissing, Object emptyValue) {
        super(tag, description);
        this.field = field;
        this.headers = headers;
        this.trim = trim;
        this.quote = quote;
        this.separator = separator;
        this.ignoreMissing = ignoreMissing;
        this.emptyValue = emptyValue;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        if (headers.length == 0) {
            return ingestDocument;
        }

        String line = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (line == null && ignoreMissing) {
            return ingestDocument;
        } else if (line == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        new CsvParser(ingestDocument, quote, separator, trim, headers, emptyValue).process(line);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements org.havenask.ingest.Processor.Factory {
        @Override
        public CsvProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                   String description, Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String quote = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "quote", "\"");
            if (quote.length() != 1) {
                throw newConfigurationException(TYPE, processorTag, "quote", "quote has to be single character like \" or '");
            }
            String separator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "separator", ",");
            if (separator.length() != 1) {
                throw newConfigurationException(TYPE, processorTag, "separator", "separator has to be single character like , or ;");
            }
            boolean trim = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "trim", false);
            Object emptyValue = null;
            if(config.containsKey("empty_value")){
                emptyValue = ConfigurationUtils.readObject(TYPE, processorTag, config, "empty_value");
            }
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            List<String> targetFields = ConfigurationUtils.readList(TYPE, processorTag, config, "target_fields");
            if (targetFields.isEmpty()) {
                throw newConfigurationException(TYPE, processorTag, "target_fields", "target fields list can't be empty");
            }
            return new CsvProcessor(processorTag, description, field, targetFields.toArray(new String[0]), trim, separator.charAt(0),
                quote.charAt(0), ignoreMissing, emptyValue);
        }
    }
}
