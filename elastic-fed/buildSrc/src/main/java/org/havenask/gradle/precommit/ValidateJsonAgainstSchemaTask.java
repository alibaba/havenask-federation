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

package org.havenask.gradle.precommit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaException;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.gradle.api.DefaultTask;
import org.gradle.api.UncheckedIOException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.ChangeType;
import org.gradle.work.Incremental;
import org.gradle.work.InputChanges;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Incremental task to validate a set of JSON files against against a schema.
 */
public class ValidateJsonAgainstSchemaTask extends DefaultTask {

    private final ObjectMapper mapper = new ObjectMapper();
    private File jsonSchema;
    private File report;
    private FileCollection inputFiles;

    @Incremental
    @InputFiles
    public FileCollection getInputFiles() {
        return inputFiles;
    }

    public void setInputFiles(FileCollection inputFiles) {
        this.inputFiles = inputFiles;
    }

    @InputFile
    public File getJsonSchema() {
        return jsonSchema;
    }

    public void setJsonSchema(File jsonSchema) {
        this.jsonSchema = jsonSchema;
    }

    public void setReport(File report) {
        this.report = report;
    }

    @OutputFile
    public File getReport() {
        return this.report;
    }

    @TaskAction
    public void validate(InputChanges inputChanges) throws IOException {
        File jsonSchemaOnDisk = getJsonSchema();
        getLogger().debug("JSON schema : [{}]", jsonSchemaOnDisk.getAbsolutePath());
        SchemaValidatorsConfig config = new SchemaValidatorsConfig();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        JsonSchema jsonSchema = factory.getSchema(mapper.readTree(jsonSchemaOnDisk), config);
        Map<File, Set<String>> errors = new LinkedHashMap<>();
        // incrementally evaluate input files
        StreamSupport.stream(inputChanges.getFileChanges(getInputFiles()).spliterator(), false)
            .filter(f -> f.getChangeType() != ChangeType.REMOVED)
            .forEach(fileChange -> {
                File file = fileChange.getFile();
                if (file.isDirectory() == false) {
                    // validate all files and hold on to errors for a complete report if there are failures
                    getLogger().debug("Validating JSON [{}]", file.getName());
                    try {
                        Set<ValidationMessage> validationMessages = jsonSchema.validate(mapper.readTree(file));
                        maybeLogAndCollectError(validationMessages, errors, file);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        if (errors.isEmpty()) {
            try (PrintWriter printWriter = new PrintWriter(getReport())) {
                printWriter.println("Success! No validation errors found.");
            }
        } else {
            try (PrintWriter printWriter = new PrintWriter(getReport())) {
                printWriter.printf("Schema: %s%n", jsonSchemaOnDisk);
                printWriter.println("----------Validation Errors-----------");
                errors.values().stream().flatMap(Collection::stream).forEach(printWriter::println);
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Error validating JSON. See the report at: ");
            sb.append(getReport().toURI().toASCIIString());
            sb.append(System.lineSeparator());
            sb.append(
                String.format("JSON validation failed: %d files contained %d violations", errors.keySet().size(), errors.values().size())
            );
            throw new JsonSchemaException(sb.toString());
        }
    }

    private void maybeLogAndCollectError(Set<ValidationMessage> messages, Map<File, Set<String>> errors, File file) {
        for (ValidationMessage message : messages) {
            getLogger().error("[validate JSON][ERROR][{}][{}]", file.getName(), message.toString());
            errors.computeIfAbsent(file, k -> new LinkedHashSet<>())
                .add(String.format("%s: %s", file.getAbsolutePath(), message.toString()));
        }
    }
}
