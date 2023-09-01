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

package org.havenask.engine.index.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper.DenseVectorFieldType;
import org.havenask.index.mapper.DocumentMapper;
import org.havenask.index.mapper.MapperParsingException;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperTestCase;
import org.havenask.index.mapper.ParsedDocument;
import org.havenask.plugins.Plugin;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@AwaitsFix(bugUrl = "https://github.com/alibaba/havenask-federation/issues/202")
public class DenseVectorFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new HavenaskEnginePlugin(Settings.EMPTY));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "dense_vector").field("dims", 4);
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "dims",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4)),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 5))
        );
        checker.registerConflictCheck(
            "similarity",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("similarity", "dot_product")),
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("similarity", "l2_norm"))
        );
        checker.registerConflictCheck(
            "index_options",
            fieldMapping(b -> b.field("type", "dense_vector").field("dims", 4).field("similarity", "dot_product")),
            fieldMapping(
                b -> b.field("type", "dense_vector")
                    .field("dims", 4)
                    .field("similarity", "dot_product")
                    .startObject("index_options")
                    .field("type", "hc")
                    .endObject()
            )
        );
    }

    @Override
    public void testMeta() throws IOException {
        // TODO base testMeta failed, need to fix
    }

    public void testFieldType() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 4);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.endObject();
        }));
        DenseVectorFieldType denseVectorFieldType = (DenseVectorFieldType) mapperService.fieldType("field");
        assertThat(denseVectorFieldType.isSearchable(), equalTo(false));
        assertThat(denseVectorFieldType.isAggregatable(), equalTo(false));
        assertThat(denseVectorFieldType.isStored(), equalTo(false));
    }

    public void testIndexedVector() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 4);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.endObject();
        }));

        float[] vector = new float[] { 1.0f, -2.0f, 3.0f, -4.0f };
        ParsedDocument doc = mapper.parse(source(b -> b.array("field", vector)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(VectorField.class));

        VectorField vectorField = (VectorField) fields[0];
        float[] array = (float[]) VectorField.readValue(vectorField.binaryValue().bytes);
        assertArrayEquals("Parsed vector is not equal to original.", vector, array, 0.001f);
    }

    public void testInvalidParameters() {
        // invalid dims
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 0)))
        );
        assertThat(e.getMessage(), containsString("Dimension value must be greater than 0 for vector: field"));

        MapperParsingException e2 = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "dense_vector").field("dims", 10000)))
        );
        assertThat(e2.getMessage(), containsString("Dimension value must be less than 2048 for vector: field"));

        // invalid similarity
        MapperParsingException e3 = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(b -> b.field("type", "dense_vector").field("dims", 3).field("similarity", "wrong_similarity"))
            )
        );
        assertThat(e3.getMessage(), containsString("No similarity matches wrong_similarity"));

        // invalid index_options
        MapperParsingException e4 = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector")
                        .field("dims", 3)
                        .field("similarity", "dot_product")
                        .startObject("index_options")
                        .field("type", "wrong_type")
                        .endObject()
                )
            )
        );
        assertThat(e4.getMessage(), containsString("No algorithm matches wrong_type"));
    }

    public void testWriteWrongDims() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 4);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "hnsw");
            b.endObject();
        }));

        float[] vector = new float[] { 1.0f, -2.0f, 3.0f };
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.array("field", vector))));
        assertThat(e.getCause().getMessage(), containsString("vector length expects: 4, actually: 3"));
    }
}
