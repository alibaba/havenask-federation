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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SuppressForbidden;

@SuppressForbidden(reason = "We need to use serialization to convert the vector to byte array")
public class VectorField extends Field {

    public <T> VectorField(String name, T value, IndexableFieldType type) {
        super(name, new BytesRef(), type);
        try {
            this.setBytesValue(toByte(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> byte[] toByte(T values) throws Exception {
        byte[] bytes;
        try (
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)
        ) {
            objectStream.writeObject(values);
            bytes = byteStream.toByteArray();
        }
        return bytes;
    }

    public static Object readValue(byte[] value) throws IOException {
        try (
            ByteArrayInputStream byteStream = new ByteArrayInputStream(value);
            ObjectInputStream objectStream = new ObjectInputStream(byteStream)
        ) {
            return objectStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
