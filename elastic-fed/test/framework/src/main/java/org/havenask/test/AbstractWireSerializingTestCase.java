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

package org.havenask.test;

import org.havenask.Version;
import org.havenask.common.io.stream.NamedWriteable;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Standard test case for testing the wire serialization of subclasses of {@linkplain Writeable}.
 * See {@link AbstractNamedWriteableTestCase} for subclasses of {@link NamedWriteable}.
 */
public abstract class AbstractWireSerializingTestCase<T extends Writeable> extends AbstractWireTestCase<T> {
    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    protected abstract Writeable.Reader<T> instanceReader();

    /**
     * Copy the {@link Writeable} by round tripping it through {@linkplain StreamInput} and {@linkplain StreamOutput}.
     */
    @Override
    protected final T copyInstance(T instance, Version version) throws IOException {
        return copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), version);
    }
}
