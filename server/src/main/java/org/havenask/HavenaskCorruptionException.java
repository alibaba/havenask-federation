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

package org.havenask;

import java.io.IOException;

/**
 * This exception is thrown when Havenask detects
 * an inconsistency in one of it's persistent files.
 */
public class HavenaskCorruptionException extends IOException {

    /**
     * Creates a new {@link HavenaskCorruptionException}
     * @param message the exception message.
     */
    public HavenaskCorruptionException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link HavenaskCorruptionException} with the given exceptions stacktrace.
     * This constructor copies the stacktrace as well as the message from the given
     * {@code Throwable} into this exception.
     *
     * @param ex the exception cause
     */
    public HavenaskCorruptionException(Throwable ex) {
        super(ex);
    }
}
