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

package org.havenask.engine.search.action;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.RAMOutputStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;

public class TransportHavenaskSearchHelper {
    public static final String HAVENASK_SCROLL_ID = "havenask_scroll_id";

    static String buildHavenaskScrollId(String nodeId, String scrollSessionId) {
        try (RAMOutputStream out = new RAMOutputStream()) {
            out.writeString(HAVENASK_SCROLL_ID);
            out.writeString(nodeId);
            out.writeString(scrollSessionId);
            byte[] bytes = new byte[(int) out.getFilePointer()];
            out.writeTo(bytes, 0);
            return Base64.getUrlEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static ParsedHavenaskScrollId parseHavenaskScrollId(String scrollId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            final String firstChunk = in.readString();
            if (!HAVENASK_SCROLL_ID.equals(firstChunk)) {
                throw new IllegalArgumentException("Invalid havenask scroll id");
            }
            String nodeId = in.readString();
            String scrollSessionId = in.readString();
            return new ParsedHavenaskScrollId(nodeId, scrollSessionId);
        } catch (Exception e) {
            throw new IllegalArgumentException("parse havenask scroll id failed: ", e);
        }
    }
}
