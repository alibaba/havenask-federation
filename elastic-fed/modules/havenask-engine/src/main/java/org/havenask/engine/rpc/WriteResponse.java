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

package org.havenask.engine.rpc;

import suez.service.proto.ErrorCode;

public class WriteResponse {
    private final long checkpoint;
    private final ErrorCode errorCode;
    private final String errorMessage;

    public WriteResponse(long checkpoint) {
        this.checkpoint = checkpoint;
        this.errorCode = null;
        this.errorMessage = "";
    }

    public WriteResponse(ErrorCode errorCode, String errorMessage) {
        this.checkpoint = -1L;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
