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

package org.havenask.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpResponse;
import org.havenask.common.Booleans;
import org.havenask.transport.NettyAllocator;

import java.util.List;

/**
 * Split up large responses to prevent batch compression {@link JdkZlibEncoder} down the pipeline.
 */
@ChannelHandler.Sharable
class Netty4HttpResponseCreator extends MessageToMessageEncoder<Netty4HttpResponse> {

    private static final String DO_NOT_SPLIT = "havenask.unsafe.do_not_split_http_responses";

    private static final boolean DO_NOT_SPLIT_HTTP_RESPONSES;
    private static final int SPLIT_THRESHOLD;

    static {
        DO_NOT_SPLIT_HTTP_RESPONSES = Booleans.parseBoolean(System.getProperty(DO_NOT_SPLIT), false);
        // Netty will add some header bytes if it compresses this message. So we downsize slightly.
        SPLIT_THRESHOLD = (int) (NettyAllocator.suggestedMaxAllocationSize() * 0.99);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Netty4HttpResponse msg, List<Object> out) {
        if (DO_NOT_SPLIT_HTTP_RESPONSES || msg.content().readableBytes() <= SPLIT_THRESHOLD) {
            out.add(msg.retain());
        } else {
            HttpResponse response = new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers());
            out.add(response);
            ByteBuf content = msg.content();
            while (content.readableBytes() > SPLIT_THRESHOLD) {
                out.add(new DefaultHttpContent(content.readRetainedSlice(SPLIT_THRESHOLD)));
            }
            out.add(new DefaultLastHttpContent(content.readRetainedSlice(content.readableBytes())));
        }
    }
}
