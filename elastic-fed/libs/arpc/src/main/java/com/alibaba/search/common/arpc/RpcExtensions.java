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

package com.alibaba.search.common.arpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Internal;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamException;

public final class RpcExtensions {
    public static final int GLOBAL_SERVICE_ID_FIELD_NUMBER = 1000;
    public static final GeneratedMessage.GeneratedExtension<DescriptorProtos.ServiceOptions, Integer> globalServiceId = GeneratedMessage.newFileScopedGeneratedExtension(Integer.class, (Message)null);
    public static final int LOCAL_METHOD_ID_FIELD_NUMBER = 1000;
    public static final GeneratedMessage.GeneratedExtension<DescriptorProtos.MethodOptions, Integer> localMethodId = GeneratedMessage.newFileScopedGeneratedExtension(Integer.class, (Message)null);
    private static Descriptors.Descriptor internal_static_arpc_ErrorMsg_descriptor;
    private static GeneratedMessage.FieldAccessorTable internal_static_arpc_ErrorMsg_fieldAccessorTable;
    private static Descriptors.Descriptor internal_static_arpc_TraceInfo_descriptor;
    private static GeneratedMessage.FieldAccessorTable internal_static_arpc_TraceInfo_fieldAccessorTable;
    private static Descriptors.FileDescriptor descriptor;

    private RpcExtensions() {
    }

    public static void registerAllExtensions(ExtensionRegistry registry) {
        registry.add(globalServiceId);
        registry.add(localMethodId);
    }

    public static Descriptors.FileDescriptor getDescriptor() {
        return descriptor;
    }

    static {
        String[] descriptorData = new String[]{"\n\u001farpc/proto/rpc_extensions.proto\u0012\u0004arpc\u001a google/protobuf/descriptor.proto\"1\n\bErrorMsg\u0012\u0011\n\terror_msg\u0018\u0001 \u0001(\t\u0012\u0012\n\nerror_code\u0018\u0002 \u0001(\u0005\"§\u0001\n\tTraceInfo\u0012\u0017\n\u000fserverQueueSize\u0018\u0001 \u0001(\u0005\u0012\u0019\n\u0011handleRequestTime\u0018\u0002 \u0001(\u0003\u0012\u001f\n\u0017workItemWaitProcessTime\u0018\u0003 \u0001(\u0003\u0012\u001b\n\u0013workItemProcessTime\u0018\u0004 \u0001(\u0003\u0012\u001b\n\u0013requestOnServerTime\u0018\u0005 \u0001(\u0003\u0012\u000b\n\u0003rtt\u0018\u0006 \u0001(\u0003:;\n\u0011global_service_id\u0012\u001f.google.protobuf.ServiceOptions\u0018è\u0007 \u0002(\r:8\n\u000flocal_method_id\u0012\u001e.google.protobuf.MethodO", "ptions\u0018è\u0007 \u0002(\rB \n\u001ecom.alibaba.search.common.arpc"};
        Descriptors.FileDescriptor.InternalDescriptorAssigner assigner = new Descriptors.FileDescriptor.InternalDescriptorAssigner() {
            public ExtensionRegistry assignDescriptors(Descriptors.FileDescriptor root) {
                RpcExtensions.descriptor = root;
                RpcExtensions.internal_static_arpc_ErrorMsg_descriptor = (Descriptors.Descriptor)RpcExtensions.getDescriptor().getMessageTypes().get(0);
                RpcExtensions.internal_static_arpc_ErrorMsg_fieldAccessorTable = new GeneratedMessage.FieldAccessorTable(RpcExtensions.internal_static_arpc_ErrorMsg_descriptor, new String[]{"ErrorMsg", "ErrorCode"}, ErrorMsg.class, ErrorMsg.Builder.class);
                RpcExtensions.internal_static_arpc_TraceInfo_descriptor = (Descriptors.Descriptor)RpcExtensions.getDescriptor().getMessageTypes().get(1);
                RpcExtensions.internal_static_arpc_TraceInfo_fieldAccessorTable = new GeneratedMessage.FieldAccessorTable(RpcExtensions.internal_static_arpc_TraceInfo_descriptor, new String[]{"ServerQueueSize", "HandleRequestTime", "WorkItemWaitProcessTime", "WorkItemProcessTime", "RequestOnServerTime", "Rtt"}, TraceInfo.class, TraceInfo.Builder.class);
                RpcExtensions.globalServiceId.internalInit((Descriptors.FieldDescriptor)RpcExtensions.descriptor.getExtensions().get(0));
                RpcExtensions.localMethodId.internalInit((Descriptors.FieldDescriptor)RpcExtensions.descriptor.getExtensions().get(1));
                return null;
            }
        };
        FileDescriptor.internalBuildGeneratedFileFrom(descriptorData, new Descriptors.FileDescriptor[]{DescriptorProtos.getDescriptor()}, assigner);
    }

    public static final class TraceInfo extends GeneratedMessage implements TraceInfoOrBuilder {
        private static final TraceInfo defaultInstance = new TraceInfo(true);
        private int bitField0_;
        public static final int SERVERQUEUESIZE_FIELD_NUMBER = 1;
        private int serverQueueSize_;
        public static final int HANDLEREQUESTTIME_FIELD_NUMBER = 2;
        private long handleRequestTime_;
        public static final int WORKITEMWAITPROCESSTIME_FIELD_NUMBER = 3;
        private long workItemWaitProcessTime_;
        public static final int WORKITEMPROCESSTIME_FIELD_NUMBER = 4;
        private long workItemProcessTime_;
        public static final int REQUESTONSERVERTIME_FIELD_NUMBER = 5;
        private long requestOnServerTime_;
        public static final int RTT_FIELD_NUMBER = 6;
        private long rtt_;
        private byte memoizedIsInitialized;
        private int memoizedSerializedSize;
        private static final long serialVersionUID = 0L;

        private TraceInfo(Builder builder) {
            super(builder);
            this.memoizedIsInitialized = -1;
            this.memoizedSerializedSize = -1;
        }

        private TraceInfo(boolean noInit) {
            this.memoizedIsInitialized = -1;
            this.memoizedSerializedSize = -1;
        }

        public static TraceInfo getDefaultInstance() {
            return defaultInstance;
        }

        public TraceInfo getDefaultInstanceForType() {
            return defaultInstance;
        }

        public static final Descriptors.Descriptor getDescriptor() {
            return RpcExtensions.internal_static_arpc_TraceInfo_descriptor;
        }

        protected GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
            return RpcExtensions.internal_static_arpc_TraceInfo_fieldAccessorTable;
        }

        public boolean hasServerQueueSize() {
            return (this.bitField0_ & 1) == 1;
        }

        public int getServerQueueSize() {
            return this.serverQueueSize_;
        }

        public boolean hasHandleRequestTime() {
            return (this.bitField0_ & 2) == 2;
        }

        public long getHandleRequestTime() {
            return this.handleRequestTime_;
        }

        public boolean hasWorkItemWaitProcessTime() {
            return (this.bitField0_ & 4) == 4;
        }

        public long getWorkItemWaitProcessTime() {
            return this.workItemWaitProcessTime_;
        }

        public boolean hasWorkItemProcessTime() {
            return (this.bitField0_ & 8) == 8;
        }

        public long getWorkItemProcessTime() {
            return this.workItemProcessTime_;
        }

        public boolean hasRequestOnServerTime() {
            return (this.bitField0_ & 16) == 16;
        }

        public long getRequestOnServerTime() {
            return this.requestOnServerTime_;
        }

        public boolean hasRtt() {
            return (this.bitField0_ & 32) == 32;
        }

        public long getRtt() {
            return this.rtt_;
        }

        private void initFields() {
            this.serverQueueSize_ = 0;
            this.handleRequestTime_ = 0L;
            this.workItemWaitProcessTime_ = 0L;
            this.workItemProcessTime_ = 0L;
            this.requestOnServerTime_ = 0L;
            this.rtt_ = 0L;
        }

        public final boolean isInitialized() {
            byte isInitialized = this.memoizedIsInitialized;
            if (isInitialized != -1) {
                return isInitialized == 1;
            } else {
                this.memoizedIsInitialized = 1;
                return true;
            }
        }

        public void writeTo(CodedOutputStream output) throws IOException {
            this.getSerializedSize();
            if ((this.bitField0_ & 1) == 1) {
                output.writeInt32(1, this.serverQueueSize_);
            }

            if ((this.bitField0_ & 2) == 2) {
                output.writeInt64(2, this.handleRequestTime_);
            }

            if ((this.bitField0_ & 4) == 4) {
                output.writeInt64(3, this.workItemWaitProcessTime_);
            }

            if ((this.bitField0_ & 8) == 8) {
                output.writeInt64(4, this.workItemProcessTime_);
            }

            if ((this.bitField0_ & 16) == 16) {
                output.writeInt64(5, this.requestOnServerTime_);
            }

            if ((this.bitField0_ & 32) == 32) {
                output.writeInt64(6, this.rtt_);
            }

            this.getUnknownFields().writeTo(output);
        }

        public int getSerializedSize() {
            int size = this.memoizedSerializedSize;
            if (size != -1) {
                return size;
            } else {
                size = 0;
                if ((this.bitField0_ & 1) == 1) {
                    size += CodedOutputStream.computeInt32Size(1, this.serverQueueSize_);
                }

                if ((this.bitField0_ & 2) == 2) {
                    size += CodedOutputStream.computeInt64Size(2, this.handleRequestTime_);
                }

                if ((this.bitField0_ & 4) == 4) {
                    size += CodedOutputStream.computeInt64Size(3, this.workItemWaitProcessTime_);
                }

                if ((this.bitField0_ & 8) == 8) {
                    size += CodedOutputStream.computeInt64Size(4, this.workItemProcessTime_);
                }

                if ((this.bitField0_ & 16) == 16) {
                    size += CodedOutputStream.computeInt64Size(5, this.requestOnServerTime_);
                }

                if ((this.bitField0_ & 32) == 32) {
                    size += CodedOutputStream.computeInt64Size(6, this.rtt_);
                }

                size += this.getUnknownFields().getSerializedSize();
                this.memoizedSerializedSize = size;
                return size;
            }
        }

        protected Object writeReplace() throws ObjectStreamException {
            return super.writeReplace();
        }

        public static TraceInfo parseFrom(ByteString data) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((ByteString)data)).buildParsed();
        }

        public static TraceInfo parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((ByteString)data, extensionRegistry)).buildParsed();
        }

        public static TraceInfo parseFrom(byte[] data) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((byte[])data)).buildParsed();
        }

        public static TraceInfo parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((byte[])data, extensionRegistry)).buildParsed();
        }

        public static TraceInfo parseFrom(InputStream input) throws IOException {
            return ((Builder)newBuilder().mergeFrom((InputStream)input)).buildParsed();
        }

        public static TraceInfo parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            return ((Builder)newBuilder().mergeFrom((InputStream)input, extensionRegistry)).buildParsed();
        }

        public static TraceInfo parseDelimitedFrom(InputStream input) throws IOException {
            Builder builder = newBuilder();
            return builder.mergeDelimitedFrom(input) ? builder.buildParsed() : null;
        }

        public static TraceInfo parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            Builder builder = newBuilder();
            return builder.mergeDelimitedFrom(input, extensionRegistry) ? builder.buildParsed() : null;
        }

        public static TraceInfo parseFrom(CodedInputStream input) throws IOException {
            return ((Builder)newBuilder().mergeFrom((CodedInputStream)input)).buildParsed();
        }

        public static TraceInfo parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            return newBuilder().mergeFrom(input, extensionRegistry).buildParsed();
        }

        public static Builder newBuilder() {
            return RpcExtensions.TraceInfo.Builder.create();
        }

        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder(TraceInfo prototype) {
            return newBuilder().mergeFrom(prototype);
        }

        public Builder toBuilder() {
            return newBuilder(this);
        }

        protected Builder newBuilderForType(GeneratedMessage.BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        static {
            defaultInstance.initFields();
        }

        public static final class Builder extends GeneratedMessage.Builder<Builder> implements TraceInfoOrBuilder {
            private int bitField0_;
            private int serverQueueSize_;
            private long handleRequestTime_;
            private long workItemWaitProcessTime_;
            private long workItemProcessTime_;
            private long requestOnServerTime_;
            private long rtt_;

            public static final Descriptors.Descriptor getDescriptor() {
                return RpcExtensions.internal_static_arpc_TraceInfo_descriptor;
            }

            protected GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
                return RpcExtensions.internal_static_arpc_TraceInfo_fieldAccessorTable;
            }

            private Builder() {
                this.maybeForceBuilderInitialization();
            }

            private Builder(GeneratedMessage.BuilderParent parent) {
                super(parent);
                this.maybeForceBuilderInitialization();
            }

            private void maybeForceBuilderInitialization() {
                if (RpcExtensions.TraceInfo.alwaysUseFieldBuilders) {
                }

            }

            private static Builder create() {
                return new Builder();
            }

            public Builder clear() {
                super.clear();
                this.serverQueueSize_ = 0;
                this.bitField0_ &= -2;
                this.handleRequestTime_ = 0L;
                this.bitField0_ &= -3;
                this.workItemWaitProcessTime_ = 0L;
                this.bitField0_ &= -5;
                this.workItemProcessTime_ = 0L;
                this.bitField0_ &= -9;
                this.requestOnServerTime_ = 0L;
                this.bitField0_ &= -17;
                this.rtt_ = 0L;
                this.bitField0_ &= -33;
                return this;
            }

            public Builder clone() {
                return create().mergeFrom(this.buildPartial());
            }

            public Descriptors.Descriptor getDescriptorForType() {
                return RpcExtensions.TraceInfo.getDescriptor();
            }

            public TraceInfo getDefaultInstanceForType() {
                return RpcExtensions.TraceInfo.getDefaultInstance();
            }

            public TraceInfo build() {
                TraceInfo result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                } else {
                    return result;
                }
            }

            private TraceInfo buildParsed() throws InvalidProtocolBufferException {
                TraceInfo result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result).asInvalidProtocolBufferException();
                } else {
                    return result;
                }
            }

            public TraceInfo buildPartial() {
                TraceInfo result = new TraceInfo(this);
                int from_bitField0_ = this.bitField0_;
                int to_bitField0_ = 0;
                if ((from_bitField0_ & 1) == 1) {
                    to_bitField0_ |= 1;
                }

                result.serverQueueSize_ = this.serverQueueSize_;
                if ((from_bitField0_ & 2) == 2) {
                    to_bitField0_ |= 2;
                }

                result.handleRequestTime_ = this.handleRequestTime_;
                if ((from_bitField0_ & 4) == 4) {
                    to_bitField0_ |= 4;
                }

                result.workItemWaitProcessTime_ = this.workItemWaitProcessTime_;
                if ((from_bitField0_ & 8) == 8) {
                    to_bitField0_ |= 8;
                }

                result.workItemProcessTime_ = this.workItemProcessTime_;
                if ((from_bitField0_ & 16) == 16) {
                    to_bitField0_ |= 16;
                }

                result.requestOnServerTime_ = this.requestOnServerTime_;
                if ((from_bitField0_ & 32) == 32) {
                    to_bitField0_ |= 32;
                }

                result.rtt_ = this.rtt_;
                result.bitField0_ = to_bitField0_;
                this.onBuilt();
                return result;
            }

            public Builder mergeFrom(Message other) {
                if (other instanceof TraceInfo) {
                    return this.mergeFrom((TraceInfo)other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(TraceInfo other) {
                if (other == RpcExtensions.TraceInfo.getDefaultInstance()) {
                    return this;
                } else {
                    if (other.hasServerQueueSize()) {
                        this.setServerQueueSize(other.getServerQueueSize());
                    }

                    if (other.hasHandleRequestTime()) {
                        this.setHandleRequestTime(other.getHandleRequestTime());
                    }

                    if (other.hasWorkItemWaitProcessTime()) {
                        this.setWorkItemWaitProcessTime(other.getWorkItemWaitProcessTime());
                    }

                    if (other.hasWorkItemProcessTime()) {
                        this.setWorkItemProcessTime(other.getWorkItemProcessTime());
                    }

                    if (other.hasRequestOnServerTime()) {
                        this.setRequestOnServerTime(other.getRequestOnServerTime());
                    }

                    if (other.hasRtt()) {
                        this.setRtt(other.getRtt());
                    }

                    this.mergeUnknownFields(other.getUnknownFields());
                    return this;
                }
            }

            public final boolean isInitialized() {
                return true;
            }

            public Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
                UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder(this.getUnknownFields());

                while(true) {
                    int tag = input.readTag();
                    switch (tag) {
                        case 0:
                            this.setUnknownFields(unknownFields.build());
                            this.onChanged();
                            return this;
                        case 8:
                            this.bitField0_ |= 1;
                            this.serverQueueSize_ = input.readInt32();
                            break;
                        case 16:
                            this.bitField0_ |= 2;
                            this.handleRequestTime_ = input.readInt64();
                            break;
                        case 24:
                            this.bitField0_ |= 4;
                            this.workItemWaitProcessTime_ = input.readInt64();
                            break;
                        case 32:
                            this.bitField0_ |= 8;
                            this.workItemProcessTime_ = input.readInt64();
                            break;
                        case 40:
                            this.bitField0_ |= 16;
                            this.requestOnServerTime_ = input.readInt64();
                            break;
                        case 48:
                            this.bitField0_ |= 32;
                            this.rtt_ = input.readInt64();
                            break;
                        default:
                            if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                                this.setUnknownFields(unknownFields.build());
                                this.onChanged();
                                return this;
                            }
                    }
                }
            }

            public boolean hasServerQueueSize() {
                return (this.bitField0_ & 1) == 1;
            }

            public int getServerQueueSize() {
                return this.serverQueueSize_;
            }

            public Builder setServerQueueSize(int value) {
                this.bitField0_ |= 1;
                this.serverQueueSize_ = value;
                this.onChanged();
                return this;
            }

            public Builder clearServerQueueSize() {
                this.bitField0_ &= -2;
                this.serverQueueSize_ = 0;
                this.onChanged();
                return this;
            }

            public boolean hasHandleRequestTime() {
                return (this.bitField0_ & 2) == 2;
            }

            public long getHandleRequestTime() {
                return this.handleRequestTime_;
            }

            public Builder setHandleRequestTime(long value) {
                this.bitField0_ |= 2;
                this.handleRequestTime_ = value;
                this.onChanged();
                return this;
            }

            public Builder clearHandleRequestTime() {
                this.bitField0_ &= -3;
                this.handleRequestTime_ = 0L;
                this.onChanged();
                return this;
            }

            public boolean hasWorkItemWaitProcessTime() {
                return (this.bitField0_ & 4) == 4;
            }

            public long getWorkItemWaitProcessTime() {
                return this.workItemWaitProcessTime_;
            }

            public Builder setWorkItemWaitProcessTime(long value) {
                this.bitField0_ |= 4;
                this.workItemWaitProcessTime_ = value;
                this.onChanged();
                return this;
            }

            public Builder clearWorkItemWaitProcessTime() {
                this.bitField0_ &= -5;
                this.workItemWaitProcessTime_ = 0L;
                this.onChanged();
                return this;
            }

            public boolean hasWorkItemProcessTime() {
                return (this.bitField0_ & 8) == 8;
            }

            public long getWorkItemProcessTime() {
                return this.workItemProcessTime_;
            }

            public Builder setWorkItemProcessTime(long value) {
                this.bitField0_ |= 8;
                this.workItemProcessTime_ = value;
                this.onChanged();
                return this;
            }

            public Builder clearWorkItemProcessTime() {
                this.bitField0_ &= -9;
                this.workItemProcessTime_ = 0L;
                this.onChanged();
                return this;
            }

            public boolean hasRequestOnServerTime() {
                return (this.bitField0_ & 16) == 16;
            }

            public long getRequestOnServerTime() {
                return this.requestOnServerTime_;
            }

            public Builder setRequestOnServerTime(long value) {
                this.bitField0_ |= 16;
                this.requestOnServerTime_ = value;
                this.onChanged();
                return this;
            }

            public Builder clearRequestOnServerTime() {
                this.bitField0_ &= -17;
                this.requestOnServerTime_ = 0L;
                this.onChanged();
                return this;
            }

            public boolean hasRtt() {
                return (this.bitField0_ & 32) == 32;
            }

            public long getRtt() {
                return this.rtt_;
            }

            public Builder setRtt(long value) {
                this.bitField0_ |= 32;
                this.rtt_ = value;
                this.onChanged();
                return this;
            }

            public Builder clearRtt() {
                this.bitField0_ &= -33;
                this.rtt_ = 0L;
                this.onChanged();
                return this;
            }
        }
    }

    public interface TraceInfoOrBuilder extends MessageOrBuilder {
        boolean hasServerQueueSize();

        int getServerQueueSize();

        boolean hasHandleRequestTime();

        long getHandleRequestTime();

        boolean hasWorkItemWaitProcessTime();

        long getWorkItemWaitProcessTime();

        boolean hasWorkItemProcessTime();

        long getWorkItemProcessTime();

        boolean hasRequestOnServerTime();

        long getRequestOnServerTime();

        boolean hasRtt();

        long getRtt();
    }

    public static final class ErrorMsg extends GeneratedMessage implements ErrorMsgOrBuilder {
        private static final ErrorMsg defaultInstance = new ErrorMsg(true);
        private int bitField0_;
        public static final int ERROR_MSG_FIELD_NUMBER = 1;
        private Object errorMsg_;
        public static final int ERROR_CODE_FIELD_NUMBER = 2;
        private int errorCode_;
        private byte memoizedIsInitialized;
        private int memoizedSerializedSize;
        private static final long serialVersionUID = 0L;

        private ErrorMsg(Builder builder) {
            super(builder);
            this.memoizedIsInitialized = -1;
            this.memoizedSerializedSize = -1;
        }

        private ErrorMsg(boolean noInit) {
            this.memoizedIsInitialized = -1;
            this.memoizedSerializedSize = -1;
        }

        public static ErrorMsg getDefaultInstance() {
            return defaultInstance;
        }

        public ErrorMsg getDefaultInstanceForType() {
            return defaultInstance;
        }

        public static final Descriptors.Descriptor getDescriptor() {
            return RpcExtensions.internal_static_arpc_ErrorMsg_descriptor;
        }

        protected GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
            return RpcExtensions.internal_static_arpc_ErrorMsg_fieldAccessorTable;
        }

        public boolean hasErrorMsg() {
            return (this.bitField0_ & 1) == 1;
        }

        public String getErrorMsg() {
            Object ref = this.errorMsg_;
            if (ref instanceof String) {
                return (String)ref;
            } else {
                ByteString bs = (ByteString)ref;
                String s = bs.toStringUtf8();
                if (Internal.isValidUtf8(bs)) {
                    this.errorMsg_ = s;
                }

                return s;
            }
        }

        private ByteString getErrorMsgBytes() {
            Object ref = this.errorMsg_;
            if (ref instanceof String) {
                ByteString b = ByteString.copyFromUtf8((String)ref);
                this.errorMsg_ = b;
                return b;
            } else {
                return (ByteString)ref;
            }
        }

        public boolean hasErrorCode() {
            return (this.bitField0_ & 2) == 2;
        }

        public int getErrorCode() {
            return this.errorCode_;
        }

        private void initFields() {
            this.errorMsg_ = "";
            this.errorCode_ = 0;
        }

        public final boolean isInitialized() {
            byte isInitialized = this.memoizedIsInitialized;
            if (isInitialized != -1) {
                return isInitialized == 1;
            } else {
                this.memoizedIsInitialized = 1;
                return true;
            }
        }

        public void writeTo(CodedOutputStream output) throws IOException {
            this.getSerializedSize();
            if ((this.bitField0_ & 1) == 1) {
                output.writeBytes(1, this.getErrorMsgBytes());
            }

            if ((this.bitField0_ & 2) == 2) {
                output.writeInt32(2, this.errorCode_);
            }

            this.getUnknownFields().writeTo(output);
        }

        public int getSerializedSize() {
            int size = this.memoizedSerializedSize;
            if (size != -1) {
                return size;
            } else {
                size = 0;
                if ((this.bitField0_ & 1) == 1) {
                    size += CodedOutputStream.computeBytesSize(1, this.getErrorMsgBytes());
                }

                if ((this.bitField0_ & 2) == 2) {
                    size += CodedOutputStream.computeInt32Size(2, this.errorCode_);
                }

                size += this.getUnknownFields().getSerializedSize();
                this.memoizedSerializedSize = size;
                return size;
            }
        }

        protected Object writeReplace() throws ObjectStreamException {
            return super.writeReplace();
        }

        public static ErrorMsg parseFrom(ByteString data) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((ByteString)data)).buildParsed();
        }

        public static ErrorMsg parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((ByteString)data, extensionRegistry)).buildParsed();
        }

        public static ErrorMsg parseFrom(byte[] data) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((byte[])data)).buildParsed();
        }

        public static ErrorMsg parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return ((Builder)newBuilder().mergeFrom((byte[])data, extensionRegistry)).buildParsed();
        }

        public static ErrorMsg parseFrom(InputStream input) throws IOException {
            return ((Builder)newBuilder().mergeFrom((InputStream)input)).buildParsed();
        }

        public static ErrorMsg parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            return ((Builder)newBuilder().mergeFrom((InputStream)input, extensionRegistry)).buildParsed();
        }

        public static ErrorMsg parseDelimitedFrom(InputStream input) throws IOException {
            Builder builder = newBuilder();
            return builder.mergeDelimitedFrom(input) ? builder.buildParsed() : null;
        }

        public static ErrorMsg parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            Builder builder = newBuilder();
            return builder.mergeDelimitedFrom(input, extensionRegistry) ? builder.buildParsed() : null;
        }

        public static ErrorMsg parseFrom(CodedInputStream input) throws IOException {
            return ((Builder)newBuilder().mergeFrom((CodedInputStream)input)).buildParsed();
        }

        public static ErrorMsg parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            return newBuilder().mergeFrom(input, extensionRegistry).buildParsed();
        }

        public static Builder newBuilder() {
            return RpcExtensions.ErrorMsg.Builder.create();
        }

        public Builder newBuilderForType() {
            return newBuilder();
        }

        public static Builder newBuilder(ErrorMsg prototype) {
            return newBuilder().mergeFrom(prototype);
        }

        public Builder toBuilder() {
            return newBuilder(this);
        }

        protected Builder newBuilderForType(GeneratedMessage.BuilderParent parent) {
            Builder builder = new Builder(parent);
            return builder;
        }

        static {
            defaultInstance.initFields();
        }

        public static final class Builder extends GeneratedMessage.Builder<Builder> implements ErrorMsgOrBuilder {
            private int bitField0_;
            private Object errorMsg_;
            private int errorCode_;

            public static final Descriptors.Descriptor getDescriptor() {
                return RpcExtensions.internal_static_arpc_ErrorMsg_descriptor;
            }

            protected GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
                return RpcExtensions.internal_static_arpc_ErrorMsg_fieldAccessorTable;
            }

            private Builder() {
                this.errorMsg_ = "";
                this.maybeForceBuilderInitialization();
            }

            private Builder(GeneratedMessage.BuilderParent parent) {
                super(parent);
                this.errorMsg_ = "";
                this.maybeForceBuilderInitialization();
            }

            private void maybeForceBuilderInitialization() {
                if (RpcExtensions.ErrorMsg.alwaysUseFieldBuilders) {
                }

            }

            private static Builder create() {
                return new Builder();
            }

            public Builder clear() {
                super.clear();
                this.errorMsg_ = "";
                this.bitField0_ &= -2;
                this.errorCode_ = 0;
                this.bitField0_ &= -3;
                return this;
            }

            public Builder clone() {
                return create().mergeFrom(this.buildPartial());
            }

            public Descriptors.Descriptor getDescriptorForType() {
                return RpcExtensions.ErrorMsg.getDescriptor();
            }

            public ErrorMsg getDefaultInstanceForType() {
                return RpcExtensions.ErrorMsg.getDefaultInstance();
            }

            public ErrorMsg build() {
                ErrorMsg result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result);
                } else {
                    return result;
                }
            }

            private ErrorMsg buildParsed() throws InvalidProtocolBufferException {
                ErrorMsg result = this.buildPartial();
                if (!result.isInitialized()) {
                    throw newUninitializedMessageException(result).asInvalidProtocolBufferException();
                } else {
                    return result;
                }
            }

            public ErrorMsg buildPartial() {
                ErrorMsg result = new ErrorMsg(this);
                int from_bitField0_ = this.bitField0_;
                int to_bitField0_ = 0;
                if ((from_bitField0_ & 1) == 1) {
                    to_bitField0_ |= 1;
                }

                result.errorMsg_ = this.errorMsg_;
                if ((from_bitField0_ & 2) == 2) {
                    to_bitField0_ |= 2;
                }

                result.errorCode_ = this.errorCode_;
                result.bitField0_ = to_bitField0_;
                this.onBuilt();
                return result;
            }

            public Builder mergeFrom(Message other) {
                if (other instanceof ErrorMsg) {
                    return this.mergeFrom((ErrorMsg)other);
                } else {
                    super.mergeFrom(other);
                    return this;
                }
            }

            public Builder mergeFrom(ErrorMsg other) {
                if (other == RpcExtensions.ErrorMsg.getDefaultInstance()) {
                    return this;
                } else {
                    if (other.hasErrorMsg()) {
                        this.setErrorMsg(other.getErrorMsg());
                    }

                    if (other.hasErrorCode()) {
                        this.setErrorCode(other.getErrorCode());
                    }

                    this.mergeUnknownFields(other.getUnknownFields());
                    return this;
                }
            }

            public final boolean isInitialized() {
                return true;
            }

            public Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
                UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder(this.getUnknownFields());

                while(true) {
                    int tag = input.readTag();
                    switch (tag) {
                        case 0:
                            this.setUnknownFields(unknownFields.build());
                            this.onChanged();
                            return this;
                        case 10:
                            this.bitField0_ |= 1;
                            this.errorMsg_ = input.readBytes();
                            break;
                        case 16:
                            this.bitField0_ |= 2;
                            this.errorCode_ = input.readInt32();
                            break;
                        default:
                            if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                                this.setUnknownFields(unknownFields.build());
                                this.onChanged();
                                return this;
                            }
                    }
                }
            }

            public boolean hasErrorMsg() {
                return (this.bitField0_ & 1) == 1;
            }

            public String getErrorMsg() {
                Object ref = this.errorMsg_;
                if (!(ref instanceof String)) {
                    String s = ((ByteString)ref).toStringUtf8();
                    this.errorMsg_ = s;
                    return s;
                } else {
                    return (String)ref;
                }
            }

            public Builder setErrorMsg(String value) {
                if (value == null) {
                    throw new NullPointerException();
                } else {
                    this.bitField0_ |= 1;
                    this.errorMsg_ = value;
                    this.onChanged();
                    return this;
                }
            }

            public Builder clearErrorMsg() {
                this.bitField0_ &= -2;
                this.errorMsg_ = RpcExtensions.ErrorMsg.getDefaultInstance().getErrorMsg();
                this.onChanged();
                return this;
            }

            void setErrorMsg(ByteString value) {
                this.bitField0_ |= 1;
                this.errorMsg_ = value;
                this.onChanged();
            }

            public boolean hasErrorCode() {
                return (this.bitField0_ & 2) == 2;
            }

            public int getErrorCode() {
                return this.errorCode_;
            }

            public Builder setErrorCode(int value) {
                this.bitField0_ |= 2;
                this.errorCode_ = value;
                this.onChanged();
                return this;
            }

            public Builder clearErrorCode() {
                this.bitField0_ &= -3;
                this.errorCode_ = 0;
                this.onChanged();
                return this;
            }
        }
    }

    public interface ErrorMsgOrBuilder extends MessageOrBuilder {
        boolean hasErrorMsg();

        String getErrorMsg();

        boolean hasErrorCode();

        int getErrorCode();
    }
}

