/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.driver.ser.binary.types.DetachedEdgeSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.DetachedPathSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.DetachedVertexSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.driver.ser.binary.RequestMessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.ResponseMessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

/**
 * @see GraphBinaryMessageSerializerV1
 */
public class GraphDbGraphBinaryMessageSerializerV1 extends GraphBinaryMessageSerializerV1 {

    private GraphBinaryReader reader;
    private GraphBinaryWriter writer;
    private RequestMessageSerializer requestSerializer;
    private ResponseMessageSerializer responseSerializer;

    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private int estimateSize = DEFAULT_BUFFER_SIZE;

    public GraphDbGraphBinaryMessageSerializerV1() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphDbGraphBinaryMessageSerializerV1(final TypeSerializerRegistry.Builder builder) {
        this(builder.create());
    }

    public GraphDbGraphBinaryMessageSerializerV1(final TypeSerializerRegistry registry) {
        super(registry);
        //old version writer = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE);
        writer = new GraphBinaryWriter(TypeSerializerRegistry.build()
                .addCustomType(DetachedVertex.class, new DetachedVertexSerializer())
                .addCustomType(DetachedEdge.class, new DetachedEdgeSerializer())
                .addCustomType(DetachedPath.class, new DetachedPathSerializer())
                .create());

        reader = new GraphBinaryReader(TypeSerializerRegistry.build()
                .addCustomType(DetachedVertex.class, new DetachedVertexSerializer())
                .addCustomType(DetachedEdge.class, new DetachedEdgeSerializer())
                .addCustomType(DetachedPath.class, new DetachedPathSerializer())
                .create());

        requestSerializer = new RequestMessageSerializer();
        responseSerializer = new ResponseMessageSerializer();
    }

    /**
     * server do response
     */
    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {

        final ByteBuf buffer = allocator.buffer(estimateSize);
        try {
            responseSerializer.writeValue(responseMessage, buffer, writer);
        } catch (Exception ex) {
            buffer.release();
            throw ex;
        }

        estimateSize = buffer.readableBytes();
        return buffer;
    }

    /**
     * driver parse response
     * @param msg the message buffer to deserialize
     * @return the deserialized response message
     * @throws SerializationException if an error occurs during deserialization
     */
    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        return responseSerializer.readValue(msg, reader);
    }
}
