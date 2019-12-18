/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import org.apache.commons.collections.IteratorUtils;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import java.util.List;

/**
 * {@link ListSerializer}
 * {@link PathSerializer}
 * {@link VertexSerializer}
 */
public class DetachedVertexSerializer extends CustomSimpleTypeSerializer<DetachedVertex>  {
    public DetachedVertexSerializer() {
        super(DataType.CUSTOM, DataType.VERTEX.toString());
    }

    @Override
    protected DetachedVertex readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final Object id = context.read(buffer);
        final String label = context.readValue(buffer, String.class, false);
        final List<VertexProperty> vertexProperties = context.read(buffer);
        final DetachedVertex.Builder v = DetachedVertex.build();
        v.setId(id).setLabel(label);
        vertexProperties.forEach(vertexProperty -> v.addProperty(DetachedVertexProperty.build().setId(vertexProperty.id()).setLabel(vertexProperty.key()).setValue(vertexProperty.value()).create()));
        return v.create();
    }

    @Override
    protected void writeValue(final DetachedVertex value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        context.write(value.id(), buffer);
        context.writeValue(value.label(), buffer, false);
        List<VertexProperty> vertexProperties = IteratorUtils.toList(value.properties());
        context.write(vertexProperties, buffer);
    }
}
