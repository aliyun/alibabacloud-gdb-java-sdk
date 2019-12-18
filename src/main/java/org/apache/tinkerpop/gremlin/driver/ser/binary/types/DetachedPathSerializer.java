/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;

import java.util.List;
import java.util.Set;
/**
 * {@link PathSerializer}
 */
public class DetachedPathSerializer extends CustomSimpleTypeSerializer<DetachedPath> {

    public DetachedPathSerializer() {
        super(DataType.CUSTOM, DataType.PATH.toString());
    }

    @Override
    protected DetachedPath readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final MutablePath path = (MutablePath) MutablePath.make();
        final List<Set<String>> labels = context.read(buffer);
        final List<Object> objects = context.read(buffer);

        if (labels.size() != objects.size()) {
            throw new IllegalStateException("Format for Path object requires that the labels and objects fields be of the same length");
        }
        for (int ix = 0; ix < labels.size(); ix++) {
            path.extend(objects.get(ix), labels.get(ix));
        }

        return DetachedFactory.detach(path, true);
    }

    @Override
    protected void writeValue(final DetachedPath value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        context.write(value.labels(), buffer);
        context.write(value.objects(), buffer);
    }
}
