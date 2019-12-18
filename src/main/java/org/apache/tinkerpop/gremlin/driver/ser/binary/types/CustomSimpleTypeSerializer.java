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
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.CustomTypeSerializer;

/**
 * Base class for serialization of types that don't contain type specific information only {type_code}, {value_flag}
 * and {value}.
 */
public abstract class CustomSimpleTypeSerializer<T> implements CustomTypeSerializer<T> {
    private final DataType dataType;
    private final String typeName;

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    public CustomSimpleTypeSerializer(final DataType dataType, final String typeName) {
        this.dataType = dataType;
        this.typeName = typeName;
    }

    @Override
    public T read(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        // No {type_info}, just {value_flag}{value}
        return readValue(buffer, context, true);
    }

    @Override
    public T readValue(final ByteBuf buffer, final GraphBinaryReader context, final boolean nullable) throws SerializationException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }

        return readValue(buffer, context);
    }

    /**
     * Reads a non-nullable value according to the type format.
     * @param buffer A buffer which reader index has been set to the beginning of the {value}.
     * @param context The binary writer.
     * @return
     * @throws SerializationException
     */
    protected abstract T readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException;

    @Override
    public void write(final T value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        writeValue(value, buffer, context, true);
    }

    @Override
    public void writeValue(final T value, final ByteBuf buffer, final GraphBinaryWriter context, final boolean nullable) throws SerializationException {
        if (value == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            context.writeValueFlagNull(buffer);
            return;
        }

        if (nullable) {
            context.writeValueFlagNone(buffer);
        }

        writeValue(value, buffer, context);
    }

    /**
     * Writes a non-nullable value into a buffer using the provided allocator.
     * @param value A non-nullable value.
     * @param buffer The buffer allocator to use.
     * @param context The binary writer.
     * @throws SerializationException
     */
    protected abstract void writeValue(final T value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException;
}
