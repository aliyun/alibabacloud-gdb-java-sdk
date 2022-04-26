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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import org.apache.commons.collections.IteratorUtils;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DetachedEdgeSerializer extends CustomSimpleTypeSerializer<DetachedEdge> {
    public DetachedEdgeSerializer() {
        super(DataType.CUSTOM, DataType.EDGE.toString());
    }

    @Override
    protected DetachedEdge readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        Object id = context.read(buffer);
        String label = (String) context.readValue(buffer, String.class, false);
        Object inVId = context.read(buffer);
        String inVLabel = (String) context.readValue(buffer, String.class, false);
        Object outVId = context.read(buffer);
        String outVLabel = (String) context.readValue(buffer, String.class, false);
        List<Property> properties = context.read(buffer);
        context.read(buffer);

        Map<String, Object> stringObjectMap = new HashMap<>(4);
        properties.forEach(property -> stringObjectMap.put(property.key(), property));
        //return new DetachedEdge(id, label, properties.stream().collect(Collectors.toMap(Property::key, Function.identity())), inV, outV);
        return new DetachedEdge(id, label, stringObjectMap, outVId, outVLabel, inVId, inVLabel);
    }

    @Override
    protected void writeValue(final DetachedEdge value, final ByteBuf buffer, final GraphBinaryWriter context) throws SerializationException {
        context.write(value.id(), buffer);
        context.writeValue(value.label(), buffer, false);
        context.write(value.inVertex().id(), buffer);
        context.writeValue(value.inVertex().label(), buffer, false);
        context.write(value.outVertex().id(), buffer);
        context.writeValue(value.outVertex().label(), buffer, false);
        List<Property> properties = IteratorUtils.toList(value.properties());
        context.write(properties, buffer);
        context.write((Object) null, buffer);
    }
}
