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
