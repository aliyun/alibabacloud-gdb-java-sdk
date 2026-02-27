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
package org.apache.tinkerpop.gremlin.groovy;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.*;
import java.util.stream.Collectors;

public class GdbGroovyTranslator implements  Translator.ScriptTranslator {
    private final String traversalSource;

    private GdbGroovyTranslator(final String traversalSource) {
        this.traversalSource = traversalSource;
    }

    public static final GdbGroovyTranslator of(final String traversalSource) {
        return new GdbGroovyTranslator(traversalSource);
    }


    public ParameterizedScriptResult translateParameterization(final Bytecode bytecode) {
        return new ParameterizedScriptResult(this.internalTranslate(this.traversalSource, bytecode),Bindings.relase());
    }

    @Override
    public String translate(final Bytecode bytecode) {
        return this.internalTranslate(this.traversalSource, bytecode);
    }

    @Override
    public String getTargetLanguage() {
        return "gdb-gremlin-groovy";
    }

    @Override
    public String toString() {
        return StringFactory.translatorString(this);
    }

    @Override
    public String getTraversalSource() {
        return this.traversalSource;
    }

    /**
     * Translate bytecode to parameterized script.
     * Represents the result of translating bytecode to a parameterized Gremlin script.
     */
    public static final class ParameterizedScriptResult {
        private final String  script;
        private final Map<String,Object> bindings = new HashMap<>();

        public ParameterizedScriptResult(String script, Map<String,Object> bindings) {
            this.script = script;
            this.bindings.putAll(bindings);
        }

        /**
         * Provides the script
         * @return the script string
         */
        public String getScript() {
            return script;
        }

        /**
         * Provides the bindings
         * @return the bindings map
         */
        public Map<String,Object> getBindings() {
            return bindings;
        }
    }

    public static final class Bindings {
        private static final ThreadLocal<Map<Object, String>> MAP = ThreadLocal.withInitial(HashMap::new);

        public static <V> V put(final V value,final String variable) {
            MAP.get().put(value,variable);
            return value;
        }

        public static <V> String getBoundVariableOrAssign(final V value) {
            final Map<Object, String> map = MAP.get();
            if (! map.containsKey(value)) {
                put(value,getNextProxyKey());
            }
            return map.get(value);
        }

        public static Map<String,Object> relase(){
            Map<String,Object> result = MAP.get().entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue,Map.Entry::getKey));
            MAP.get().clear();
            return result;
        }

        private static String getNextProxyKey() {
            return "_args_" + String.valueOf(MAP.get().size());
        }

        @Override
        public String toString() {
            return "bindings[" + Thread.currentThread().getName() + "]";
        }
    }

    private String internalTranslate(final String start, final Bytecode bytecode) {
        final StringBuilder traversalScript = new StringBuilder(start);
        for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
            final String methodName = instruction.getOperator();
            if (0 == instruction.getArguments().length) {
                traversalScript.append(".").append(methodName).append("()");
            } else {
                traversalScript.append(".");
                String temp = methodName + "(";
                for (final Object object : instruction.getArguments()) {
                    temp = temp + convertToString(object) + ",";
                }
                traversalScript.append(temp.substring(0, temp.length() - 1)).append(")");
            }
        }

        return traversalScript.toString();
    }

    /**
     * for each operator argument, try extraction
     *   -  need extraction?     objectType
     *   - （Yes）                Binding
     *   - （Recursion, No）      Bytecode
     *   -  (Recursion, No）      Traversal
     *   - （Yes）                String
     *   - （Recursion, No）      Set
     *   - （Recursion, No）      List
     *   - （Recursion, No）      Map
     *   - （Yes）                Long
     *   - （Yes）                Double
     *   - （Yes）                Float
     *   - （Yes）                Integer
     *   - （Recursion, No）      P
     *   - （Enumeration, No）    SackFunctions.Barrier
     *   - （Enumeration, No）    VertexProperty.Cardinality
     *   - （Enumeration, No）    TraversalOptionParent.Pick
     *   - （Enumeration, No）    Enum
     *   - （Recursion, No）      Vertex
     *   - （Recursion, No）      Edge
     *   - （Recursion, No）      VertexProperty
     *   - （Yes）                Lambda
     *   - （Recursion, No）      TraversalStrategyProxy
     *   - （Enumeration, No）    TraversalStrategy
     *   -  (Yes)                 Other
     * @param object the object to convert to a string representation
     * @return String representation of the object
     */
    private String convertToString(final Object object) {
        if (object instanceof Bytecode.Binding) {
            return Bindings.getBoundVariableOrAssign(((Bytecode.Binding) object).variable());
        } else if (object instanceof Bytecode) {
            return this.internalTranslate("__", (Bytecode) object);
        } else if (object instanceof Traversal) {
            return convertToString(((Traversal) object).asAdmin().getBytecode());
        } else if (object instanceof String) {
            return Bindings.getBoundVariableOrAssign(object);
        } else if (object instanceof Set) {
            final Set<String> set = new HashSet<>(((Set) object).size());
            for (final Object item : (Set) object) {
                set.add(convertToString(item));
            }
            return set.toString() + " as Set";
        } else if (object instanceof List) {
            final List<String> list = new ArrayList<>(((List) object).size());
            for (final Object item : (List) object) {
                list.add(convertToString(item));
            }
            return list.toString();
        } else if (object instanceof Map) {
            final StringBuilder map = new StringBuilder("[");
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                map.append("(").
                        append(convertToString(entry.getKey())).
                        append("):(").
                        append(convertToString(entry.getValue())).
                        append("),");
            }
            map.deleteCharAt(map.length() - 1);
            return map.append("]").toString();
        } else if (object instanceof Long) {
            return Bindings.getBoundVariableOrAssign(object);
        } else if (object instanceof Double) {
            return Bindings.getBoundVariableOrAssign(object);
        } else if (object instanceof Float) {
            return Bindings.getBoundVariableOrAssign(object);
        } else if (object instanceof Integer) {
            return Bindings.getBoundVariableOrAssign(object);
        } else if (object instanceof Class) {
            return ((Class) object).getCanonicalName();
        } else if (object instanceof P) {
            return convertPToString((P) object, new StringBuilder()).toString();
        } else if (object instanceof SackFunctions.Barrier) {
            return "SackFunctions.Barrier." + object.toString();
        } else if (object instanceof VertexProperty.Cardinality) {
            return "VertexProperty.Cardinality." + object.toString();
        } else if (object instanceof TraversalOptionParent.Pick) {
            return "TraversalOptionParent.Pick." + object.toString();
        } else if (object instanceof Enum) {
            return ((Enum) object).getDeclaringClass().getSimpleName() + "." + object.toString();
        } else if (object instanceof Element) {
            if (object instanceof Vertex) {
                final Vertex vertex = (Vertex) object;
                return "new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(" +
                        convertToString(vertex.id()) + "," +
                        convertToString(vertex.label()) + ", Collections.emptyMap())";
            } else if (object instanceof Edge) {
                final Edge edge = (Edge) object;
                return "new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge(" +
                        convertToString(edge.id()) + "," +
                        convertToString(edge.label()) + "," +
                        "Collections.emptyMap()," +
                        convertToString(edge.outVertex().id()) + "," +
                        convertToString(edge.outVertex().label()) + "," +
                        convertToString(edge.inVertex().id()) + "," +
                        convertToString(edge.inVertex().label()) + ")";
            } else {// VertexProperty
                final VertexProperty vertexProperty = (VertexProperty) object;
                return "new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty(" +
                        convertToString(vertexProperty.id()) + "," +
                        convertToString(vertexProperty.label()) + "," +
                        convertToString(vertexProperty.value()) + "," +
                        "Collections.emptyMap()," +
                        convertToString(vertexProperty.element()) + ")";
            }
        } else if (object instanceof Lambda) {
            final String lambdaString = ((Lambda) object).getLambdaScript().trim();
            String value = lambdaString.startsWith("{") ? lambdaString : "{" + lambdaString + "}";
            return Bindings.getBoundVariableOrAssign(value);
        } else if (object instanceof TraversalStrategyProxy) {
            final TraversalStrategyProxy proxy = (TraversalStrategyProxy) object;
            if (proxy.getConfiguration().isEmpty()) {
                return proxy.getStrategyClass().getCanonicalName() + ".instance()";
            } else {
                return proxy.getStrategyClass().getCanonicalName() + ".create(new org.apache.commons.configuration.MapConfiguration(" + convertToString(ConfigurationConverter.getMap(proxy.getConfiguration())) + "))";
            }
        } else if (object instanceof TraversalStrategy) {
            return convertToString(new TraversalStrategyProxy(((TraversalStrategy) object)));
        } else {
            return null == object ? "null" : Bindings.getBoundVariableOrAssign(object.toString());
        }
    }

    private StringBuilder convertPToString(final P p, final StringBuilder current) {
        if (p instanceof ConnectiveP) {
            final List<P<?>> list = ((ConnectiveP) p).getPredicates();
            for (int i = 0; i < list.size(); i++) {
                convertPToString(list.get(i), current);
                if (i < list.size() - 1) {
                    current.append(p instanceof OrP ? ".or(" : ".and(");
                }
            }
            current.append(")");
        } else if (p instanceof  TextP) {
            current.append("TextP.").append(p.getBiPredicate().toString()).append("(").append(convertToString(p.getValue())).append(")");
        } else {
            current.append("P.").append(p.getBiPredicate().toString()).append("(").append(convertToString(p.getValue())).append(")");
        }
        return current;
    }
}
