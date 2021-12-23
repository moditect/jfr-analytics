/*
 *  Copyright 2021 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.moditect.jfranalytics;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.EventStream;

public class JfrSchema implements Schema {

    private final Map<String, JfrScannableTable> tableTypes;

    public JfrSchema(Path jfrFile) {
        this.tableTypes = Collections.unmodifiableMap(getTableTypes(jfrFile));
    }

    private static Map<String, JfrScannableTable> getTableTypes(Path jfrFile) {

        try (var es = EventStream.openFile(jfrFile)) {
            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
            Map<String, JfrScannableTable> tableTypes = new HashMap<>();

            es.onEvent(event -> {
                if (!tableTypes.containsKey(event.getEventType().getName())) {
                    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

                    for (ValueDescriptor field : event.getEventType().getFields()) {
                        RelDataType type;

                        switch (field.getTypeName()) {
                            case "long":
                                type = typeFactory.createJavaType(long.class);
                                break;
                            case "java.lang.String":
                                type = typeFactory.createJavaType(String.class);
                                break;
                            default:
                                type = null;
                        }

                        if (type != null) {
                            builder.add(field.getName(), type.getSqlTypeName()).nullable(true);
                        }
                    }

                    tableTypes.put(event.getEventType().getName(), new JfrScannableTable(builder.build()));
                }
            });

            es.start();

            return tableTypes;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public @Nullable Table getTable(String name) {
        return tableTypes.get(name);
    }

    @Override
    public Set<String> getTableNames() {
        return tableTypes.keySet();
    }

    @Override
    public @Nullable RelProtoDataType getType(String name) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Set<String> getTypeNames() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getFunctionNames() {
        return Collections.emptySet();
    }

    @Override
    public @Nullable Schema getSubSchema(String name) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return Collections.emptySet();
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

}
