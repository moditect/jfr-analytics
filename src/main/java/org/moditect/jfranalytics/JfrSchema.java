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
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import jdk.jfr.EventType;
import jdk.jfr.Timespan;
import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.EventStream;

public class JfrSchema implements Schema {

    private static final System.Logger LOGGER = System.getLogger(JfrSchema.class.getName());
    private static final int LOCAL_OFFSET = TimeZone.getDefault().getOffset(System.currentTimeMillis());

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
                    List<AttributeValueConverter> converters = new ArrayList<>();

                    for (ValueDescriptor field : event.getEventType().getFields()) {
                        RelDataType type = getRelDataType(event.getEventType(), field, typeFactory);
                        if (type == null) {
                            continue;
                        }

                        builder.add(field.getName(), type.getSqlTypeName()).nullable(true);
                        converters.add(getConverter(field, type));
                    }

                    tableTypes.put(event.getEventType().getName(),
                            new JfrScannableTable(jfrFile, event.getEventType(), builder.build(), converters.toArray(new AttributeValueConverter[0])));
                }
            });

            es.start();

            return tableTypes;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static RelDataType getRelDataType(EventType eventType, ValueDescriptor field, RelDataTypeFactory typeFactory) {
        RelDataType type;
        switch (field.getTypeName()) {
            case "int":
                type = typeFactory.createJavaType(int.class);
            case "long":
                if ("jdk.jfr.Timestamp".equals(field.getContentType())) {
                    type = typeFactory.createJavaType(Timestamp.class);
                }
                else {
                    type = typeFactory.createJavaType(long.class);
                }
                break;
            case "java.lang.String":
                type = typeFactory.createJavaType(String.class);
                break;
            case "java.lang.Thread":
                type = typeFactory.createJavaType(String.class);
                break;
            case "jdk.types.StackTrace":
                type = typeFactory.createJavaType(String.class);
                break;
            default:
                LOGGER.log(Level.WARNING, "Unknown attribute type: {0}; event type {1}", field.getTypeName(), eventType.getName());
                type = null;
        }
        return type;
    }

    private static AttributeValueConverter getConverter(ValueDescriptor field, RelDataType type) {
        // timestamps are adjusted by Calcite using local TZ offset; account for that
        if (field.getName().equals("startTime")) {
            return event -> event.getStartTime().toEpochMilli() + LOCAL_OFFSET;
        }
        else if (field.getName().equals("duration")) {
            return event -> event.getDuration().toNanos();
        }
        else if (field.getName().equals("eventThread")) {
            return event -> event.getThread().getJavaName();
        }
        else if (field.getName().equals("stackTrace")) {
            return event -> event.getStackTrace().toString().replaceAll(",     ", System.lineSeparator() + "     ");
        }
        else {
            if (field.getAnnotation(Timespan.class) != null) {
                return event -> event.getDuration(field.getName()).toNanos();
            }
            else if (type != null && type.getSqlTypeName() == SqlTypeName.BIGINT) {
                return event -> event.getLong(field.getName());
            }
            else {
                return event -> event.getValue(field.getName());
            }
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
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
