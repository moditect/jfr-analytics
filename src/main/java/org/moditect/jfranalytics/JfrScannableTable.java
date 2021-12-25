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

import java.nio.file.Path;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import jdk.jfr.EventType;

public class JfrScannableTable extends AbstractTable implements ScannableTable {

    private final Path jfrFile;
    private final EventType eventType;
    private final RelDataType rowType;
    private final AttributeValueConverter[] converters;

    public JfrScannableTable(Path jfrFile, EventType eventType, RelDataType rowType, AttributeValueConverter[] converters) {
        this.jfrFile = jfrFile;
        this.eventType = eventType;
        this.rowType = rowType;
        this.converters = converters;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType;
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return new JfrEnumerable(jfrFile, eventType, converters);
    }
}
