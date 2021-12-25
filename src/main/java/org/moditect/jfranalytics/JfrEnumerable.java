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
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;

import jdk.jfr.EventType;
import jdk.jfr.consumer.EventStream;

public class JfrEnumerable extends AbstractEnumerable<Object[]> {

    private final Path jfrFile;
    private final EventType eventType;
    private final RelDataType rowType;

    public JfrEnumerable(Path jfrFile, EventType eventType, RelDataType rowType) {
        this.jfrFile = jfrFile;
        this.eventType = eventType;
        this.rowType = rowType;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        try (var es = EventStream.openFile(jfrFile)) {
            int localOffset = TimeZone.getDefault().getOffset(System.currentTimeMillis());
            List<Object[]> results = new ArrayList<>();

            es.onEvent(eventType.getName(), event -> {
                Object[] row = new Object[rowType.getFieldCount()];

                int i = 0;
                for (String field : rowType.getFieldNames()) {
                    // timestamps are adjusted by Calcite using local TZ offset; account for that
                    if (field.equals("startTime")) {
                        row[i] = event.getStartTime().toEpochMilli() + localOffset;
                    }
                    else if (field.equals("eventThread")) {
                        row[i] = event.getThread().getJavaName();
                    }
                    else if (field.equals("stackTrace")) {
                        row[i] = event.getStackTrace().toString().replaceAll(",     ", System.lineSeparator() + "     ");
                    }
                    else {
                        row[i] = event.getValue(field);
                    }
                    i++;
                }

                results.add(row);
            });
            es.start();

            return Linq4j.enumerator(results);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
