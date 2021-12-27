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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

public class JfrSchemaFactory implements SchemaFactory {

    public static final String INLINE_MODEL = """
            inline: {
              version: '1.0',
              defaultSchema: 'jfr',
              schemas: [
                {
                  name: 'jfr',
                  type: 'custom',
                  factory: 'org.moditect.jfranalytics.JfrSchemaFactory',
                  operand: {
                    file: '%s'
                  }
                }
              ]
            }
            """;

    public static String getInlineModel(Path jfrFile) {
        return INLINE_MODEL.formatted(jfrFile
            .toAbsolutePath()
            .toString()
            .replace("\\", "\\\\"));
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String file = (String) operand.get("file");
        if (file == null) {
            throw new IllegalArgumentException("Please specify a JFR file name via the 'file' operand");
        }

        Path jfrFile = new File(file).toPath().toAbsolutePath();
        if (!Files.exists(jfrFile)) {
            throw new IllegalArgumentException("Given JFR file doesn't exist: " + jfrFile);
        }

        return new JfrSchema(jfrFile);
    }

}
