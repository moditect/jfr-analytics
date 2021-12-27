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

import java.util.List;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedStackTrace;

/**
 * Truncates a {@link RecordedStackTrace} to the given maximum depth.
 */
public class TruncateStackTraceFunction {

    public static final ScalarFunction INSTANCE = ScalarFunctionImpl.create(TruncateStackTraceFunction.class, "eval");

    public String eval(Object recordedStackTrace, int depth) {
        if (recordedStackTrace == null) {
            return null;
        }
        if (!(recordedStackTrace instanceof RecordedStackTrace)) {
            throw new IllegalArgumentException("Unexpected value type: " + recordedStackTrace);
        }
        if (depth < 1) {
            throw new IllegalArgumentException("At least one frame must be retained");
        }

        List<RecordedFrame> frames = ((RecordedStackTrace) recordedStackTrace).getFrames();
        StringBuilder builder = new StringBuilder();

        int i = 0;
        while (i < depth && i < frames.size()) {
            appendFrame(frames.get(i), builder);
            i++;
        }

        return builder.toString();
    }

    private void appendFrame(RecordedFrame frame, StringBuilder builder) {
        if (!frame.isJavaFrame() || frame.getMethod().isHidden()) {
            return;
        }

        RecordedMethod method = frame.getMethod();
        builder.append(method.getType().getName());
        builder.append('.');
        builder.append(method.getName());

        builder.append('(');
        appendParameters(method.getDescriptor(), builder);
        builder.append(')');

        int line = frame.getLineNumber();
        if (line >= 0) {
            builder.append(':').append(line);
        }

        builder.append(System.lineSeparator());
    }

    /**
     * Appends the parameter types to the given builder.
     *
     *  @see https://docs.oracle.com/javase/specs/jvms/se17/html/jvms-4.html#jvms-4.3.3
     */
    private void appendParameters(String methodDescriptor, StringBuilder builder) {
        boolean beforeFirstParameter = true;

        for (int i = 1; i < methodDescriptor.lastIndexOf(')'); i++) {
            if (beforeFirstParameter) {
                beforeFirstParameter = false;
            }
            else {
                builder.append(", ");
            }

            // put array brackets after the type name
            int arrayDimension = 0;
            while ((methodDescriptor.charAt(i)) == '[') {
                arrayDimension++;
                i++;
            }

            char nextChar = methodDescriptor.charAt(i);
            switch (nextChar) {
                case 'B':
                    builder.append("byte");
                    break;
                case 'C':
                    builder.append("char");
                    break;
                case 'D':
                    builder.append("double");
                    break;
                case 'F':
                    builder.append("float");
                    break;
                case 'I':
                    builder.append("int");
                    break;
                case 'J':
                    builder.append("long");
                    break;
                case 'L':
                    int typeNameStartIndex = builder.length();
                    int lastDotIndex = -1;
                    i++;

                    // consume type name
                    while ((nextChar = methodDescriptor.charAt(i)) != ';') {
                        if (nextChar == '/') {
                            builder.append('.');
                            lastDotIndex = builder.length();
                        }
                        else {
                            builder.append(nextChar);
                        }
                        i++;
                    }

                    // only keep unqualified name
                    if (lastDotIndex > 0) {
                        builder.delete(typeNameStartIndex, lastDotIndex);
                    }
                    break;
                case 'S':
                    builder.append("short");
                    break;
                case 'Z':
                    builder.append("boolean");
                    break;
            }

            for (int y = 0; y < arrayDimension; y++) {
                builder.append("[]");
            }
        }
    }
}
