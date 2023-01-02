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

import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;

public class FrameHelper {

    public static String asText(RecordedFrame frame) {
        if (!frame.isJavaFrame() || frame.getMethod().isHidden()) {
            return null;
        }

        StringBuilder builder = new StringBuilder();

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

        return builder.toString();
    }

    /**
     * Appends the parameter types to the given builder.
     *
     *  @see https://docs.oracle.com/javase/specs/jvms/se17/html/jvms-4.html#jvms-4.3.3
     */
    private static void appendParameters(String methodDescriptor, StringBuilder builder) {
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
