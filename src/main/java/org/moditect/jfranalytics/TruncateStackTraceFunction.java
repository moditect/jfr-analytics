/*
 *  Copyright 2021 - 2023 The original authors
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
            builder.append(FrameHelper.asText(frames.get(i)));
            builder.append(System.lineSeparator());
            i++;
        }

        return builder.toString();
    }
}
