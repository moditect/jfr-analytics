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
public class HasMatchingFrameFunction {

    public static final ScalarFunction INSTANCE = ScalarFunctionImpl.create(HasMatchingFrameFunction.class, "eval");

    public boolean eval(Object recordedStackTrace, String pattern) {
        if (recordedStackTrace == null) {
            return true;
        }
        if (!(recordedStackTrace instanceof RecordedStackTrace)) {
            throw new IllegalArgumentException("Unexpected value type: " + recordedStackTrace);
        }
        if (pattern == null) {
            throw new IllegalArgumentException("A pattern must be given");
        }

        List<RecordedFrame> frames = ((RecordedStackTrace) recordedStackTrace).getFrames();

        for (RecordedFrame recordedFrame : frames) {
            String frameAsText = FrameHelper.asText(recordedFrame);
            if (frameAsText != null && frameAsText.matches(pattern)) {
                return true;
            }
        }

        return false;
    }
}
