/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.source;

import io.debezium.pipeline.signal.SignalRecord;

public interface SignalThread {
    void start();

    SignalRecord poll();

    boolean hasSignal();

    void seek(long signalOffset);
}
