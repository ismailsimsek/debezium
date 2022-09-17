/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

/**
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 *
 */

public class SignalRecord {
    public final String id;
    public final String type;
    public final String data;
    public final long signalOffset;

    public SignalRecord(String id, String type, String data) {
        this.id = id;
        this.type = type;
        this.data = data;
        this.signalOffset = 0L;
    }

    public SignalRecord(String id, String type, String data, long signalOffset) {
        this.id = id;
        this.type = type;
        this.data = data;
        this.signalOffset = signalOffset;
    }
}
