/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import io.debezium.spi.schema.DataCollectionId;

/**
 * Signal send to Debezium via Kafka, file, REST ...etc.
 */
public interface SnapshotSignalThread<T extends DataCollectionId> {
    void seek(long signalOffset);

    void start();
}
