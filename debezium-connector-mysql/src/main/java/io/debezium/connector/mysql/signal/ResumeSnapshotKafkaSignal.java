/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.ResumeIncrementalSnapshot;
import io.debezium.pipeline.spi.Partition;

public class ResumeSnapshotKafkaSignal<P extends Partition> extends ResumeIncrementalSnapshot<P> {
    public ResumeSnapshotKafkaSignal(EventDispatcher dispatcher) {
        super(dispatcher);
    }
}
