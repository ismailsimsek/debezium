/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import java.util.List;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.StopSnapshot;
import io.debezium.pipeline.spi.Partition;

/**
 * A {@link ExternalSignal} implementation to stop a current executing snapshot.
 *
 * @author Chris Cranford
 */
public class StopSnapshotKafkaSignal<P extends Partition> extends StopSnapshot<P> {
    private final List<String> dataCollections;
    private final long signalOffset;

    public StopSnapshotKafkaSignal(EventDispatcher dispatcher, List<String> dataCollections, long signalOffset) {
        super(dispatcher);
        this.dataCollections = dataCollections;
        this.signalOffset = signalOffset;
    }

    public List<String> getDataCollections() {
        return dataCollections;
    }

    public long getSignalOffset() {
        return signalOffset;
    }
}
