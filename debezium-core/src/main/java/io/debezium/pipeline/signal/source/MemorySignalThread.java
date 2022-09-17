/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.source;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The class responsible for processing of signals delivered to Debezium via a JVM memory. 
 * The signal message must have the following structure:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 */
public class MemorySignalThread<P extends Partition, T extends DataCollectionId> extends AbstractSignalThread<P, T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemorySignalThread.class);

    public MemorySignalThread(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                              AbstractIncrementalSnapshotChangeEventSource<P, T> incrementalSnapshotChangeEventSource) {
        super(connectorType, connectorConfig, incrementalSnapshotChangeEventSource, "memory-signal");
        LOGGER.info("Reading signals from jvm memory");
    }

    protected void monitorSignals() {
    }

    public static void addMemorySignal(SignalRecord signal) {
        SIGNALS.add(signal);
    }
}
