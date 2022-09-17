/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.source;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Threads;

public abstract class AbstractSignalThread<P extends Partition, T extends DataCollectionId> implements SignalThread {
    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    protected ExecutorService signalListenerExecutor;
    protected final AbstractIncrementalSnapshotChangeEventSource<P, T> incrementalSnapshotChangeEventSource;
    public static final Queue<SignalRecord> SIGNALS = new ConcurrentLinkedQueue<>();

    public AbstractSignalThread(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                                AbstractIncrementalSnapshotChangeEventSource<P, T> incrementalSnapshotChangeEventSource, String signalName) {
        String connectorName = connectorConfig.getLogicalName();
        this.incrementalSnapshotChangeEventSource = incrementalSnapshotChangeEventSource;
        signalListenerExecutor = Threads.newSingleThreadExecutor(connectorType, connectorName, signalName, true);
    }

    public void start() {
        signalListenerExecutor.submit(this::monitorSignals);
    }

    protected abstract void monitorSignals();

    public SignalRecord poll() {
        return SIGNALS.poll();
    }

    protected void add(SignalRecord signal) {
        SIGNALS.add(signal);
    }

    public boolean hasSignal() {
        return !SIGNALS.isEmpty();
    }

    // for the moment only relevant for kafka signals
    public void seek(long signalOffset) {
        return;
    }

}
