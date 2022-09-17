/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import java.util.List;
import java.util.Optional;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.ExecuteSnapshot;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class ExecuteSnapshotKafkaSignal<P extends Partition> extends ExecuteSnapshot<P> {
    private final List<String> dataCollections;
    private final long signalOffset;
    private final Optional<String> additionalCondition;

    public ExecuteSnapshotKafkaSignal(EventDispatcher<P,? extends DataCollectionId> dispatcher, List<String> dataCollections, long signalOffset, Optional<String> additionalCondition) {
        super(dispatcher);
        this.dataCollections = dataCollections;
        this.signalOffset = signalOffset;
        this.additionalCondition = additionalCondition;
    }

    public List<String> getDataCollections() {
        return dataCollections;
    }

    @Override
    public long getSignalOffset() {
        return signalOffset;
    }

    public Optional<String> getAdditionalCondition() {
        return additionalCondition;
    }

}
