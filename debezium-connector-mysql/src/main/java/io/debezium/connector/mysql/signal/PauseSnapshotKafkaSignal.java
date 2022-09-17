/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.PauseIncrementalSnapshot;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class PauseSnapshotKafkaSignal<P extends Partition> extends PauseIncrementalSnapshot<P> {
    public PauseSnapshotKafkaSignal(EventDispatcher<P,? extends DataCollectionId> dispatcher) {
        super(dispatcher);
    }
}
