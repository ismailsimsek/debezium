/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.source;

import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The class responsible for processing of signals delivered to Debezium via a file.
 * The signal message must have the following structure:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 */
public class FileSignalThread<P extends Partition, T extends DataCollectionId> extends AbstractSignalThread<P, T> {
    public static final Field SIGNAL_FILE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "file")
            .withDisplayName("Signal file name").withType(ConfigDef.Type.STRING).withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the file for the signals to the connector").withValidation(Field::isRequired);

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSignalThread.class);
    private final Path signalFilePath;

    public FileSignalThread(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                            AbstractIncrementalSnapshotChangeEventSource<P, T> incrementalSnapshotChangeEventSource) {

        super(connectorType, connectorConfig, incrementalSnapshotChangeEventSource, "file-signal");
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false).edit()
                .build();
        this.signalFilePath = Paths.get(signalConfig.getString(SIGNAL_FILE));
        LOGGER.info("Reading '{}' file for signals", signalFilePath.toAbsolutePath());
    }

    protected void monitorSignals() {
        while (true) {
            try {
                // processing all the signals.
                List<String> lines = Files.readAllLines(signalFilePath, StandardCharsets.UTF_8);
                // remove signals from file!
                if (!lines.isEmpty()) {
                    new FileWriter(signalFilePath.toFile(), false).close();
                }
                Iterator<String> lineIterator = lines.iterator();
                while (lineIterator.hasNext()) {
                    String signalLine = lineIterator.next();
                    if (signalLine == null || signalLine.isEmpty()) {
                        lineIterator.remove();
                        continue;
                    }
                    try {
                        LOGGER.trace("Processing signal line: {}", signalLine);
                        final Document signal = DocumentReader.defaultReader().read(signalLine);
                        String id = signal.getString("id");
                        String type = signal.getString("type");
                        String data = signal.getString("data");
                        this.add(new SignalRecord(id, type, data));
                        LOGGER.info("Processing signal id {}, {}, {}", id, type, data);
                    }
                    catch (final Exception e) {
                        LOGGER.error("Skipped signal due to an error '{}'", signalLine, e);
                    }
                    lineIterator.remove();
                }
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to read signal file " + signalFilePath, e);
            }
        }
    }

}
