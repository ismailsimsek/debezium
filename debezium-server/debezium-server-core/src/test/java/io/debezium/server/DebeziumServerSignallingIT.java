/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.source.MemorySignalThread;
import io.debezium.testing.testcontainers.MySqlGtidsTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@QuarkusTestResource(value = MySqlGtidsTestResourceLifecycleManager.class, restrictToAnnotatedClass = true)
@TestProfile(DebeziumServerSignallingIT.TestProfile.class)
public class DebeziumServerSignallingIT {
    private static final int MESSAGE_COUNT = 444;
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumServerSignallingIT.class);

    @Inject
    DebeziumServer server;
    public static final String SIGNAL_FILE = "signal.txt";
    public static final Path SIGNAL_FILE_PATH = Testing.Files.createTestingPath(SIGNAL_FILE).toAbsolutePath();
    private static final String SIGNAL = "{\"id\": \"1\",\"type\": \"execute-snapshot\",\"data\": \"{\\\"data-collections\\\": [\\\"inventory.customers\\\",\\\"inventory.*\\\"]}\"}";

    @BeforeAll
    public static void setup() throws IOException {
        Testing.Files.delete(SIGNAL_FILE_PATH);
        System.out.println("Creating File: " + SIGNAL_FILE_PATH.toAbsolutePath());
        LOGGER.error("Creating File: " + SIGNAL_FILE_PATH.toAbsolutePath());
        FileUtils.writeStringToFile(SIGNAL_FILE_PATH.toFile(), SIGNAL, Charset.defaultCharset());
    }

    @Test
    public void testSimpleUpload() {
        (new Thread(() -> {
            try {
                Thread.sleep(30000);
            }
            catch (InterruptedException e) {
                throw new DebeziumException(e);
            }
            SignalRecord s = new SignalRecord("1111", "execute-snapshot", "{\"data-collections\": [\"inventory.customers\"]}");
            LOGGER.error("Adding memory signal 1111");
            MemorySignalThread.addMemorySignal(s);
            try {
                Thread.sleep(30000);
            }
            catch (InterruptedException e) {
                throw new DebeziumException(e);
            }
            LOGGER.error("Adding memory signal 2222");
            SignalRecord s2 = new SignalRecord("2222", "execute-snapshot", "{\"data-collections\": [\"inventory.products\"]}");
            MemorySignalThread.addMemorySignal(s2);
        })).start();

        Testing.Print.disable();
        Awaitility.await().atMost(Duration.ofSeconds(6600)).until(() -> {
            final TestConsumer testConsumer = (TestConsumer) server.getConsumer();
            try {
                return testConsumer.getValues().size() >= MESSAGE_COUNT;
            }
            catch (Exception e) {
                return false;
            }
        });
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            // @TODOconfig.put("debezium.source.signal.class", "io.debezium.pipeline.signal.source.FileSignalThread");
            config.put("debezium.source.signal.file", SIGNAL_FILE_PATH.toAbsolutePath().toString());
            config.put("debezium.source.read.only", "true");
            config.put("debezium.source.logical.name", "testc");
            config.put("debezium.source.database.server.name", "testc");
            config.put("debezium.source.database.server.id", "1234");
            config.put("debezium.source.database.include.list", "inventory");
            // config.put("debezium.source.schema.include.list", "");
            config.put("debezium.source.table.include.list", "inventory.*");
            config.put("debezium.source.schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
            config.put("debezium.source.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
            config.put("quarkus.log.level", "INFO");
            config.put("quarkus.log.category.\"io.debezium.pipeline.signal\".level", "INFO");
            config.put("quarkus.log.category.\"io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotChangeEventSource\".level", "DEBUG");
            config.put("quarkus.log.category.\"io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext\".level", "DEBUG");
            return config;
        }
    }

}
