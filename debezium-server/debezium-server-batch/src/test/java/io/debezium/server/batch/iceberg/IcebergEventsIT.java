/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.batch.iceberg;

import java.time.Duration;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.server.DebeziumServer;
import io.debezium.server.batch.ConfigSource;
import io.debezium.server.batch.TestDatabase;
import io.debezium.server.batch.TestS3Minio;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
public class IcebergEventsIT {
    @ConfigProperty(name = "debezium.sink.type")
    String sinkType;
    @Inject
    DebeziumServer server;

    @Inject
    @ConfigProperty(name = "quarkus.minio.port")
    String minioPort;

    {
        // Testing.Debug.enable();
        Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testIcebergEvents() throws Exception {
        Testing.printError("====> Minio : " + minioPort);
        Testing.printError("====> System : " + System.getProperty("quarkus.minio.port"));
        Testing.Print.enable();
        Assertions.assertThat(sinkType.equals("icebergevents"));

        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            // Testing.printError(s3server.getObjectList(ConfigSource.S3_BUCKET));
            // s3server.listFiles();
            return TestS3Minio.getObjectList(ConfigSource.S3_BUCKET).size() >= 9;
        });
        TestS3Minio.listFiles();
    }
}
