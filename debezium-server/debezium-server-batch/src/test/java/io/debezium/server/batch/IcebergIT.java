/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.batch;

import java.net.URISyntaxException;
import java.time.Duration;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import io.debezium.server.DebeziumServer;
import static io.debezium.server.batch.ConfigSource.S3_BUCKET;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
public class IcebergIT extends SparkTestBase {

    @Inject
    DebeziumServer server;
    @ConfigProperty(name = "debezium.sink.type")
    String sinkType;

    {
        // Testing.Debug.enable();
        Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws URISyntaxException {
        if (!sinkType.equals("iceberg")) {
            return;
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testIcebergConsumer() throws Exception {
        //Testing.Print.enable();
        Assertions.assertThat(sinkType.equals("iceberg"));
        //TestS3Minio.listFiles();
        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            try {
                Dataset<Row> ds = spark.read().format("iceberg")
                        .load("s3a://test-bucket/iceberg_warehouse/testc.inventory.customers");
                ds.show();
                return ds.count() >= 4;
            }
            catch (Exception e) {
                return false;
            }
        });
        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            return TestS3Minio.getIcebergDataFiles(S3_BUCKET).size() >= 2;
        });
        TestS3Minio.listFiles();
    }
}
