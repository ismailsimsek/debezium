package io.debezium.server.batch;

import java.time.Duration;
import javax.inject.Inject;
import io.debezium.server.DebeziumServer;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
public class IcebergEventsIT extends SparkTestBase {
    @ConfigProperty(name = "debezium.sink.type")
    String sinkType;
    @Inject
    DebeziumServer server;

    @Test
    public void testIcebergEvents() throws Exception {
        Testing.Print.enable();
        Assertions.assertEquals(sinkType,"icebergevents");

        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            try {
                Dataset<Row> ds = spark.read().format("iceberg")
                        .load("s3a://test-bucket/iceberg_warehouse/debezium_events");
                ds.show();
                return ds.count() >= 1;
            }
            catch (Exception e) {
                return false;
            }
        });
        TestS3Minio.listFiles();
    }
}
