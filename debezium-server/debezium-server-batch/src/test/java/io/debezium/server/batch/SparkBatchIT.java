/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.batch;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.server.DebeziumServer;
import io.debezium.server.TestDatabase;
import io.debezium.server.batch.batchwriter.spark.SparkIcebergBatchRecordWriter;
import io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
public class SparkBatchIT {

    private static final int MESSAGE_COUNT = 2;
    protected static S3Client s3client = null;
    static ProfileCredentialsProvider pcred = ProfileCredentialsProvider.create("default");
    protected static final S3MinioServer s3server = new S3MinioServer();

    protected static TestDatabase db = null;

    {
        // Testing.Debug.enable();
        Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() {
        if (db != null) {
            db.stop();
        }
        if (s3server != null) {
            s3server.stop();
        }
    }

    @Inject
    DebeziumServer server;
    @ConfigProperty(name = "debezium.sink.type")
    String sinkType;

    void setupDependencies(@Observes ConnectorStartedEvent event) throws URISyntaxException {
        if (!sinkType.equals("sparkbatch")) {
            return;
        }
        db = new TestDatabase();
        db.start();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @BeforeAll
    static void setUpS3()
            throws IOException, InvalidKeyException, NoSuchAlgorithmException, XmlParserException, InsufficientDataException, ServerException,
            InternalException, InvalidResponseException, ErrorResponseException, KeyManagementException {
        s3server.start();
    }

    @Test
    public void testSparkBatch() throws Exception {
        Testing.Print.enable();
        Assertions.assertThat(sinkType.equals("sparkbatch"));

        SparkIcebergBatchRecordWriter bw = new SparkIcebergBatchRecordWriter(new LakeTableObjectKeyMapper());

        ObjectMapper objectMapper = new ObjectMapper();
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.close();

        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            List<Item> objects = s3server.getObjectList(ConfigSource.S3_BUCKET);
            Testing.printError(objects);
            // we expect to see 2 batch files {0,1}
            for (Item o : objects) {
                if (o.toString().contains("table123") && o.toString().contains("-1.")) {
                    Testing.print(objects.toString());
                    return true;
                }
            }
            return false;
        });

        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            List<Item> objects = s3server.getObjectList(ConfigSource.S3_BUCKET);
            Testing.printError("Found " + objects.size() + " Objects. Expecting " + MESSAGE_COUNT);
            return objects.size() >= MESSAGE_COUNT;
        });

    }
}
