/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.server.DebeziumServer;
import io.debezium.server.TestDatabase;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.server.s3.batchwriter.JsonBatchRecordWriter;
import io.debezium.server.s3.keymapper.TimeBasedDailyObjectKeyMapper;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
public class S3BatchIT {

    private static final int MESSAGE_COUNT = 2;
    static ProfileCredentialsProvider pcred = ProfileCredentialsProvider.create("default");
    protected static final TestS3MinioServer s3server = new TestS3MinioServer();

    protected static TestDatabase db = null;

    {
        // Testing.Debug.enable();
        Testing.Files.delete(S3TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(S3TestConfigSource.OFFSET_STORE_PATH);
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
        if (!sinkType.equals("s3batch")) {
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
    public void testS3Batch() throws Exception {
        Testing.Print.enable();
        Assertions.assertThat(sinkType.equals("s3batch"));

        Awaitility.await().atMost(Duration.ofSeconds(S3TestConfigSource.waitForSeconds()))
                .until(() -> s3server.getObjectList(S3TestConfigSource.S3_BUCKET).toString().contains(S3TestConfigSource.S3_BUCKET));

        Awaitility.await().atMost(Duration.ofSeconds(S3TestConfigSource.waitForSeconds())).until(() -> {
            return s3server.getObjectList(S3TestConfigSource.S3_BUCKET).size() >= MESSAGE_COUNT;
        });

        JsonBatchRecordWriter bw = new JsonBatchRecordWriter(new TimeBasedDailyObjectKeyMapper());

        ObjectMapper objectMapper = new ObjectMapper();
        bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.close();

        Awaitility.await().atMost(Duration.ofSeconds(S3TestConfigSource.waitForSeconds())).until(() -> {
            List<Item> objects = s3server.getObjectList(S3TestConfigSource.S3_BUCKET);
            // we expect to see 2 batch files {0,1}
            for (Item o : objects) {
                if (o.toString().contains("table1") && o.toString().contains("-1.json")) {
                    Testing.print(objects.toString());
                    return true;
                }
            }
            return false;
        });
    }
}
