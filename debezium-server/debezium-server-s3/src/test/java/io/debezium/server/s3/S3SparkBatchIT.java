/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.net.URISyntaxException;
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
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.server.s3.batchwriter.SparkBatchRecordWriter;
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
public class S3SparkBatchIT {

    private static final int MESSAGE_COUNT = 2;
    protected static S3Client s3client = null;
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
        if (!sinkType.equals("s3sparkbatch")) {
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

    private List<S3Object> getObjectList() {
        ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket(S3TestConfigSource.S3_BUCKET)
                .build();
        ListObjectsResponse res = s3client.listObjects(listObjects);
        return res.contents();
    }

    @BeforeAll
    static void setUpS3() throws URISyntaxException {
        s3server.start();
        s3client = S3Client.builder()
                .region(Region.of(S3TestConfigSource.S3_REGION))
                .credentialsProvider(pcred)
                .endpointOverride(new java.net.URI("http://localhost:" + TestS3MinioServer.MINIO_DEFAULT_PORT_MAP))
                .build();
        s3client.createBucket(CreateBucketRequest.builder().bucket(S3TestConfigSource.S3_BUCKET).build());
        Assertions.assertThat(s3client.listBuckets().toString().contains(S3TestConfigSource.S3_BUCKET));
    }

    @Test
    public void testS3SparkBatch() throws Exception {
        Testing.Print.enable();
        Assertions.assertThat(sinkType.equals("s3sparkbatch"));

        Awaitility.await().atMost(Duration.ofSeconds(S3TestConfigSource.waitForSeconds()))
                .until(() -> s3client.listBuckets().toString().contains(S3TestConfigSource.S3_BUCKET));

        Awaitility.await().atMost(Duration.ofSeconds(S3TestConfigSource.waitForSeconds())).until(() -> {
            List<S3Object> objects = getObjectList();
            Testing.printError("Found " + objects.size() + " Objects. Expecting " + MESSAGE_COUNT);
            return objects.size() >= MESSAGE_COUNT;
        });

        SparkBatchRecordWriter bw = new SparkBatchRecordWriter(new TimeBasedDailyObjectKeyMapper());

        ObjectMapper objectMapper = new ObjectMapper();
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.append("table123", objectMapper.readTree("{\"row1\": \"data\"}"));
        bw.close();

        Awaitility.await().atMost(Duration.ofSeconds(S3TestConfigSource.waitForSeconds())).until(() -> {
            List<S3Object> objects = getObjectList();
            Testing.printError(objects);
            // we expect to see 2 batch files {0,1}
            for (S3Object o : objects) {
                if (o.key().contains("table123") && o.key().contains("-1.")) {
                    Testing.print(objects.toString());
                    return true;
                }
            }
            return false;
        });
    }
}
