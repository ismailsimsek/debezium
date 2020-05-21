/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.ListIterator;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to Kinesis stream.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
public class S3IT {

    private static final int MESSAGE_COUNT = 4;
    protected static TestDatabase db = null;
    protected static TestS3 s3server = null;
    protected static S3Client s3client = null;
    @Inject
    DebeziumServer server;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
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

    void setupDependencies(@Observes ConnectorStartedEvent event) throws InvalidPortException, InvalidEndpointException, URISyntaxException {
        if (!TestConfigSource.isItTest()) {
            return;
        }
        // db = new TestDatabase();
        // db.start();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testS3() throws Exception {
        Testing.Print.enable();

        s3server = new TestS3();
        s3server.start();
        ProfileCredentialsProvider pcred = ProfileCredentialsProvider.create("default");
        s3client = S3Client.builder()
                .region(Region.of(TestConfigSource.AWS_REGION))
                .credentialsProvider(pcred)
                .endpointOverride(new java.net.URI("http://" + s3server.getContainerIpAddress() + ':' + s3server.getMappedPort()))
                .build();
        s3client.createBucket(CreateBucketRequest.builder().bucket(TestConfigSource.S3_BUCKET).build());
        // s3client.setEndpoint("http://" + s3server.getContainerIpAddress() + ':' + s3server.getMappedPort());
        s3client.createBucket(CreateBucketRequest.builder().bucket("testing-s3-slient").build());
        Assertions.assertThat(s3client.listBuckets().toString().contains("testing-s3-slient"));
        Assertions.assertThat(s3client.listBuckets().toString().contains(TestConfigSource.S3_BUCKET));

        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(TestConfigSource.S3_BUCKET)
                    .build();
            ListObjectsResponse res = s3client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            String multipartKey = "multiPartKey";
            for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext();) {
                S3Object myValue = (S3Object) iterVals.next();
                Testing.print(Paths.get("multiPartKey").toAbsolutePath());
                Files.deleteIfExists(Paths.get("multiPartKey"));
                s3client.getObject(GetObjectRequest.builder().bucket(TestConfigSource.S3_BUCKET).key(myValue.key()).build(),
                        ResponseTransformer.toFile(Paths.get("multiPartKey")));
            }
            return objects.size() >= MESSAGE_COUNT;
        });
    }
}
