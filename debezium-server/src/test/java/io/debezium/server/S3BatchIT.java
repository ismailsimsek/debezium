/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;
import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.time.Duration;
import java.util.List;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to Kinesis stream.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
public class S3BatchIT {

    private static final int MESSAGE_COUNT = 2;
    protected static TestS3 s3server = null;
    protected static S3Client s3client = null;
    @Inject
    DebeziumServer server;
    @AfterAll
    static void stop() {
        if (s3server != null) {
            s3server.stop();
        }
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
            if (objects.size() >= MESSAGE_COUNT){
                Testing.print(objects.toString());
            }
            return objects.size() >= MESSAGE_COUNT;
        });
    }
}
