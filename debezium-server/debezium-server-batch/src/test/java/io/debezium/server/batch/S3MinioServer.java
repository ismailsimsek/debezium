/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.batch;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public class S3MinioServer {

    protected static final Logger LOGGER = LoggerFactory.getLogger(S3MinioServer.class);
    static final int MINIO_DEFAULT_PORT = 9000;
    static final int MINIO_DEFAULT_PORT_MAP = 9000;
    static final String DEFAULT_IMAGE = "minio/minio:latest";
    static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    static final String HEALTH_ENDPOINT = "/minio/health/ready";
    static final String MINIO_ACCESS_KEY;
    static final String MINIO_SECRET_KEY;

    static {
        DefaultCredentialsProvider pcred = DefaultCredentialsProvider.create();
        MINIO_ACCESS_KEY = pcred.resolveCredentials().accessKeyId();
        MINIO_SECRET_KEY = pcred.resolveCredentials().secretAccessKey();
    }

    public MinioClient client;
    private GenericContainer container = null;

    public void start() throws NoSuchAlgorithmException, KeyManagementException, ServerException, InsufficientDataException, InternalException, InvalidResponseException,
            InvalidKeyException, XmlParserException, ErrorResponseException, IOException {

        this.container = new FixedHostPortGenericContainer(DEFAULT_IMAGE)
                .withFixedExposedPort(MINIO_DEFAULT_PORT_MAP, MINIO_DEFAULT_PORT)
                .waitingFor(new HttpWaitStrategy()
                        .forPath(HEALTH_ENDPOINT)
                        .forPort(MINIO_DEFAULT_PORT)
                        .withStartupTimeout(Duration.ofSeconds(30)))
                .withEnv("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                .withEnv("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                .withEnv("MINIO_REGION_NAME", ConfigSource.S3_REGION)
                .withCommand("server " + DEFAULT_STORAGE_DIRECTORY);
        this.container.start();

        client = MinioClient.builder()
                .endpoint("http://" + container.getHost() + ":" + container.getMappedPort(MINIO_DEFAULT_PORT))
                .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                .build();
        client.ignoreCertCheck();
        client.makeBucket(MakeBucketArgs.builder()
                .region(ConfigSource.S3_REGION)
                .bucket(ConfigSource.S3_BUCKET)
                .build());

        LOGGER.info("Minio Started!");
    }

    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public String getContainerIpAddress() {
        return this.container.getContainerIpAddress();
    }

    public Integer getMappedPort() {
        return this.container.getMappedPort(MINIO_DEFAULT_PORT);
    }

    public Integer getFirstMappedPort() {
        return this.container.getFirstMappedPort();
    }

    public void listBuckets()
            throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException,
            XmlParserException, ErrorResponseException {
        List<Bucket> bucketList = client.listBuckets();
        for (Bucket bucket : bucketList) {
            LOGGER.info("Bucket: {} {}", bucket.name(), bucket.creationDate());
        }
    }

    public void listFiles() {
        listFiles(null);
    }

    public void listFiles(String message) {
        LOGGER.info("-----------------------------------------------------------------");
        if (message != null) {
            LOGGER.info("{}", message);
        }
        try {
            List<Bucket> bucketList = client.listBuckets();
            for (Bucket bucket : bucketList) {
                LOGGER.info("Bucket:{} ROOT", bucket.name());
                Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder().bucket(bucket.name()).recursive(true).build());
                for (Result<Item> result : results) {
                    Item item = result.get();
                    LOGGER.info("Bucket:{} Item:{} Size:{}", bucket.name(), item.objectName(), item.size());
                }
            }
        }
        catch (Exception e) {
            LOGGER.info("Failed listing bucket");
        }
        LOGGER.info("-----------------------------------------------------------------");

    }

    public List<Item> getObjectList(String bucketName) {
        List<Item> objects = new ArrayList<Item>();

        try {
            Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder().bucket(bucketName).recursive(true).build());
            for (Result<Item> result : results) {
                Item item = result.get();
                objects.add(item);
            }
        }
        catch (Exception e) {
            LOGGER.info("Failed listing bucket");
        }
        return objects;
    }

}
