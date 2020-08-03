/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.net.URISyntaxException;

public class JsonBatchRecordWriter extends AbstractBatchRecordWriter {

    private final S3Client s3Client;

    public JsonBatchRecordWriter(ObjectKeyMapper mapper, AwsCredentialsProvider credProvider, String bucket)
            throws URISyntaxException {
        super(mapper, credProvider, bucket);
        S3ClientBuilder clientBuilder = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(this.credProvider);
        // used for testing, using minio
        if (!endpointOverride.trim().toLowerCase().equals("false")) {
            clientBuilder.endpointOverride(new URI(endpointOverride));
        }
        this.s3Client = clientBuilder.build();
        LOGGER.info("Using default S3Client '{}'", this.s3Client);
        LOGGER.info("Starting S3 Batch Consumer({})", this.getClass().getName());
    }

    protected void uploadBatchFile(String destination) {
        Integer batchId = map_batchid.get(destination);
        final String data = map_data.get(destination);
        String s3File = objectKeyMapper.map(destination, batchTime, batchId);
        LOGGER.debug("Uploading s3File destination:{} key:{}", destination, s3File);
        final PutObjectRequest putRecord = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3File)
                .tagging(tags)
                .build();
        s3Client.putObject(putRecord, RequestBody.fromString(data));
        // increment batch id
        map_batchid.put(destination, batchId + 1);
        // start new batch
        map_data.remove(destination);
        cdcDb.commit();
        LOGGER.debug("Upload Succeeded! destination:{} key:{}", destination, s3File);
    }

}
