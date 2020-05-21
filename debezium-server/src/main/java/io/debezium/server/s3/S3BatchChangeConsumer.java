/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.Json;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.s3.batchwriter.BatchRecordWriter;
import io.debezium.server.s3.batchwriter.JsonBatchRecordWriter;
import io.debezium.server.s3.objectkeymapper.BatchObjectKeyMapper;
import io.debezium.server.s3.objectkeymapper.DefaultBatchObjectKeyMapper;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Jiri Pechanec
 */
@Named("s3")
@Dependent
public class S3BatchChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.s3batch.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_BUCKET_NAME = PROP_PREFIX + "bucket.name";
    @ConfigProperty(name = PROP_PREFIX + "credentials.profile", defaultValue = "default")
    String credentialsProfile;
    @ConfigProperty(name = "debezium.sink.s3batch.endpointoverride", defaultValue = "")
    String endpointOverride;
    @Inject
    @CustomConsumerBuilder
    Instance<S3Client> customClient;
    @Inject
    Instance<BatchObjectKeyMapper> customObjectKeyMapper;
    @ConfigProperty(name = PROP_REGION_NAME)
    private String region;
    @ConfigProperty(name = PROP_BUCKET_NAME)
    private String bucket;
    private S3Client client = null;
    private BatchObjectKeyMapper objectKeyMapper = new DefaultBatchObjectKeyMapper();
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    @PostConstruct
    void connect() throws URISyntaxException {
        if (customObjectKeyMapper.isResolvable()) {
            objectKeyMapper = customObjectKeyMapper.get();
        }
        LOGGER.info("Using '{}' stream name mapper", objectKeyMapper);
        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured S3Client '{}'", client);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        S3ClientBuilder clientBuilder = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(ProfileCredentialsProvider.create(credentialsProfile));
        // used for testing with minio
        if (!StringUtils.isEmpty(endpointOverride)) {
            clientBuilder.endpointOverride(new URI(endpointOverride));
        }
        client = clientBuilder.build();
        LOGGER.info("Using default S3Client '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing S3 client: ", e);
        }
    }

    private byte[] getByte(Object object) {
        if (object instanceof byte[]) {
            return (byte[]) object;
        }
        else if (object instanceof String) {
            return ((String) object).getBytes();
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    private String getString(Object object) {
        if (object instanceof String) {
            return (String) object;
        }
        throw new DebeziumException(unsupportedTypeMessage(object));
    }

    public String unsupportedTypeMessage(Object object) {
        final String type = (object == null) ? "null" : object.getClass().getName();
        return "Unexpected data type '" + type + "'";
    }

    private void print(ChangeEvent<Object, Object> record) {
        LOGGER.error("destination=>" + record.destination());
        LOGGER.error("hashCode=>" + record.hashCode());
        LOGGER.error("key=>" + record.key().toString());
        LOGGER.error("value=>" + record.value().toString());
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        try {
            LOGGER.error("STARTING");
            LocalDateTime batchTime = LocalDateTime.now();
            BatchRecordWriter batchWriter = new JsonBatchRecordWriter();
            for (ChangeEvent<Object, Object> record : records) {
                // print(record);
                batchWriter.append(objectKeyMapper.map(record.destination(), batchTime), record);
                committer.markProcessed(record);
            }
            batchWriter.upload(client, bucket);
            committer.markBatchFinished();
        }
        catch (Exception e) {
            throw new InterruptedException(e.getMessage());
        }
    }
}
