/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.connect.json.JsonDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("s3batch")
@Dependent
public class S3BatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.s3.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_BUCKET_NAME = PROP_PREFIX + "bucket.name";
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());
    final String credentialsProfile = ConfigProvider.getConfig().getOptionalValue(PROP_PREFIX + "credentials.profile", String.class).orElse("default");
    final Boolean useInstanceProfile = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.credentials.useinstancecred", Boolean.class).orElse(false);

    @Inject
    Instance<ObjectKeyMapper> customObjectKeyMapper;

    @ConfigProperty(name = PROP_BUCKET_NAME, defaultValue = "My-S3-Bucket")
    String bucket;
    S3Client s3client;
    // private final ObjectKeyMapper objectKeyMapper = new TimeBasedDailyObjectKeyMapper();
    BatchRecordWriter batchWriter;
    ObjectKeyMapper objectKeyMapper = new TimeBasedDailyObjectKeyMapper();

    @PostConstruct
    void connect() throws URISyntaxException, InterruptedException {

        if (customObjectKeyMapper.isResolvable()) {
            objectKeyMapper = customObjectKeyMapper.get();
        }
        LOGGER.info("Using '{}' object name mapper", objectKeyMapper);

        AwsCredentialsProvider credProvider;
        if (useInstanceProfile) {
            LOGGER.info("Using Instance Profile Credentials For S3");
            credProvider = InstanceProfileCredentialsProvider.create();
        }
        else {
            credProvider = ProfileCredentialsProvider.create(credentialsProfile);
        }

        if (valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            batchWriter = new JsonBatchRecordWriter(objectKeyMapper, credProvider, bucket);
            batchWriter = new SparkBatchRecordWriter(objectKeyMapper, credProvider, bucket);
        }
        else {
            throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) value formats are {json,}!");
        }
    }

    @PreDestroy
    void close() {
        try {
            batchWriter.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing batchWriter:{} ", e.getMessage());
        }
        try {
            s3client.close();
        }
        catch (Exception e) {
            LOGGER.error("Exception while closing S3 client: ", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        try {
            for (ChangeEvent<Object, Object> record : records) {
                // final Serde<Json> serde = DebeziumSerdes.payloadJson(Json.class);
                // serde.serializer().serialize()
                JsonDeserializer keyJsonDeserializer = new JsonDeserializer();
                JsonNode keyJson = keyJsonDeserializer.deserialize(record.destination(), getBytes(record.key()));
                JsonNode valueJson = keyJsonDeserializer.deserialize(record.destination(), getBytes(record.value()));
                LOGGER.error(valueJson.toString());
                LOGGER.error(keyJson.toString());
                continue;
                // batchWriter.append(record.destination(), keyJson, valueJson);
                // committer.markProcessed(record);
            }
            committer.markBatchFinished();
        }
        catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            LOGGER.error(sw.toString());
            throw new InterruptedException(e.getMessage());
        }
    }
}
