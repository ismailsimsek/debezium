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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.s3.batchwriter.BatchRecordWriter;
import io.debezium.server.s3.batchwriter.SparkBatchRecordWriter;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("s3sparkbatch")
@Dependent
public class S3SparkBatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(S3SparkBatchChangeConsumer.class);
    protected static final String PROP_PREFIX = "debezium.sink.s3.";
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    @Inject
    Instance<ObjectKeyMapper> customObjectKeyMapper;
    BatchRecordWriter batchWriter;
    ObjectKeyMapper objectKeyMapper = new TimeBasedDailyObjectKeyMapper();

    @PreDestroy
    void close() {
        try {
            batchWriter.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing sparkBatchWriter:{} ", e.getMessage());
        }
    }

    @PostConstruct
    void connect() throws URISyntaxException, InterruptedException {

        if (customObjectKeyMapper.isResolvable()) {
            objectKeyMapper = customObjectKeyMapper.get();
        }
        LOGGER.info("Using '{}' object name mapper", objectKeyMapper);
        if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) value formats are {json,}!");
        }
        batchWriter = new SparkBatchRecordWriter(objectKeyMapper);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        try {
            for (ChangeEvent<Object, Object> record : records) {
                JsonDeserializer jsonDeserializer = new JsonDeserializer();
                JsonNode valueJson = jsonDeserializer.deserialize(record.destination(), getBytes(record.value()));
                batchWriter.append(record.destination(), valueJson);
                // committer.markProcessed(record);
            }
            batchWriter.uploadBatch();
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
