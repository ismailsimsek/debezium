/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.iceberg;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.s3.keymapper.ObjectKeyMapper;
import io.debezium.server.s3.keymapper.TimeBasedDailyObjectKeyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.iceberg.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_BUCKET_NAME = PROP_PREFIX + "bucket.name";

    @Inject
    Instance<ObjectKeyMapper> customObjectKeyMapper;
    @ConfigProperty(name = PROP_BUCKET_NAME, defaultValue = "My-S3-Bucket")
    String bucket;
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());
    final String keyFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.key", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    static final Schema SCHEMA = new Schema(
            required(1, "event_destination", Types.StringType.get(), "event destination"),
            optional(2, "event_key", Types.StringType.get()),
            optional(3, "event_value", Types.StringType.get()),
            optional(4, "event_value_format", Types.TimestampType.withZone()),
            optional(5, "event_key_format", Types.TimestampType.withZone()),
            optional(6, "event_sink_timestamp", Types.TimestampType.withZone())
    );
    // debezium.format.value.schemas.enable
    // debezium.source.database.dbname


    @ConfigProperty(name = PROP_REGION_NAME, defaultValue = "eu-central-1")
    String region;

    @ConfigProperty(name = PROP_REGION_NAME, defaultValue = "My-DB")
    String database;

    String tableName = "debezium_events";

    Configuration hadoopConf;
    HadoopTables hadoopTables;

    private ObjectKeyMapper objectKeyMapper = new TimeBasedDailyObjectKeyMapper();

    @PostConstruct
    void connect() throws URISyntaxException {
        // loop and add ConfigProvider
        Configuration hadoopConf = new Configuration();
        hadoopTables = new HadoopTables(hadoopConf);

        if (customObjectKeyMapper.isResolvable()) {
            objectKeyMapper = customObjectKeyMapper.get();
        }
        LOGGER.info("Using '{}' stream name mapper", objectKeyMapper);
    }

    @PreDestroy
    void close() {
        //        try {
        //            s3client.close();
        //        }
        //        catch (Exception e) {
        //            LOGGER.error("Exception while closing S3 client: ", e);
        //        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LocalDateTime batchTime = LocalDateTime.now();
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.debug("key:{}", record.key());
            LOGGER.debug("value:{}", record.value());
            LOGGER.debug("dest:{}", record.destination());

            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
