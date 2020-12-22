/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("icebergevents")
@Dependent
public class IcebergEventsChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    static final String TABLE_NAME = "debezium_events";
    static final Schema TABLE_SCHEMA = new Schema(
            required(1, "event_destination", Types.StringType.get(), "event destination"),
            optional(2, "event_key", Types.StringType.get()),
            optional(3, "event_key_value", Types.StringType.get()),
            optional(4, "event_value", Types.StringType.get()),
            optional(5, "event_value_format", Types.StringType.get()),
            optional(6, "event_key_format", Types.StringType.get()),
            optional(7, "event_sink_timestamp", Types.TimestampType.withZone()));
    static final PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA).identity("event_destination").day("event_sink_timestamp").build();
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergEventsChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.iceberg.";
    final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.row.limit", Integer.class).orElse(500);
    @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
    String valueFormat;
    @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
    String keyFormat;
    Configuration hadoopConf = new Configuration();
    @ConfigProperty(name = PROP_PREFIX + "catalog-impl" /* CatalogProperties.CATALOG_IMPL */, defaultValue = "hadoop")
    String catalogImpl;
    @ConfigProperty(name = PROP_PREFIX + "warehouse" /* CatalogProperties.WAREHOUSE_LOCATION */)
    String warehouseLocation;
    @ConfigProperty(name = PROP_PREFIX + "fs.defaultFS")
    String defaultFs;
    @ConfigProperty(name = "debezium.transforms")
    String transforms;
    @ConfigProperty(name = "value.converter.schemas.enable", defaultValue = "false")
    Boolean formatValueSchemasEnable;
    @ConfigProperty(name = "key.converter.schemas.enable", defaultValue = "true")
    Boolean formatKeySchemasEnable;
    @ConfigProperty(name = "debezium.format.schemas.enable")
    // @ConfigProperty(name = "converter.schemas.enable")
    Boolean formatSchemasEnable;
    @ConfigProperty(name = "debezium.source.database.dbname")
    String databaseName;

    Catalog icebergCatalog;
    Table eventTable;

    @PostConstruct
    void connect() throws InterruptedException {
        if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
        }
        if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
        }

        // loop and set hadoopConf
        for (String name : ConfigProvider.getConfig().getPropertyNames()) {
            if (name.startsWith(PROP_PREFIX)) {
                this.hadoopConf.set(name.substring(PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
                LOGGER.debug("Setting Hadoop Conf '{}' from application.properties!", name.substring(PROP_PREFIX.length()));
            }
        }

        if (warehouseLocation == null || warehouseLocation.trim().isEmpty()) {
            warehouseLocation = defaultFs + "/iceberg/warehouse";
        }

        icebergCatalog = new HadoopCatalog("iceberg", hadoopConf, warehouseLocation);

        if (!icebergCatalog.tableExists(TableIdentifier.of(TABLE_NAME))) {
            icebergCatalog.createTable(TableIdentifier.of(TABLE_NAME), TABLE_SCHEMA, TABLE_PARTITION);
        }
        eventTable = icebergCatalog.loadTable(TableIdentifier.of(TABLE_NAME));
        // hadoopTables = new HadoopTables(hadoopConf);// do we need this ??
        // @TODO iceberg 11 . make catalog dynamic using catalogImpl parametter!
        // if (catalogImpl != null) {
        // icebergCatalog = CatalogUtil.loadCatalog(catalogImpl, name, options, hadoopConf);
        // }

    }

    // @PreDestroy
    // void close() {
    // // try {
    // // s3client.close();
    // // }
    // // catch (Exception e) {
    // // LOGGER.error("Exception while closing S3 client: ", e);
    // // }
    // }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LocalDateTime batchTime = LocalDateTime.now();
        ArrayList<Record> icebergRecords = Lists.newArrayList();
        GenericRecord icebergRecord = GenericRecord.create(TABLE_SCHEMA);
        int batchId = 0;
        int cntNumRows = 0;
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.debug("key===>{}", record.key());
            LOGGER.debug("value===>{}", record.value());
            LOGGER.debug("dest===>{}", record.destination());
            Map<String, Object> var1 = Maps.newHashMapWithExpectedSize(TABLE_SCHEMA.columns().size());
            var1.put("event_destination", record.destination());
            var1.put("event_key", getString(record.key()));
            var1.put("event_key_value", null); // @TODO extract key value!
            var1.put("event_value", getString(record.value()));
            var1.put("event_value_format", valueFormat);
            var1.put("event_key_format", keyFormat);
            var1.put("event_sink_timestamp", LocalDateTime.now().atOffset(ZoneOffset.UTC));
            // @TODO add schema enabled flags! for key and value!
            // @TODO add flattened flag SMT unwrap!
            // @TODO add db name
            // @TODO extract value from key and store it - event_key_value!

            icebergRecords.add(icebergRecord.copy(var1));

            cntNumRows++;
            if (cntNumRows > batchLimit) {
                commitBatch(icebergRecords, batchTime, batchId);
                cntNumRows = 0;
                batchId++;
                icebergRecords.clear();
            }
            // committer.markProcessed(record);
        }
        commitBatch(icebergRecords, batchTime, batchId);
        icebergRecords.clear();
        committer.markBatchFinished();
    }

    private void commitBatch(ArrayList<Record> icebergRecords, LocalDateTime batchTime, int batchId) throws InterruptedException {
        final String fileName = UUID.randomUUID() + "-" + batchTime.toEpochSecond(ZoneOffset.UTC) + "-" + batchId + "." + FileFormat.PARQUET.toString().toLowerCase();
        OutputFile out = eventTable.io().newOutputFile(eventTable.locationProvider().newDataLocation(fileName));

        FileAppender<Record> writer = null;
        try {
            writer = Parquet.write(out)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .forTable(eventTable)
                    .overwrite()
                    .build();

            try (Closeable toClose = writer) {
                writer.addAll(icebergRecords);
            }

        }
        catch (IOException e) {
            LOGGER.error("Failed committing events to iceberg table!", e);
            throw new InterruptedException(e.getMessage());
        }

        DataFile dataFile = DataFiles.builder(eventTable.spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(out.location())
                .withFileSizeInBytes(writer.length())
                .withSplitOffsets(writer.splitOffsets())
                .withMetrics(writer.metrics())
                .build();

        LOGGER.debug("Appending new file '{}' !", dataFile.path());
        eventTable.newAppend()
                .appendFile(dataFile)
                .commit();
        LOGGER.info("Committed events to table! {}", eventTable.location());
    }

}
