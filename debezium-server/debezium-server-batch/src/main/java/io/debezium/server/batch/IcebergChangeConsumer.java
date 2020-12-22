/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;
import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
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
import org.apache.kafka.connect.json.JsonDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    JsonDeserializer jsonDeserializer = new JsonDeserializer();

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
        // @TODO iceberg 11 . make catalog dynamic using catalogImpl parametter!
        // if (catalogImpl != null) {
        // icebergCatalog = CatalogUtil.loadCatalog(catalogImpl, name, options, hadoopConf);
        // }

    }

    public Record getIcebergRecord(GenericRecord genericRecord, ChangeEvent<Object, Object> event) {
        JsonNode valueJson = jsonDeserializer.deserialize(event.destination(), getBytes(event.value()));
        LOGGER.error(genericRecord.struct().toString());
        // @TODO convert event to json
        //  @TODO  json to Record
        return genericRecord;
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
                .collect(Collectors.groupingBy(
                        ChangeEvent::destination,
                        Collectors.mapping(p -> p,
                                Collectors.toCollection(ArrayList::new)
                        )
                ));

        for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> event : result.entrySet()) {
            Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of(event.getKey()));
            // @TODO create table if not exists!
            // if (!icebergCatalog.tableExists(TableIdentifier.of(TABLE_NAME))) {
            // icebergCatalog.createTable(TableIdentifier.of(TABLE_NAME), TABLE_SCHEMA, TABLE_PARTITION);
            // }
            GenericRecord genericRecord = GenericRecord.create(icebergTable.schema());
            ArrayList<Record> icebergRecords = event.getValue().stream()
                    .map(x -> getIcebergRecord(genericRecord, x))
                    .collect(Collectors.toCollection(ArrayList::new));

            commitTable(icebergTable, icebergRecords);
        }
        committer.markBatchFinished();
    }

    private void commitTable(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {
        final String fileName = UUID.randomUUID() + "-" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) + "-" + 0 + "." + FileFormat.PARQUET.toString().toLowerCase();
        OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));

        FileAppender<Record> writer = null;
        try {
            writer = Parquet.write(out)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .forTable(icebergTable)
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

        DataFile dataFile = DataFiles.builder(icebergTable.spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(out.location())
                .withFileSizeInBytes(writer.length())
                .withSplitOffsets(writer.splitOffsets())
                .withMetrics(writer.metrics())
                .build();

        // @TODO commit all files/TABLES at once! together! possible?
        LOGGER.debug("Appending new file '{}' !", dataFile.path());
        icebergTable.newAppend()
                .appendFile(dataFile)
                .commit();
        LOGGER.info("Committed events to table! {}", icebergTable.location());
    }

}
