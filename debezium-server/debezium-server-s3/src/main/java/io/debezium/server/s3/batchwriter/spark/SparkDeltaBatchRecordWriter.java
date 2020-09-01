/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3.batchwriter.spark;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;

import io.debezium.server.s3.keymapper.ObjectKeyMapper;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SparkDeltaBatchRecordWriter extends AbstractSparkBatchRecordWriter {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SparkDeltaBatchRecordWriter.class);

    public SparkDeltaBatchRecordWriter(ObjectKeyMapper mapper) throws URISyntaxException {
        super(mapper);
        this.saveFormat = "delta";
        LOGGER.info("Starting S3 Spark-Delta-Io Consumer({})", this.getClass().getName());
        LOGGER.info("Spark save format is '{}'", saveFormat);
    }

    protected void writeFile(Dataset<Row> df, String deltaTableName) {

        // try {
        // df.writeTo("s3a://testaatableaa").append();
        LOGGER.error(df.schema().toString());
        df.write().format("delta").mode("append").save(deltaTableName);
        // }
        // catch (NoSuchTableException e) {
        // LOGGER.info("Table Not Found! Creating Delta table: '{}'!", fileName);
        // df.writeTo("aatableaa").using(saveFormat).createOrReplace();
        // }
    }

    protected void uploadBatchFile(String destination) {
        Integer batchId = map_batchid.get(destination);
        final String data = map_data.get(destination);
        String s3File = objectKeyMapper.map(destination, batchTime, batchId, saveFormat);
        LOGGER.debug("Uploading s3File With Spark destination:'{}' key:'{}'", destination, s3File);
        updateSparkSession();
        List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
        Dataset<String> ds = spark.createDataset(jsonData, Encoders.STRING());
        DataFrameReader dfReader = spark.read();
        // Read DF with Schema if schema exists
        if (!jsonData.isEmpty()) {
            try {
                StructType schema = SparkBatchSchemaUtil.getSparkDfSchema(Iterables.getLast(jsonData));
                if (schema != null && !schema.isEmpty()) {
                    dfReader.schema(schema);
                    LOGGER.info("Found Schema in data: {}", schema.toDDL());
                }
            }
            catch (JsonProcessingException e) {
                LOGGER.warn(e.getMessage());
                LOGGER.warn("Failed to create Spark Schema. Falling back to Schema inference");
            }

            Dataset<Row> df = dfReader.json(ds);
            if (removeSchema && Arrays.asList(df.columns()).contains("payload")) {
                df = df.select("payload.*");
            }
            final String fileName = bucket + "/" + s3File;
            df.write()
                    .mode(SaveMode.Append)
                    .format(saveFormat)
                    .save(fileName);
        }
        // increment batch id
        map_batchid.put(destination, batchId + 1);
        // start new batch
        map_data.remove(destination);
        cdcDb.commit();
        LOGGER.debug("Upload Succeeded! destination:'{}' key:'{}'", destination, s3File);
    }

}
