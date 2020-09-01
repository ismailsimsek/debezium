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
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;

import io.debezium.server.s3.batchwriter.AbstractBatchRecordWriter;
import io.debezium.server.s3.keymapper.ObjectKeyMapper;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
abstract class AbstractSparkBatchRecordWriter extends AbstractBatchRecordWriter {

    private static final String SPARK_PROP_PREFIX = "debezium.sink.s3sparkbatch.";
    final Boolean removeSchema = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3sparkbatch.removeschema", Boolean.class).orElse(true);
    protected String saveFormat = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3sparkbatch.saveformat", String.class).orElse("json");
    final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3sparkbatch.row.limit", Integer.class).orElse(500);
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractSparkBatchRecordWriter.class);

    SparkSession spark;

    private final SparkConf sparkconf = new SparkConf()
            .setAppName("CDC-S3-Batch-Spark-Sink")
            .setMaster("local");

    public AbstractSparkBatchRecordWriter(ObjectKeyMapper mapper) throws URISyntaxException {
        super(mapper);
        this.initSparkconf();
        this.spark = SparkSession
                .builder()
                .config(this.sparkconf)
                .getOrCreate();
        // spark.sparkContext().setLogLevel("WARN");

        LOGGER.info("Starting S3 Spark Consumer({})", this.getClass().getName());
        LOGGER.info("Spark save format is '{}'", saveFormat);

    }

    private void initSparkconf() {

        if (useInstanceProfile) {
            this.sparkconf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider");
            LOGGER.info("Setting Spark Conf '{}'='{}'", "fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider");
        }
        else {
            this.sparkconf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
            LOGGER.info("Setting Spark Conf '{}'='{}'", "fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        }

        for (String name : ConfigProvider.getConfig().getPropertyNames()) {
            if (name.startsWith(SPARK_PROP_PREFIX)) {
                this.sparkconf.set(name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
                LOGGER.info("Setting Spark Conf '{}'='{}'", name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
            }
        }
        this.sparkconf.set("spark.ui.enabled", "false");
    }

    protected void updateSparkSession() {
        if (spark.sparkContext() == null || spark.sparkContext().isStopped()) {
            spark = spark.newSession();
        }
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

    @Override
    public void close() {
        super.close();
        if (!spark.sparkContext().isStopped()) {
            spark.close();
        }
    }
}
