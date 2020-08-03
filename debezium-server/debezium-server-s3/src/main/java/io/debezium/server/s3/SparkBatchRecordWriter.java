/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.ConfigProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static io.debezium.server.s3.SparkDfSchemaUtil.getSparkDfSchema;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SparkBatchRecordWriter extends AbstractBatchRecordWriter {

    private static final String SPARK_PROP_PREFIX = "debezium.sink.s3sparkbatch.";
    final Boolean removeSchema = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.s3sparkbatch.removeschema", Boolean.class).orElse(true);

    private final SparkConf sparkconf = new SparkConf()
            .setAppName("CDC s3 Sink Using Spark")
            .setMaster("local");

    private void addSparkconf() {
        // used for testing, using minio
        if (!endpointOverride.trim().toLowerCase().equals("false")) {
            this.sparkconf.set("spark.hadoop.fs.s3a.endpoint", endpointOverride);
        }
        this.sparkconf
                .set("fs.s3a.access.key", this.credProvider.resolveCredentials().accessKeyId())
                .set("fs.s3a.secret.key", this.credProvider.resolveCredentials().secretAccessKey())
                .set("fs.s3a.endpoint", "s3." + region + ".amazonaws.com");
        for (String name : ConfigProvider.getConfig().getPropertyNames()) {
            if (name.startsWith(SPARK_PROP_PREFIX)) {
                this.sparkconf.set(name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
                LOGGER.info("Setting Spark Conf '{}'='{}'", name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
            }
        }

    }

    private SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .config(this.sparkconf)
                .getOrCreate();
    }

    public SparkBatchRecordWriter(ObjectKeyMapper mapper, AwsCredentialsProvider credProvider, String bucket) throws URISyntaxException {
        super(mapper, credProvider, bucket);
        this.addSparkconf();
        LOGGER.info("Starting S3 Spark Consumer({})", this.getClass().getName());
    }

    protected void uploadBatchFile(String destination) {
        Integer batchId = map_batchid.get(destination);
        final String data = map_data.get(destination);
        String s3File = objectKeyMapper.map(destination, batchTime, batchId);
        LOGGER.debug("Uploading s3File destination:{} key:{}", destination, s3File);
        List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
        SparkSession spark = getSparkSession();
        Dataset<String> ds = spark.createDataset(jsonData, Encoders.STRING());
        DataFrameReader dfReader = spark.read();
        // Read DF with Schema if schema exists
        if (!jsonData.isEmpty()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode lastEvent = mapper.readTree(Iterables.getLast(jsonData));
                StructType schema = getSparkDfSchema(lastEvent);
                if (schema != null) {
                    dfReader.schema(schema);
                }
            }
            catch (JsonProcessingException e) {
                LOGGER.warn(e.getMessage());
            }
        }
        Dataset<Row> df = dfReader.json(ds);
        if (removeSchema && Arrays.asList(df.columns()).contains("payload")) {
            df = df.select("payload.*");
        }

        df.write()
                .mode(SaveMode.Append)
                .parquet(s3File);
        // increment batch id
        map_batchid.put(destination, batchId + 1);
        // start new batch
        map_data.remove(destination);
        cdcDb.commit();
        spark.close();
        LOGGER.debug("Upload Succeeded! destination:{} key:{}", destination, s3File);
    }
}
