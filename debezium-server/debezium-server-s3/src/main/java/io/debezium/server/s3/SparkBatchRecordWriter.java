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
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SparkBatchRecordWriter extends AbstractBatchRecordWriter {
    private final SparkConf sparkconf;
    private final SparkSession spark;

    public SparkBatchRecordWriter(ObjectKeyMapper mapper, AwsCredentialsProvider credProvider, String bucket) throws URISyntaxException {
        super(mapper, credProvider, bucket);

        this.sparkconf = new SparkConf()
                .setAppName("CDC s3 Sink Using Spark")
                .setMaster("local")
                .set("spark.master", "local")
                .set("fs.s3a.access.key", this.credProvider.resolveCredentials().accessKeyId())
                .set("fs.s3a.secret.key", this.credProvider.resolveCredentials().secretAccessKey())
                .set("fs.s3a.endpoint", "s3." + region + ".amazonaws.com");
        // used for testing, using minio
        if (!endpointOverride.trim().toLowerCase().equals("false")) {
            this.sparkconf.set("spark.hadoop.fs.s3a.endpoint", endpointOverride);
        }
        this.spark = SparkSession
                .builder()
                .config(this.sparkconf)
                .getOrCreate();
        // this.spark.newSession()

        LOGGER.info("Starting S3 Spark Consumer({})", this.getClass().getName());
    }

    protected void uploadBatchFile(String destination) {
        Integer batchId = map_batchid.get(destination);
        final String data = map_data.get(destination);
        String s3File = objectKeyMapper.map(destination, batchTime, batchId);
        LOGGER.debug("Uploading s3File destination:{} key:{}", destination, s3File);
        List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
        Dataset<String> ds = spark.createDataset(jsonData, Encoders.STRING());
        DataFrameReader dfReader = spark.read();
        // Read DF with Schema if schema exists
        // @TODO add helper class to process json schema, convert to dataframe. with schema
        // https://sparkbyexamples.com/spark/spark-read-json-with-schema/
        // https://sparkbyexamples.com/spark/spark-sql-dataframe-data-types/
        if (!jsonData.isEmpty()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode lastEvent = mapper.readTree(Iterables.getLast(jsonData));
                if (lastEvent != null && lastEvent.has("schema")) {
                    StructType schema = null; // @TODO get DF schema.
                    dfReader.schema(schema);
                }
            }
            catch (JsonProcessingException e) {
                LOGGER.warn(e.getMessage());
            }
        }
        Dataset<Row> df = dfReader.json(ds);

        df.write()
                .mode(SaveMode.Append)
                .parquet(s3File);
        // increment batch id
        map_batchid.put(destination, batchId + 1);
        // start new batch
        map_data.remove(destination);
        cdcDb.commit();
        LOGGER.debug("Upload Succeeded! destination:{} key:{}", destination, s3File);

    }

    @Override
    public void close() {
        super.close();
        spark.close();
    }
}
