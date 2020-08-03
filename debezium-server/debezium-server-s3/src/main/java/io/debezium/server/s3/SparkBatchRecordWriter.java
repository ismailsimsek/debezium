/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.eclipse.microprofile.config.ConfigProvider;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Files;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class SparkBatchRecordWriter implements BatchRecordWriter, AutoCloseable {

    private static final String PROP_PREFIX = "debezium.sink.s3.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_BUCKET_NAME = PROP_PREFIX + "bucket.name";
    protected final File TEMPDIR = Files.createTempDir();
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBatchRecordWriter.class);
    private LocalDateTime batchTime = LocalDateTime.now();
    final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.batch.row.limit", Integer.class).orElse(500);
    final String tags = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.object.tags", String.class).orElse("");
    final String region = ConfigProvider.getConfig().getOptionalValue(PROP_REGION_NAME, String.class).orElse("eu-central-1");
    final String endpointOverride = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.endpointoverride", String.class).orElse("false");

    // private final S3Client s3Client;
    private final String bucket;
    private final ObjectKeyMapper objectKeyMapper;
    final DB cdcDb;
    final ConcurrentMap<String, String> map_data;
    final ConcurrentMap<String, Integer> map_batchid;
    final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
    private final SparkConf sparkconf;
    private final SparkSession spark;
    private final AwsCredentialsProvider credProvider;

    public SparkBatchRecordWriter(ObjectKeyMapper mapper, AwsCredentialsProvider credProvider, String bucket) {
        this.credProvider = credProvider;
        this.bucket = bucket;
        this.objectKeyMapper = mapper;

        // init db
        this.cdcDb = DBMaker
                .fileDB(TEMPDIR.toPath().resolve("debeziumevents.db").toFile())
                .fileMmapEnable()
                .transactionEnable()
                .closeOnJvmShutdown()
                .fileDeleteAfterClose()
                .make();
        map_data = cdcDb
                .hashMap("map_data", Serializer.STRING, Serializer.STRING)
                .createOrOpen();
        map_batchid = cdcDb
                .hashMap("map_batchid", Serializer.STRING, Serializer.INTEGER)
                .createOrOpen();
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

        // @TODO use schema in the json event to fix dataframe schem
        LOGGER.info("Starting S3 Batch Consumer({})", this.getClass().getName());
        LOGGER.info("Set Batch Row limit to {} Rows", batchLimit);
        LOGGER.info("Local Cache (MapDb) Location:{}", TEMPDIR.toPath().resolve("debeziumevents.db").toAbsolutePath().toString());
        setupTimer();
    }

    private void setupTimer() {
        final Integer timerBatchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.batch.time.limit", Integer.class).orElse(3600);
        LOGGER.info("Set Batch Time limit to {} Second", timerBatchLimit);
        Runnable timerTask = () -> {
            LOGGER.debug("Timer is up uploading batch data!");
            try {
                this.uploadBatch();
            }
            catch (Exception e) {
                LOGGER.error("Timer based batch upload failed data will be uploaded with next batch!");
            }
        };
        timerExecutor.scheduleWithFixedDelay(timerTask, timerBatchLimit, timerBatchLimit, TimeUnit.SECONDS);
    }

    @Override
    public void append(String destination, JsonNode keyJson, JsonNode valueJson) {

        if (!map_data.containsKey(destination)) {
            map_data.put(destination, valueJson.toString());
            map_batchid.putIfAbsent(destination, 0);
            cdcDb.commit();
            return;
        }

        map_data.put(destination, map_data.get(destination) + IOUtils.LINE_SEPARATOR + valueJson.toString());

        if (StringUtils.countMatches(map_data.get(destination), IOUtils.LINE_SEPARATOR) >= batchLimit) {
            LOGGER.debug("Batch Limit reached Uploading Data, destination:{} batchId:{}", destination, map_batchid.get(destination));
            this.uploadBatchFile(destination);
        }
        cdcDb.commit();
    }

    private void uploadBatchFile(String destination) {
        Integer batchId = map_batchid.get(destination);
        final String data = map_data.get(destination);
        String s3File = objectKeyMapper.map(destination, batchTime, batchId);
        LOGGER.debug("Uploading s3File destination:{} key:{}", destination, s3File);
        // @TODO add helper class to process json schema, convert to dataframe. with schema
        // https://sparkbyexamples.com/spark/spark-read-json-with-schema/
        // https://sparkbyexamples.com/spark/spark-sql-dataframe-data-types/
        List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
        Dataset<String> ds = spark.createDataset(jsonData, Encoders.STRING());
        // schema
        // Dataset<Row> df = spark.read().schema(mySchema).json(_df);
        Dataset<Row> df = spark.read().json(ds);
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
    public void uploadBatch() {
        int numBatchFiles = 0;
        for (String k : map_data.keySet()) {
            uploadBatchFile(k);
            numBatchFiles++;
        }
        this.setBatchTime();
        LOGGER.info("Uploaded {} Batch Files, started new batch Batch Time:{}", numBatchFiles, this.batchTime.toEpochSecond(ZoneOffset.UTC));
        // if (!map_data.isEmpty()) {
        // LOGGER.error("Non Processed Batch Data Found batchTime:{} destination: {}!!", batchTime.toString(), map_data.keySet().toString());
        // }
    }

    @Override
    public void close() {
        stopTimer();
        if (!cdcDb.isClosed()) {
            this.uploadBatch();
            closeDb();
        }
        spark.stop();
        TEMPDIR.delete();
    }

    private void setBatchTime() {
        batchTime = LocalDateTime.now();
    }

    private void closeDb() {
        if (!cdcDb.isClosed()) {
            // upload data second time
            if (!map_data.isEmpty()) {
                this.uploadBatch();
            }
            if (!map_data.isEmpty()) {
                LOGGER.error("Non Processed Batch Data Found!");
            }
            else {
                LOGGER.info("All Batch Data Successfully Processed.");
            }

            LOGGER.info("Closing S3 Batch Consumer({})", this.getClass().getName());
            cdcDb.close();
        }
    }

    private void stopTimer() {
        timerExecutor.shutdown();
        try {
            if (!timerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                timerExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("Timer Shutting Down Failed {}", e.getMessage());
            timerExecutor.shutdownNow();
        }
    }
}
