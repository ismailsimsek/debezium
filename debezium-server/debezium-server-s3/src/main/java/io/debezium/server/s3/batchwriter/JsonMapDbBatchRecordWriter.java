/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3.batchwriter;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.debezium.server.s3.objectkeymapper.ObjectKeyMapper;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class JsonMapDbBatchRecordWriter implements BatchRecordWriter, AutoCloseable {

    protected final File TEMPDIR = Files.createTempDir();
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMapDbBatchRecordWriter.class);
    private LocalDateTime batchTime = LocalDateTime.now();
    final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.batch.row.limit", Integer.class).orElse(500);

    private final S3Client s3Client;
    private final String bucket;
    private final ObjectKeyMapper objectKeyMapper;
    final DB cdcDb;
    final ConcurrentMap<String, String> map_data;
    final ConcurrentMap<String, Integer> map_batchid;
    final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();

    public JsonMapDbBatchRecordWriter(ObjectKeyMapper mapper, S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.objectKeyMapper = mapper;

        // init db
        this.cdcDb = DBMaker
                .fileDB(TEMPDIR.toPath().resolve("debeziumevents.db").toFile())
                .fileMmapEnable()
                // .transactionEnable()
                .closeOnJvmShutdown()
                .fileDeleteAfterClose()
                .make();
        map_data = cdcDb
                .hashMap("map_data", Serializer.STRING, Serializer.STRING)
                .createOrOpen();
        map_batchid = cdcDb
                .hashMap("map_batchid", Serializer.STRING, Serializer.INTEGER)
                .createOrOpen();

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
    public void append(String destination, String eventValue) {

        if (!map_data.containsKey(destination)) {
            map_data.put(destination, eventValue);
            map_batchid.putIfAbsent(destination, 0);
            cdcDb.commit();
            return;
        }

        map_data.put(destination, map_data.get(destination) + IOUtils.LINE_SEPARATOR + eventValue);

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
        final PutObjectRequest putRecord = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3File)
                .build();
        s3Client.putObject(putRecord, RequestBody.fromString(data));
        // increment batch id
        map_batchid.put(destination, batchId + 1);
        // start new batch
        map_data.remove(destination);
        cdcDb.commit();
        LOGGER.debug("Upload Succeeded! destination:{} key:{}", destination, s3File);

    }

    @Override
    public void uploadBatch() {
        for (String k : map_data.keySet()) {
            uploadBatchFile(k);
        }
        this.setBatchTime();
        LOGGER.info("Uploaded Batch and started new batch Time:{}", this.batchTime.toEpochSecond(ZoneOffset.UTC));
        // if (!map_data.isEmpty()) {
        // LOGGER.error("Non Processed Batch Data Found batchTime:{} destination: {}!!", batchTime.toString(), map_data.keySet().toString());
        // }
    }

    @Override
    public void close() {
        stopTimer();
        this.uploadBatch();
        closeDb();
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
            LOGGER.error("Timer Shutingdown Failed {}", e.getMessage());
            timerExecutor.shutdownNow();
        }
    }
}
