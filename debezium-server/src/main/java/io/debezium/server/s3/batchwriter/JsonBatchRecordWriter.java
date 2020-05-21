/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.debezium.server.s3.batchwriter;

import com.google.common.io.Files;
import io.debezium.engine.ChangeEvent;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JsonBatchRecordWriter implements BatchRecordWriter, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonBatchRecordWriter.class);
    static final ConcurrentHashMap<String, FileOutputStream> files = new ConcurrentHashMap<>();
    static final File TEMPDIR = Files.createTempDir();
    private final S3Client s3Client;
    private final String bucket;

    public JsonBatchRecordWriter(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public void append(String objectKey, ChangeEvent<Object, Object> record) throws IOException {

        if (!files.containsKey(objectKey)) {
            File nf = TEMPDIR.toPath().resolve(objectKey).toFile();
            LOGGER.debug("Creating file " + nf.getAbsolutePath().toLowerCase());
            nf.getParentFile().mkdirs();
            FileOutputStream outStream = new FileOutputStream(nf, true);
            files.put(objectKey, outStream);
        }
        IOUtils.write(record.value() + IOUtils.LINE_SEPARATOR, files.get(objectKey), Charset.defaultCharset());

    }

    @Override
    public void uploadBatch() throws IOException {
        for (Map.Entry<String, FileOutputStream> o : files.entrySet()) {
            o.getValue().close();
            LOGGER.debug("Uploading file {} to s3 {}", TEMPDIR.toPath().resolve(o.getKey()), bucket);
            final PutObjectRequest putRecord = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(o.getKey())
                    .build();
            s3Client.putObject(putRecord, TEMPDIR.toPath().resolve(o.getKey()));
            LOGGER.debug("Deleting File {}", o.getKey());
            TEMPDIR.toPath().resolve(o.getKey()).toFile().delete();
        }
        files.clear();
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, FileOutputStream> o : files.entrySet()) {
            o.getValue().close();
        }
        FileUtils.deleteDirectory(TEMPDIR);
    }
}
