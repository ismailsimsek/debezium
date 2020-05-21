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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.debezium.engine.ChangeEvent;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class JsonBatchRecordWriter implements BatchRecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonBatchRecordWriter.class);
    final static File TEMPDIR = Files.createTempDir();
    ConcurrentHashMap<String, FileOutputStream> files = new ConcurrentHashMap<>();

    @Override
    public void append(String objectKey, ChangeEvent<Object, Object> record) throws IOException {

        if (!files.containsKey(objectKey)) {
            File nf = TEMPDIR.toPath().resolve(objectKey).toFile();
            LOGGER.error("Creating file " + nf.getAbsolutePath().toLowerCase());
            nf.getParentFile().mkdirs();
            FileOutputStream outStream = new FileOutputStream(nf, true);
            files.put(objectKey, outStream);
        }
        IOUtils.write(record.value().toString() + IOUtils.LINE_SEPARATOR, files.get(objectKey), Charset.defaultCharset());

    }

    @Override
    public void upload(S3Client s3Client, String bucket) throws IOException {
        for (Map.Entry<String, FileOutputStream> o : files.entrySet()) {
            o.getValue().close();
            LOGGER.error("Uploading file " + o.getKey());
            final PutObjectRequest putRecord = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(o.getKey())
                    .build();
            s3Client.putObject(putRecord, TEMPDIR.toPath().resolve(o.getKey()));
        }
        remove();
    }

    @Override
    public void remove() throws IOException {
        for (Map.Entry<String, FileOutputStream> o : files.entrySet()) {
            o.getValue().close();
            TEMPDIR.toPath().resolve(o.getKey()).toFile().delete();
        }
        files.clear();
        FileUtils.deleteDirectory(TEMPDIR);
    }
}
