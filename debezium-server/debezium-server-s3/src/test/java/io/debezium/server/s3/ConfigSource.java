/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;
import io.debezium.server.TestDatabase;

public class ConfigSource extends TestConfigSource {

    public static final String S3_REGION = "us-east-1";
    public static final String S3_BUCKET = "test-bucket";

    final Map<String, String> s3Test = new HashMap<>();

    public ConfigSource() {
        // s3 and common conf
        s3Test.put("debezium.sink.type", "s3");
        s3Test.put("debezium.sink.s3.region", S3_REGION);
        s3Test.put("debezium.sink.s3.endpointoverride", "http://localhost:" + S3MinioServer.MINIO_DEFAULT_PORT_MAP);
        s3Test.put("debezium.sink.s3.bucket.name", "s3a://" + S3_BUCKET);
        // s3Test.put("debezium.sink.s3.bucket.name", S3_BUCKET);
        s3Test.put("debezium.sink.s3.objectkey.prefix", "debezium-server-");
        s3Test.put("debezium.sink.s3.credentials.useinstancecred", "false");
        // s3batch conf
        s3Test.put("debezium.sink.s3batch.row.limit", "2");
        // s3sparkbatch conf
        s3Test.put("debezium.sink.s3sparkbatch.row.limit", "2");
        // s3sparkbatch sink conf
        s3Test.put("debezium.sink.s3sparkbatch.removeschema", "true");
        s3Test.put("debezium.sink.s3sparkbatch.saveformat", "iceberg");
        // spark conf
        s3Test.put("debezium.sink.s3sparkbatch.spark.ui.enabled", "false");
        s3Test.put("debezium.sink.s3sparkbatch.spark.sql.session.timeZone", "UTC");
        s3Test.put("debezium.sink.s3sparkbatch.user.timezone", "UTC");
        s3Test.put("debezium.sink.s3sparkbatch.com.amazonaws.services.s3.enableV4", "true");
        s3Test.put("debezium.sink.s3sparkbatch.com.amazonaws.services.s3a.enableV4", "true");
        s3Test.put("debezium.sink.s3sparkbatch.spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true");
        s3Test.put("debezium.sink.s3sparkbatch.spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true");
        s3Test.put("debezium.sink.s3sparkbatch.spark.io.compression.codec", "snappy");
        s3Test.put("debezium.sink.s3sparkbatch.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        // endpoint override or testing
        s3Test.put("debezium.sink.s3sparkbatch.spark.hadoop.fs.s3a.access.key", S3MinioServer.MINIO_ACCESS_KEY);
        s3Test.put("debezium.sink.s3sparkbatch.spark.hadoop.fs.s3a.secret.key", S3MinioServer.MINIO_SECRET_KEY);
        s3Test.put("debezium.sink.s3sparkbatch.spark.hadoop.fs.s3a.path.style.access", "true");
        s3Test.put("debezium.sink.s3sparkbatch.spark.hadoop.fs.s3a.endpoint", "http://localhost:" + S3MinioServer.MINIO_DEFAULT_PORT_MAP) ; // minio specific setting
        s3Test.put("debezium.sink.s3sparkbatch.spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        s3Test.put("debezium.sink.s3sparkbatch.org.apache.iceberg", "iceberg-spark3-runtime:0.10.0");
        s3Test.put("debezium.sink.s3sparkbatch.spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        s3Test.put("debezium.sink.s3sparkbatch.spark.sql.catalog.spark_catalog.type", "hadoop");
        s3Test.put("debezium.sink.s3sparkbatch.spark.sql.catalog.spark_catalog.warehouse", "s3a://" + S3_BUCKET + "/spark3_iceberg_catalog");

        s3Test.put("debezium.sink.s3sparkbatch.spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore");
        // enable disable schema
        s3Test.put("debezium.format.value.schemas.enable", "true");
        // s3Test.put("debezium.format.value.converter", "io.debezium.converters.CloudEventsConverter");
        // s3Test.put("value.converter", "io.debezium.converters.CloudEventsConverter");
        // s3Test.put("debezium.format.value.converter.data.serializer.type" , "json");
        // s3Test.put("value.converter.data.serializer.type", "json");

        // debezium unwrap message
        s3Test.put("debezium.transforms", "unwrap");
        s3Test.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        s3Test.put("debezium.transforms.unwrap.add.fields", "op,table,lsn,source.ts_ms");
        s3Test.put("debezium.transforms.unwrap.add.headers", "db");
        s3Test.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
        // debezium source conf
        s3Test.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        s3Test.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        s3Test.put("debezium.source.offset.flush.interval.ms", "0");
        s3Test.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
        s3Test.put("debezium.source.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        s3Test.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
        s3Test.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
        s3Test.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
        s3Test.put("debezium.source.database.server.name", "testc");
        s3Test.put("debezium.source.schema.whitelist", "inventory");
        s3Test.put("debezium.source.table.whitelist", "inventory.customers,inventory.orders");

        config = s3Test;
    }
}
