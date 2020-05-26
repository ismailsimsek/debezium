/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import io.debezium.server.TestConfigSource;
import io.debezium.server.TestDatabase;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import java.util.HashMap;
import java.util.Map;

public class S3TestConfigSource extends TestConfigSource {

    public static final String S3_REGION = "eu-central-1";
    public static final String S3_BUCKET = "test-bucket";

    final Map<String, String> s3Test = new HashMap<>();

    public S3TestConfigSource() {
        s3Test.put("debezium.sink.type", "s3");
        s3Test.put("debezium.sink.s3.region", S3_REGION);
        s3Test.put("debezium.sink.s3.endpointoverride", "http://localhost:9000");
        s3Test.put("debezium.sink.s3.bucket.name", S3_BUCKET);
        s3Test.put("debezium.sink.s3.objectkey.prefix", "debezium-server-");
        /*
         * integrationTest.put("debezium.format.key", "avro");
         * integrationTest.put("debezium.format.value", "avro");
         * integrationTest.put("schema.registry.url", "http://localhost:8081");
         * integrationTest.put("debezium.schema.registry.url", "http://localhost:8081");
         * integrationTest.put("key.converter.schema.registry.url", "http://localhost:8081");
         * integrationTest.put("value.converter.schema.registry.url", "http://localhost:8081");
         * integrationTest.put("key.converter", "io.confluent.connect.avro.AvroConverter");
         * integrationTest.put("value.converter", "io.confluent.connect.avro.AvroConverter");
         */

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
        s3Test.put("debezium.source.table.whitelist", "inventory.customers");

        config = s3Test;
    }
}
