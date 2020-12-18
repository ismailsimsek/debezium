/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.iceberg;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.server.batch.batchwriter.spark.SparkBatchSchemaUtil;
import io.debezium.util.Testing;

class TestSchema {

    final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
    final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");

    @Test
    public void testSimpleSchema() throws JsonProcessingException {
        StructType s = SparkBatchSchemaUtil.getEventSparkDfSchema(unwrapWithSchema);
        assertNotNull(s);
        assertTrue(s.catalogString().contains("id:int,order_date:int,purchaser:int,quantity:int,product_id:int,__op:string"));
    }

    @Test
    public void testNestedSchema() throws JsonProcessingException {
        StructType s = SparkBatchSchemaUtil.getEventSparkDfSchema(serdeWithSchema);
        assertNotNull(s);
        assertTrue(s.catalogString().contains("before:struct<id"));
        assertTrue(s.catalogString().contains("after:struct<id"));
    }
}
