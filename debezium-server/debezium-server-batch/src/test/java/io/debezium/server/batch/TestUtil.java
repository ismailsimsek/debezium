/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.iceberg.Schema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.util.Testing;

class TestUtil {

    final String serdeUpdate = Testing.Files.readResourceAsString("json/serde-update.json");
    final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
    final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");

    @Test
    public void testSimpleSchema() throws JsonProcessingException {
        StructType s = ConsumerUtil.getEventSparkDfSchema(unwrapWithSchema);
        assertNotNull(s);
        assertTrue(s.catalogString().contains("id:int,order_date:int,purchaser:int,quantity:int,product_id:int,__op:string"));
    }

    @Test
    public void testNestedSparkSchema() throws JsonProcessingException {
        StructType s = ConsumerUtil.getEventSparkDfSchema(serdeWithSchema);
        assertNotNull(s);
        assertTrue(s.catalogString().contains("before:struct<id"));
        assertTrue(s.catalogString().contains("after:struct<id"));
    }

    @Test
    public void testNestedIcebergSchema() throws JsonProcessingException {
        Schema s = ConsumerUtil.getEventIcebergSchema(serdeWithSchema);
        StructType ss = ConsumerUtil.getEventSparkDfSchema(serdeWithSchema);
        assertNotNull(s);
        assert ss != null;
        assertEquals(s.asStruct().toString(), ss.catalogString());
        assertTrue(s.asStruct().toString().contains("before:struct<id"));
        assertTrue(s.asStruct().toString().contains("after:struct<id"));
    }
}
