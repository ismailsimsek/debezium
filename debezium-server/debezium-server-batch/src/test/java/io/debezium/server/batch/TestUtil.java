/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch;

import org.apache.iceberg.types.Types;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.iceberg.Schema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.util.Testing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestUtil {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TestUtil.class);
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
        // StructType ss = ConsumerUtil.getEventSparkDfSchema(serdeWithSchema);
        assertNotNull(s);
        assertEquals(s.findField("ts_ms").fieldId(), 26);
        assertEquals(s.findField(7).name(), "last_name");
        assertTrue(s.asStruct().toString().contains("source: optional struct<"));
        assertTrue(s.asStruct().toString().contains("after: optional struct<"));
        s = ConsumerUtil.getEventIcebergSchema(unwrapWithSchema);
    }

    @Test
    public void testNestedIcebergSchemaLoop() throws JsonProcessingException {
        Schema s = ConsumerUtil.getEventIcebergSchema(serdeWithSchema);
        LOGGER.error(s.getAliases().toString());
        LOGGER.error(s.columns().toString());
        assert s != null;
        for (Types.NestedField f : s.columns()) {
            LOGGER.error("{}, {}, {}", f.type(), f.name(), f.doc());
        }
    }
}
