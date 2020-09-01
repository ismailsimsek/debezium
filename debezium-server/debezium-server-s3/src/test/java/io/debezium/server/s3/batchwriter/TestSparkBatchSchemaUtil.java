/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3.batchwriter;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.server.s3.batchwriter.spark.SparkBatchSchemaUtil;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class TestSparkBatchSchemaUtil {

    @Test
    public void testSparkBatchSchemaUtil() throws JsonProcessingException {
        String event = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int32\",\"optional\":false,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"order_date\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"purchaser\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"quantity\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"product_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__op\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"__lsn\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"__source_ts_ms\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__deleted\"}],\"optional\":false,\"name\":\"testc.inventory.orders.Value\"},\"payload\":{\"id\":10003,\"order_date\":16850,\"purchaser\":1002,\"quantity\":2,\"product_id\":106,\"__op\":\"r\",\"__table\":\"orders\",\"__lsn\":33832960,\"__source_ts_ms\":1596309876678,\"__deleted\":\"false\"}}";
        StructType s = SparkBatchSchemaUtil.getSparkDfSchema(event);
        assertNotNull(s);
        System.out.println(s.catalogString());
        assertTrue(s.catalogString().contains("id:int,order_date:int,purchaser:int,quantity:int,product_id:int,__op:string"));
    }
}
