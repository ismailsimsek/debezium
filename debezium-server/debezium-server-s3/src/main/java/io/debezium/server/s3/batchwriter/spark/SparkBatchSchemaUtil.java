/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3.batchwriter.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.server.s3.batchwriter.AbstractBatchRecordWriter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SparkBatchSchemaUtil {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchRecordWriter.class);

    public static StructType getSparkDfSchema(JsonNode eventSchema) {

        StructType sparkSchema = new StructType();

        String schemaType = eventSchema.get("type").textValue();
        String schemaName = "root";
        if (eventSchema.has("field")) {
            schemaName = eventSchema.get("field").textValue();
        }
        LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);

        for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
            String fieldName = jsonSchemaFieldNode.get("field").textValue();
            String fieldType = jsonSchemaFieldNode.get("type").textValue();
            LOGGER.debug("Processing Field: {}.{}::{}", schemaName, fieldName, fieldType);
            // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
            switch (fieldType) {
                case "int8":
                case "int16":
                case "int32":
                case "int64":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()));
                    break;
                case "float8":
                case "float16":
                case "float32":
                case "float64":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.FloatType, true, Metadata.empty()));
                    break;
                case "boolean":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.BooleanType, true, Metadata.empty()));
                    break;
                case "string":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
                    break;
                case "bytes":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.ByteType, true, Metadata.empty()));
                    break;
                case "array":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, new ArrayType(), true, Metadata.empty()));
                    break;
                case "map":
                    sparkSchema = sparkSchema.add(new StructField(fieldName, new MapType(), true, Metadata.empty()));
                    break;
                case "struct":
                    // recursive call
                    StructType subSchema = SparkBatchSchemaUtil.getSparkDfSchema(jsonSchemaFieldNode);
                    sparkSchema = sparkSchema.add(new StructField(fieldName, subSchema, true, Metadata.empty()));
                    break;
                default:
                    // default to String type
                    sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
                    break;
            }
        }

        return sparkSchema;

    }

    public static StructType getSparkDfSchema(String event) throws JsonProcessingException {
        JsonNode jsonNode = new ObjectMapper().readTree(event);

        if (jsonNode == null
                || !jsonNode.has("fields")
                || !jsonNode.get("fields").isArray()) {
            return null;
        }
        return SparkBatchSchemaUtil.getSparkDfSchema(jsonNode.get("schema"));
    }

    public static StructType getEventSparkDfSchema(String event) throws JsonProcessingException {
        JsonNode jsonNode = new ObjectMapper().readTree(event);

        if (!SparkBatchSchemaUtil.hasSchema(jsonNode)) {
            return null;
        }
        return SparkBatchSchemaUtil.getSparkDfSchema(jsonNode.get("schema"));
    }

    public static boolean hasSchema(String event) throws JsonProcessingException {
        JsonNode jsonNode = new ObjectMapper().readTree(event);
        return SparkBatchSchemaUtil.hasSchema(jsonNode);
    }

    public static boolean hasSchema(JsonNode jsonNode) {
        return jsonNode != null
                && jsonNode.has("schema")
                && jsonNode.get("schema").has("fields")
                && jsonNode.get("schema").get("fields").isArray();
    }

}
