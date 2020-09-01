/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3.batchwriter.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.server.s3.batchwriter.AbstractBatchRecordWriter;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SparkBatchSchemaUtil {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchRecordWriter.class);

    // @TODO recursively process struct type.
    public static StructType getSparkDfSchema(String event) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jEvent = mapper.readTree(event);

        if (jEvent == null
                || !jEvent.has("schema")
                || !jEvent.get("schema").has("fields")
                || !jEvent.get("schema").get("fields").isArray()) {
            return null;
        }
        org.apache.spark.sql.types.StructField[] fields = new StructField[]{};
        List<StructField> sparkSchemaFields = new ArrayList<StructField>();
        for (JsonNode jsonSchemaFieldNode : jEvent.get("schema").get("fields")) {
            String fieldName = jsonSchemaFieldNode.get("field").textValue();
            String fieldType = jsonSchemaFieldNode.get("type").textValue();

            // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
            switch (fieldType) {
                case "int8":
                case "int16":
                case "int32":
                case "int64":
                    sparkSchemaFields.add(new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()));
                    break;
                case "float8":
                case "float16":
                case "float32":
                case "float64":
                    sparkSchemaFields.add(new StructField(fieldName, DataTypes.FloatType, true, Metadata.empty()));
                    break;
                case "boolean":
                    sparkSchemaFields.add(new StructField(fieldName, DataTypes.BooleanType, true, Metadata.empty()));
                    break;
                case "string":
                    sparkSchemaFields.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
                    break;
                case "bytes":
                    sparkSchemaFields.add(new StructField(fieldName, DataTypes.ByteType, true, Metadata.empty()));
                    break;
                case "array":
                    sparkSchemaFields.add(new StructField(fieldName, new ArrayType(), true, Metadata.empty()));
                    break;
                case "map":
                    sparkSchemaFields.add(new StructField(fieldName, new MapType(), true, Metadata.empty()));
                    break;
                case "struct":
                    sparkSchemaFields.add(new StructField(fieldName, new StructType(), true, Metadata.empty()));
                    break;
                default:
                    sparkSchemaFields.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
                    break;
            }
        }

        return new StructType(sparkSchemaFields.toArray(new StructField[0]));
    }
}
