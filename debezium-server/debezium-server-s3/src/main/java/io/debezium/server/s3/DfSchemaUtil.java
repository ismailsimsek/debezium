/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.types.AbstractDataType;
import org.apache.spark.sql.types.DataType;

public class DfSchemaUtil {

    /**
     * Maps Schema.Types to a list of Java classes that can be used to represent them.
     */
    private static final Map<org.apache.kafka.connect.data.Schema.Type, DataType> SCHEMA_TYPE_CLASSES = new EnumMap<>(
            org.apache.kafka.connect.data.Schema.Type.class);
    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<AbstractDataType>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    /**
     * Maps the Java classes to the corresponding Schema.Type.
     */
    private static final Map<Class<?>, org.apache.kafka.connect.data.Schema.Type> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.INT8, IntegerType);
        SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.INT16, IntegerType);
        SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.INT32, IntegerType);
        SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.INT64, IntegerType);
        /*
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.FLOAT32, Collections.singletonList((Class) Float.class));
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.FLOAT64, Collections.singletonList((Class) Double.class));
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.BOOLEAN, Collections.singletonList((Class) Boolean.class));
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.STRING, Collections.singletonList((Class) String.class));
         * // Bytes are special and have 2 representations. byte[] causes problems because it doesn't handle equals() and
         * // hashCode() like we want objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause
         * // those methods to fail, so ByteBuffers are recommended
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.BYTES, Arrays.asList((Class) byte[].class, (Class) ByteBuffer.class));
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.ARRAY, Collections.singletonList((Class) List.class));
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.MAP, Collections.singletonList((Class) Map.class));
         * SCHEMA_TYPE_CLASSES.put(org.apache.kafka.connect.data.Schema.Type.STRUCT, Collections.singletonList((Class) Struct.class));
         *
         * for (Map.Entry<org.apache.kafka.connect.data.Schema.Type, List<Class>> schemaClasses : SCHEMA_TYPE_CLASSES.entrySet()) {
         * for (Class<?> schemaClass : schemaClasses.getValue())
         * JAVA_CLASS_SCHEMA_TYPES.put(schemaClass, schemaClasses.getKey());
         * }
         *
         * LOGICAL_TYPE_CLASSES.put(Decimal.LOGICAL_NAME, Collections.singletonList((Class) BigDecimal.class));
         * LOGICAL_TYPE_CLASSES.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, Collections.singletonList((Class) Date.class));
         * LOGICAL_TYPE_CLASSES.put(Time.LOGICAL_NAME, Collections.singletonList((Class) Date.class));
         * LOGICAL_TYPE_CLASSES.put(Timestamp.LOGICAL_NAME, Collections.singletonList((Class) Date.class));
         * // We don't need to put these into JAVA_CLASS_SCHEMA_TYPES since that's only used to determine schemas for
         * // schemaless data and logical types will have ambiguous schemas (e.g. many of them use the same Java class) so
         * // they should not be used without schemas.
         */
    }
}
