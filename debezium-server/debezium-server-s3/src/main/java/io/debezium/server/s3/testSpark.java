/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testSpark {

    public static void main(String[] args) {
        Logger LOGGER = LoggerFactory.getLogger(testSpark.class);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.io.compression.codec", "snappy")
                .getOrCreate();
        tesschema2(spark);
        /*
         * String data =
         * "{ \"schema\":{ \"type\":\"struct\", \"fields\":[ { \"type\":\"int32\", \"optional\":false, \"field\":\"id\" }, { \"type\":\"int32\", \"optional\":false, \"name\":\"io.debezium.time.Date\", \"version\":1, \"field\":\"order_date\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"purchaser\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"quantity\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"product_id\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__op\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__table\" }, { \"type\":\"int64\", \"optional\":true, \"field\":\"__lsn\" }, { \"type\":\"int64\", \"optional\":true, \"field\":\"__source_ts_ms\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__deleted\" } ], \"optional\":false, \"name\":\"testc.inventory.orders.Value\" }, \"payload\":{ \"id\":10003, \"order_date\":16850, \"purchaser\":1002, \"quantity\":2, \"product_id\":106, \"__op\":\"r\", \"__table\":\"orders\", \"__lsn\":33832960, \"__source_ts_ms\":1596309876678, \"__deleted\":\"false\" } }"
         * ;
         * data +=
         * "\n{ \"schema\":{ \"type\":\"struct\", \"fields\":[ { \"type\":\"int32\", \"optional\":false, \"field\":\"id\" }, { \"type\":\"int32\", \"optional\":false, \"name\":\"io.debezium.time.Date\", \"version\":1, \"field\":\"order_date\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"purchaser\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"quantity\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"product_id\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__op\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__table\" }, { \"type\":\"int64\", \"optional\":true, \"field\":\"__lsn\" }, { \"type\":\"int64\", \"optional\":true, \"field\":\"__source_ts_ms\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__deleted\" } ], \"optional\":false, \"name\":\"testc.inventory.orders.Value\" }, \"payload\":{ \"id\":10003, \"order_date\":16850, \"purchaser\":1002, \"quantity\":2, \"product_id\":106, \"__op\":\"r\", \"__table\":\"orders\", \"__lsn\":33832960, \"__source_ts_ms\":1596309876678, \"__deleted\":\"false\" } }"
         * ;
         * data +=
         * "\n{ \"schema\":{ \"type\":\"struct\", \"fields\":[ { \"type\":\"int32\", \"optional\":false, \"field\":\"id\" }, { \"type\":\"int32\", \"optional\":false, \"name\":\"io.debezium.time.Date\", \"version\":1, \"field\":\"order_date\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"purchaser\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"quantity\" }, { \"type\":\"int32\", \"optional\":false, \"field\":\"product_id\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__op\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__table\" }, { \"type\":\"int64\", \"optional\":true, \"field\":\"__lsn\" }, { \"type\":\"int64\", \"optional\":true, \"field\":\"__source_ts_ms\" }, { \"type\":\"string\", \"optional\":true, \"field\":\"__deleted\" } ], \"optional\":false, \"name\":\"testc.inventory.orders.Value\" }, \"payload\":{ \"id\":10003, \"order_date\":16850, \"purchaser\":1002, \"quantity\":2, \"product_id\":106, \"__op\":\"r\", \"__table\":\"orders\", \"__lsn\":33832960, \"__source_ts_ms\":1596309876678, \"__deleted\":\"false\" } }"
         * ;
         * String data2 =
         * "{ \"id\":1003, \"first_name\":\"Edward\", \"last_name\":\"Walker\", \"email\":\"ed@walker.com\", \"__op\":\"r\", \"__table\":\"customers\", \"__lsn\":33832960, \"__source_ts_ms\":1596310608673, \"__deleted\":\"false\" }"
         * ;
         * data2 +=
         * "\n{ \"id\":1003, \"first_name\":\"Edward\", \"last_name\":\"Walker\", \"email\":\"ed@walker.com\", \"__op\":\"r\", \"__table\":\"customers\", \"__lsn\":33832960, \"__source_ts_ms\":1596310608673, \"__deleted\":\"false\" }"
         * ;
         * data2 +=
         * "\n{ \"id\":1003, \"first_name\":\"Edward\", \"last_name\":\"Walker\", \"email\":\"ed@walker.com\", \"__op\":\"r\", \"__table\":\"customers\", \"__lsn\":33832960, \"__source_ts_ms\":1596310608673, \"__deleted\":\"false\" }"
         * ;
         *
         * List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
         * Dataset<String> _df = spark.createDataset(jsonData, Encoders.STRING());
         * JSONObject myschema = new JSONObject(_df.first());
         * LOGGER.error(myschema.toString());
         * Dataset<Row> df;
         * if (myschema.has("schema")) {
         * tesschema();
         * StructType mySchema = getDFSchema(myschema);
         * LOGGER.error(mySchema.toString());
         * df = spark.read().schema(mySchema).json(_df);
         * }
         * else {
         * df = spark.read().json(_df);
         * }
         *
         * df.printSchema();
         * // remove schema
         * if (true) {
         * df = df.select("payload.*");
         * }
         *
         * // df = getDf(df);
         * df.printSchema();
         * //System.out.println(df.count());
         * //System.out.println(df.describe());
         * //System.out.println(df.columns().toString());
         *
         */
    }

    private static void tesschema2(SparkSession spark) {

        StructType schemaUntyped = new StructType()
                .add("a", "int")
                .add("b", "string")
                .add("c", "string")
                .add("d", "string");

        String data = "{\"id\":100, \"first_name\":\"Edward\"}";
        data += "\n{\"id\":1001, \"first_name\":\"Edward\"}";
        data += "\n{\"id\":1001, \"first_name\":\"Edward\", \"first_name2\":\"Edwardoo\"}";

        List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
        Dataset<String> _df = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> df = spark.read().schema(schemaUntyped).json(_df);
        df.printSchema();
        System.out.println(df.count());
        System.out.println(df.describe());
        System.out.println(Arrays.toString(df.columns()));
        System.out.println("---------------------------------------------------------------");

    }

    private static void tesschema() {

        String s = "{\"name\":\"testc.inventory.orders.Value\",\"optional\":false,\"type\":\"struct\",\"fields\":[{\"field\":\"id\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"order_date\",\"name\":\"io.debezium.time.Date\",\"optional\":false,\"type\":\"int32\",\"version\":1},{\"field\":\"purchaser\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"quantity\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"product_id\",\"optional\":false,\"type\":\"int32\"},{\"field\":\"__op\",\"optional\":true,\"type\":\"string\"},{\"field\":\"__table\",\"optional\":true,\"type\":\"string\"},{\"field\":\"__lsn\",\"optional\":true,\"type\":\"int64\"},{\"field\":\"__source_ts_ms\",\"optional\":true,\"type\":\"int64\"},{\"field\":\"__deleted\",\"optional\":true,\"type\":\"string\"}]}\n";
        s = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"nullable\":false,\"type\":\"int\"},{\"name\":\"order_date\",\"nullable\":false,\"type\":\"int32\",\"version\":1},{\"name\":\"purchaser\",\"nullable\":false,\"type\":\"int32\"},{\"name\":\"quantity\",\"nullable\":false,\"type\":\"int32\"},{\"name\":\"product_id\",\"nullable\":false,\"type\":\"int32\"},{\"name\":\"__op\",\"nullable\":true,\"type\":\"string\"},{\"name\":\"__table\",\"nullable\":true,\"type\":\"string\"},{\"name\":\"__lsn\",\"nullable\":true,\"type\":\"int64\"},{\"name\":\"__source_ts_ms\",\"nullable\":true,\"type\":\"int64\"},{\"name\":\"__deleted\",\"nullable\":true,\"type\":\"string\"}]}\n";
        // schema = (StructType) DataType.fromJson(s);
        // System.out.println(schema);
    }

    private static StructType getDFSchema(JSONObject j) {

        StructType schema = new StructType();
        System.out.println(j.get("schema").toString());
        schema = (StructType) DataType.fromJson(j.get("schema").toString());
        /*
         * schema.add("City", StringType, true)
         * .add("Country", StringType, true)
         * .add("Decommisioned", BooleanType, true)
         * .add("EstimatedPopulation", LongType, true)
         * .add("Lat", DoubleType, true)
         * .add("Location", StringType, true)
         * .add("LocationText", StringType, true)
         * .add("LocationType", StringType, true)
         * .add("Long", DoubleType, true)
         * .add("Notes", StringType, true)
         * .add("RecordNumber", LongType, true)
         * .add("State", StringType, true)
         * .add("TaxReturnsFiled", LongType, true)
         * .add("TotalWages", LongType, true)
         * .add("WorldRegion", StringType, true)
         * .add("Xaxis", DoubleType, true)
         * .add("Yaxis", DoubleType, true)
         * .add("Zaxis", DoubleType, true)
         * .add("Zipcode", StringType, true)
         * .add("ZipCodeType", StringType, true)
         */
        return schema;
    }

    private static Dataset<Row> getDf(Dataset<Row> df) {
        Dataset<Row> f;
        Row schema = null;
        String schemastr = null;

        if (Arrays.asList(df.columns()).contains("payload")) {
            f = df.select("payload.*");
        }
        else {
            f = df;
        }
        f.printSchema();
        df.printSchema();
        df.write().mode("append").parquet("test.parquet");
        // df.col("").cast(IntegerType);
        System.out.println("---------------------------------------------------------------");
        JSONObject myschema = new JSONObject(df.first().json());
        System.out.println(myschema.has("schema"));
        System.out.println(myschema.has("payload"));
        System.out.println(myschema.toString());
        System.out.println("---------------------------------------------------------------");

        return df;
    }
}
