/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch.batchwriter;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import com.google.common.collect.Iterables;
import io.debezium.server.batch.keymapper.ObjectKeyMapper;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class IcebergBatchRecordWriter extends AbstractBatchRecordWriter {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergBatchRecordWriter.class);

    public IcebergBatchRecordWriter(ObjectKeyMapper mapper) throws URISyntaxException {
        super(mapper);
        LOGGER.info("Starting Iceberg BatchRecordWriter({})", this.getClass().getName());
    }

    protected void uploadBatchFile(String destination) {
        // @TODO process records line by line loop
        // @TODO convert line to json -> then genericrecord!
        // @TODO get table from catalog!
        Integer batchId = map_batchid.get(destination);
        final String data = map_data.get(destination);
        String s3File = objectKeyMapper.map(destination, batchTime, batchId, saveFormat);
        final String fileName = bucket + "/" + s3File;
        LOGGER.debug("Uploading s3File With Spark destination:'{}' key:'{}'", destination, s3File);
        updateSparkSession();
        List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
        Dataset<String> ds = spark.createDataset(jsonData, Encoders.STRING());
        DataFrameReader dfReader = spark.read();
        // Read DF with Schema if schema exists
        if (!jsonData.isEmpty()) {
            this.setReaderSchema(dfReader, Iterables.getLast(jsonData));
            Dataset<Row> df = dfReader.json(ds);
            if (removeSchema && Arrays.asList(df.columns()).contains("payload")) {
                df = df.select("payload.*");
            }
            try {
                df.write().format(saveFormat).mode("append").save(fileName);
            }
            catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
                HadoopTables tables = new HadoopTables(this.spark.sparkContext().hadoopConfiguration());
                tables.create(SparkSchemaUtil.convert(df.schema()), fileName);
                LOGGER.debug("Created Table:{}", fileName);
                df.write().format(saveFormat).mode("append").save(fileName);
            }

        }
        // increment batch id
        map_batchid.put(destination, batchId + 1);
        // start new batch
        map_data.remove(destination);
        cdcDb.commit();
        LOGGER.debug("Upload Succeeded! destination:'{}' key:'{}'", destination, fileName);
    }

}
