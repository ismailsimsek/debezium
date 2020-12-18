### Common parameters

**Common Batch Parameters**

```
debezium.sink.batch.row.limit = 2 number of rows to triger data upload
debezium.sink.batch.time.limit = 2 seconds interval to trigger data upload
debezium.sink.batch.objectkey.prefix = debezium-cdc-
```

@TODO object key mapper!

### Consumers


**s3**

```
debezium.sink.s3.region = S3_REGION
debezium.sink.s3.bucket.name = s3a://S3_BUCKET
debezium.sink.s3.endpointoverride = http://localhost:9000, default:'false'
debezium.sink.s3.credentials.profile = default:'default'
debezium.sink.s3.credentials.useinstancecred = false
```

**s3batch**

// @TODO only supports json events!

```
debezium.sink.batch.s3.region = S3_REGION
debezium.sink.batch.s3.bucket.name = s3a://S3_BUCKET
debezium.sink.batch.s3.endpointoverride = http://localhost:9000, default:'false'
debezium.sink.batch.s3.credentials.profile = default:'default'
debezium.sink.batch.s3.credentials.useinstancecred = false
io.debezium.server.batch.keymapper.ObjectKeyMapper = {io.debezium.server.batch.keymapper.DefaultObjectKeyMapper, io.debezium.server.batch.keymapper.TimeBasedDailyObjectKeyMapper, io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper}
```

**sparkbatch, SparkBatchChangeConsumer**

```
debezium.sink.sparkbatch.saveformat = {delta,iceberg,json,avro,parquet}
io.debezium.server.batch.keymapper.ObjectKeyMapper = {io.debezium.server.batch.keymapper.DefaultObjectKeyMapper, io.debezium.server.batch.keymapper.TimeBasedDailyObjectKeyMapper, io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper}
io.debezium.server.batch.batchwriter.
```

**iceberg**

WIP