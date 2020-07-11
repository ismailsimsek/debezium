
## File based s3batch consumer
Another implementation of s3batch consumer

> **_NOTE:_**  ! Not used, replaced by mapdb (JsonMapDbBatchRecordWriter) implementation

File based event caching,
1. opens file per destination.
2. appends events to it.
3. closes and uploads files to s3 periodically.
