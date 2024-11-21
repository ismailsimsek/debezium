/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.util.Testing;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Random;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test to verify postgres money types defined in public schema.
 *
 * @author Harvey Yue
 */
public class PostgresTemporalPrecisionHandlingIT extends AbstractAsyncEngineConnectorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresTemporalPrecisionHandlingIT.class);

  static String TOPIC_NAME =  topicName("isostring.test_data_types");
  final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
      .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
      .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "isostring")
      //.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
      .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ISOSTRING)
      .build());

  @BeforeClass
  public static void beforeClass() throws SQLException {
    TestHelper.dropAllSchemas();
  }

  @Before
  public void before() {
    initializeConnectorTestFramework();
    TestHelper.execute("DROP SCHEMA IF EXISTS isostring CASCADE;");
    createTable();
  }

  @After
  public void after() {
    stopConnector();
    TestHelper.dropDefaultReplicationSlot();
    TestHelper.dropPublication();

    TestHelper.execute("DROP SCHEMA IF EXISTS isostring CASCADE;");
  }


  public void createTable() {
    TestHelper.execute("CREATE SCHEMA IF NOT EXISTS isostring ;");
    TestHelper.execute("""
                      CREATE TABLE IF NOT EXISTS isostring.test_data_types
                      (
                          c_id INTEGER             ,
                          c_json JSON              ,
                          c_jsonb JSONB            ,
                          c_date DATE              ,
                          c_timestamp0 TIMESTAMP(0),
                          c_timestamp1 TIMESTAMP(1),
                          c_timestamp2 TIMESTAMP(2),
                          c_timestamp3 TIMESTAMP(3),
                          c_timestamp4 TIMESTAMP(4),
                          c_timestamp5 TIMESTAMP(5),
                          c_timestamp6 TIMESTAMP(6),
                          c_timestamptz TIMESTAMPTZ,
                          c_time TIME WITH TIME ZONE,
                          c_time_whtz TIME WITHOUT TIME ZONE,
                          c_interval INTERVAL,
                          PRIMARY KEY(c_id)
                      ) ;
                  ALTER TABLE isostring.test_data_types REPLICA IDENTITY FULL;
        """);
    Object v = """
        INSERT INTO
           isostring.test_data_types\\s
        VALUES
           (1 , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),\\s
           (2 , '{"jfield": 111}'::json , '{"jfield": 211}'::jsonb , '2017-09-15'::DATE , '2019-07-09 02:28:57+01' , '2019-07-09 02:28:57.1+01' , '2019-07-09 02:28:57.12+01' , '2019-07-09 02:28:57.123+01' , '2019-07-09 02:28:57.1234+01' , '2019-07-09 02:28:57.12345+01' , '2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:10.123456+01', '04:05:11 PST', '04:05:11.789', INTERVAL '1' YEAR ),\\s
           (3 , '{"jfield": 222}'::json , '{"jfield": 222}'::jsonb , '2017-02-10'::DATE , '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:20.666666+01', '04:10:22', '04:05:22.789', INTERVAL '10' DAY )
        ;""";


  }

  public Struct getAfter(SourceRecord record){
    return  ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
  }

  @Test
  @FixFor("DBZ-997")
  public void shouldprocessIsoString() throws Exception {
    Testing.Print.disable();
    start(PostgresConnector.class, config.getConfig());
    assertConnectorIsRunning();

    // wait for snapshot completion
    TestHelper.execute("""
        INSERT INTO isostring.test_data_types 
        VALUES (1 , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL );""");

    SourceRecords records = consumeRecordsByTopic(1);
    SourceRecord insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
    assertEquals(TOPIC_NAME, insertRecord.topic());
    VerifyRecord.isValidInsert(insertRecord, "c_id", 1);
    LOGGER.error("{}",insertRecord.key());
    LOGGER.error("{}",getAfter(insertRecord));

    TestHelper.execute("""
        INSERT INTO isostring.test_data_types
        VALUES (2 , '{"jfield": 111}'::json , '{"jfield": 211}'::jsonb , '2017-09-15'::DATE , '2019-07-09 02:28:57+01' , '2019-07-09 02:28:57.1+01' , '2019-07-09 02:28:57.12+01' , '2019-07-09 02:28:57.123+01' , '2019-07-09 02:28:57.1234+01' , '2019-07-09 02:28:57.12345+01' , '2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:10.123456+01', '04:05:11 PST', '04:05:11.789', INTERVAL '1' YEAR )
        ;""");

    records = consumeRecordsByTopic(1);
    insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
    assertEquals(TOPIC_NAME, insertRecord.topic());
    VerifyRecord.isValidInsert(insertRecord, "c_id", 2);
    LOGGER.error("{}",insertRecord.key());
    LOGGER.error("{}",getAfter(insertRecord));
//
//    TestHelper.execute(
//        "ALTER TABLE changepk.test_table ADD COLUMN pk2 SERIAL;"
//            + "ALTER TABLE changepk.test_table DROP CONSTRAINT test_table_pkey;"
//            + "ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk,pk2);"
//            + "INSERT INTO changepk.test_table VALUES(3, 'newpkcol', 8)");
//    records = consumeRecordsByTopic(1);
//
//    insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
//    assertEquals(TOPIC_NAME, insertRecord.topic());
//    VerifyRecord.isValidInsert(insertRecord, newPkField, 3);
//    VerifyRecord.isValidInsert(insertRecord, "pk2", 8);

    stopConnector();

//    // De-synchronize JDBC PK info and decoded event schema
//    TestHelper.execute("INSERT INTO changepk.test_table VALUES(4, 'newpkcol', 20)");
//    TestHelper.execute(
//        "ALTER TABLE changepk.test_table DROP CONSTRAINT test_table_pkey;"
//            + "ALTER TABLE changepk.test_table DROP COLUMN pk2;"
//            + "ALTER TABLE changepk.test_table ADD COLUMN pk3 SERIAL;"
//            + "ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk,pk3);"
//            + "INSERT INTO changepk.test_table VALUES(5, 'dropandaddpkcol',10)");
//
//    start(PostgresConnector.class, config.getConfig());
//
//    records = consumeRecordsByTopic(2);
//
//    insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
//    assertEquals(TOPIC_NAME, insertRecord.topic());
//    VerifyRecord.isValidInsert(insertRecord, newPkField, 4);
//    Struct key = (Struct) insertRecord.key();
//    // The problematic record PK info is temporarily desynced
//    assertThat(key.schema().field("pk2")).isNull();
//    assertThat(key.schema().field("pk3")).isNull();
//
//    insertRecord = records.recordsForTopic(TOPIC_NAME).get(1);
//    assertEquals(TOPIC_NAME, insertRecord.topic());
//    VerifyRecord.isValidInsert(insertRecord, newPkField, 5);
//    VerifyRecord.isValidInsert(insertRecord, "pk3", 10);
//    key = (Struct) insertRecord.key();
//    assertThat(key.schema().field("pk2")).isNull();
//    stopConnector();

  }

}
