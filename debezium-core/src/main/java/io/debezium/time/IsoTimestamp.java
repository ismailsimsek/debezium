/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java time representations into the {@link SchemaBuilder#string() STRING} representation of
 * the time and date in a particular time zone, and for defining a Kafka Connect {@link Schema} for zoned timestamp values.
 * <p>
 * The ISO date-time format includes the date, time (including fractional parts), and offset from UTC, such as
 * '2011-12-03T10:15:30+01:00'.
 *
 * @author Randall Hauch
 * @see Timestamp
 * @see MicroTimestamp
 * @see NanoTimestamp
 * @see ZonedTime
 */
public class IsoTimestamp {
    public static final String SCHEMA_NAME = "io.debezium.time.IsoTimestamp";
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    /**
     * Returns a {@link SchemaBuilder} for a {@link IsoTimestamp}. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link IsoTimestamp} but with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    public static String toIsoString(Object value, TemporalAdjuster adjuster) {
        LocalDateTime dateTime;
        if (value instanceof Long) {
            dateTime = Instant.ofEpochMilli((long) value).atOffset(ZoneOffset.UTC).toLocalDateTime();
        }
        else {
            dateTime = Conversions.toLocalDateTime(value);
        }

        if (adjuster != null) {
            dateTime = dateTime.with(adjuster);
        }

        return dateTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
    }

    private IsoTimestamp() {
    }
}
