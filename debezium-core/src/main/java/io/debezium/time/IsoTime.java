/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java time representations into the {@link SchemaBuilder#string() STRING} representation of
 * the time in a particular time zone, and for defining a Kafka Connect {@link Schema} for zoned time values.
 * <p>
 * The ISO date-time format includes the time (including fractional parts) and offset from UTC, such as
 * '10:15:30+01:00'.
 *
 * @author Randall Hauch
 * @see Date
 * @see Time
 * @see Timestamp
 * @see IsoTime
 * @see ZonedTimestamp
 */
public class IsoTime {

    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_TIME;
    public static final String SCHEMA_NAME = "io.debezium.time.IsoTime";
    private static final Duration ONE_DAY = Duration.ofDays(1);

    /**
     * Returns a {@link SchemaBuilder} for a {@link IsoTime}. You can use the resulting SchemaBuilder
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
     * Returns a Schema for a {@link IsoTime} but with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    public static String toIsoString(Object value, boolean acceptLargeValues) {
        if (value instanceof Duration) {
            Duration duration = (Duration) value;
            if (!acceptLargeValues && (duration.isNegative() || duration.compareTo(ONE_DAY) > 0)) {
                throw new IllegalArgumentException("Time values must be between 00:00:00 and 24:00:00 (inclusive): " + duration);
            }
            // Base LocalTime (e.g., 0:00 AM)
            // Calculate the new LocalTime by adding the duration to the base time
            LocalTime localTime = LocalTime.of(0, 0).plus(duration);
            return localTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
        }

        LocalTime localTime = Conversions.toLocalTime(value);
        return localTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
    }

    private IsoTime() {
    }
}
