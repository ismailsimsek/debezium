/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.debezium.server.s3.objectkeymapper;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import io.debezium.engine.ChangeEvent;

public class TimeBasedDailyObjectKeyMapper extends DefaultObjectKeyMapper {

    @Override
    public String map(ChangeEvent<Object, Object> record, LocalDateTime batchTime) {
        String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + "-" + UUID.randomUUID().toString() + "-" + record.hashCode() + "." + valueFormat;
        String partiton = "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
        return objectKeyPrefix + record.destination() + "/" + partiton + "/" + fname;
    }
}
