/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.debezium.server.s3.objectkeymapper;

import io.debezium.engine.format.Json;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.ConfigProvider;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DefaultBatchObjectKeyMapper implements BatchObjectKeyMapper {
    final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.s3.objectkey.prefix", String.class);
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    public String map(String destination, LocalDateTime batchTime) {
        String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + "." + valueFormat;
        String partiton = "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
        return objectKeyPrefix + destination + "/" + partiton + "/" + fname;
    }
}
