/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.debezium.server.s3.objectkeymapper;

import java.time.LocalDateTime;
import java.util.UUID;

import org.eclipse.microprofile.config.ConfigProvider;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.format.Json;

public class DefaultObjectKeyMapper implements ObjectKeyMapper {
    final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.s3.objectkey.prefix", String.class);
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    @Override
    public String map(ChangeEvent<Object, Object> record, LocalDateTime batchTime) {
        String fname = batchTime.toString() + "-" + UUID.randomUUID().toString() + "-" + record.hashCode() + "." + valueFormat;
        return objectKeyPrefix + record.destination() + "/" + fname;
    }
}
