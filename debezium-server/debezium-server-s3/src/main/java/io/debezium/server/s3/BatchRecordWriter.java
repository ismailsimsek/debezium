/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;

public interface BatchRecordWriter {

    void append(String destination, JsonNode keyJson, JsonNode valueJson) throws IOException;

    void uploadBatch() throws IOException;

    void close() throws IOException;
}
