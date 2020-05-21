/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.debezium.server.s3.batchwriter;

import io.debezium.engine.ChangeEvent;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;

public interface BatchRecordWriter {

    public void append(String objectKey, ChangeEvent<Object, Object> record) throws IOException;

    public void upload(S3Client s3Client, String bucket) throws IOException;

    public void remove() throws IOException;

}
