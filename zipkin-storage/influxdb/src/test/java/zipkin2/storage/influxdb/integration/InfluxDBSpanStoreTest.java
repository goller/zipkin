/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.influxdb.integration;

import org.junit.Before;
import zipkin.internal.V2StorageComponent;
import zipkin.storage.SpanStoreTest;
import zipkin.storage.StorageComponent;
import zipkin2.storage.influxdb.InfluxDBStorage;

abstract class InfluxDBSpanStoreTest extends SpanStoreTest {

  abstract protected String database();

  protected final InfluxDBStorage v2Storage() {
    return storage;
  }

  private InfluxDBStorage storage;

  @Before public void connect() {
    storage = storageBuilder().database(database()).build();
  }

  protected abstract InfluxDBStorage.Builder storageBuilder();

  @Override protected final StorageComponent storage() {
    return V2StorageComponent.create(storage);
  }
}
