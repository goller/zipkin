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
package zipkin.storage.influxdb;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import zipkin2.CheckResult;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

@AutoValue
public abstract class InfluxDBStorage extends StorageComponent {

  public static Builder newBuilder() {
    return new $AutoValue_InfluxDBStorage.Builder()
      .url("http://localhost:8086")
      .database("zipkin")
      .measurement("zipkin")
      .username("root")
      .password("")
      .retentionPolicy("zipkin")
      .strictTraceId(true);
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  public static abstract class Builder extends StorageComponent.Builder {

    public abstract Builder url(String url);

    public abstract Builder database(String database);

    public abstract Builder measurement(String measurement);

    public abstract Builder username(String username);

    public abstract Builder password(String password);

    public abstract Builder retentionPolicy(String retentionPolicy);

    @Override public abstract Builder strictTraceId(boolean strictTraceId);

    public abstract InfluxDBStorage build();

    Builder() {
    }
  }

  abstract String url();

  abstract String database();

  abstract String measurement();

  abstract String username();

  abstract String password();

  abstract String retentionPolicy();

  abstract boolean strictTraceId();

  /** get and close are typically called from different threads */
  volatile boolean provisioned, closeCalled;

  @Override public SpanStore spanStore() {
    return new InfluxDBSpanStore(this);
  }

  @Override public SpanConsumer spanConsumer() {
    return new InfluxDBSpanConsumer(this);
  }

  @Override public CheckResult check() {
    try {
      get().ping();
      return CheckResult.OK;
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Memoized InfluxDB get() {
    InfluxDB result = InfluxDBFactory.connect(url(), username(), password());
    provisioned = true;
    return result;
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    if (provisioned) get().close();
    closeCalled = true;
  }
 @Override
  public Call<Void> accept(List<Span> spans) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    // TODO: spanName is optional
    String q = String.format("SELECT * FROM \"%s\" WHERE \"service_name\" = '%s' AND \"name\" = '%s' AND time < %dms AND time > %dms",
      this.measurement, request.serviceName(), request.spanName(), request.endTs(), request.endTs() - request.lookback());
    StringBuilder result = new StringBuilder();

    for (Iterator<Map.Entry<String, String>> i = request.annotationQuery().entrySet().iterator(); i.hasNext(); ) {
      Map.Entry<String, String> next = i.next();
      String k = next.getKey();
      String v = next.getValue();
      if (v.isEmpty()) {
        result.append(String.format("\"annotation_key\" = '%s'", k));
      } else {
        result.append(String.format("(\"annotation_key\" = '%s' AND \"annotation_value\" = '%s'", k, v));
      }
      if (i.hasNext())
        result.append(" and ");
    }
    if (result.length() > 0) {
      q += result.toString();
    }

    q += String.format(" AND \"duration\" >= %d ", request.minDuration());
    q += String.format(" AND \"duration\" <= %d ", request.maxDuration());
    q += " GROUP BY \"trace_id\" ";
    q += String.format(" SLIMIT %d ORDER BY time DESC", request.limit());

    Query query = new Query(q, this.database);
    QueryResult qresult = this.influx.query(query);
    throw new UnsupportedOperationException();
  }

  @Override
  public Call<List<String>> getServiceNames() {
    String queryStr = String.format("SHOW TAG VALUES FROM \"%s\" WITH KEY = \"service_name\"", this.measurement);
    Query query = new Query(queryStr, this.database);
    QueryResult result = this.influx.query(query);
    throw new UnsupportedOperationException();
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    String q = String.format("SELECT COUNT(\"duration\") FROM \"%s\"", this.measurement);
    q += String.format(" WHERE time < %dms", endTs);
    q += String.format(" AND time > %dms ", endTs - lookback);
    q += "GROUP BY \"id\",\"parent_id\",time(1d)";
    Query query = new Query(q, this.database);
    QueryResult result = this.influx.query(query);
    throw new UnsupportedOperationException();
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    String q = String.format("SHOW TAG VALUES FROM \"%s\" with key=\"name\" WHERE \"service_name\" = '%s'", this.measurement, serviceName);
    Query query = new Query(q, this.database);
    QueryResult result = this.influx.query(query);
    List<List<Object>> retentionPolicies = result.getResults().get(0).getSeries().get(0).getValues();
    throw new UnsupportedOperationException();
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    // SELECT * from zipkin where "trace_id"='traceId'
    String q = String.format("SELECT * FROM \"%s\" WHERE \"trace_id\" = '%s'", this.measurement, traceId);
    Query query = new Query(q, this.database);
    QueryResult result = this.influx.query(query);
    throw new UnsupportedOperationException();
  }
  
  InfluxDBStorage() {
  }
}
