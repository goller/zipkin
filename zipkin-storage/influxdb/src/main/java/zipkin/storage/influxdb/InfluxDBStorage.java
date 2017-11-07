/**
 * Copyright 2016-2017 The OpenZipkin Authors
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

import org.influxdb.InfluxDB;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

import java.util.List;

public class InfluxDBStorage extends StorageComponent implements SpanStore, SpanConsumer {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true;
    String address;
    String password;
    String username;
    String retentionPolicy;
    String database;
    String measurement;

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    public Builder address(String address) {
      this.address = address;
      return this;
    }
    public Builder password(String password) {
      this.password = password;
      return this;
    }
    public Builder username(String username) {
      this.username = username;
      return this;
    }
    public Builder retentionPolicy(String retentionPolicy) {
      this.retentionPolicy = retentionPolicy;
      return this;
    }
    public Builder database(String database) {
      this.database = database;
      return this;
    }
    public Builder measurement(String measurement) {
      this.measurement = measurement;
      return this;
    }

    @Override
    public InfluxDBStorage build() {
      return new InfluxDBStorage();
    }

    Builder() {
    }
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

  @Override
  public SpanStore spanStore() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SpanConsumer spanConsumer() {
    throw new UnsupportedOperationException();
  }

  InfluxDBStorage() {
  }
}
