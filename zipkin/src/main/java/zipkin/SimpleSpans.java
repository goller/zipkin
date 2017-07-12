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
package zipkin;

import java.util.List;
import java.util.Map;
import zipkin.internal.SimpleSpanCodec;

import static zipkin.Constants.LOCAL_COMPONENT;

public final class SimpleSpans {

  static final int FLAG_CS = 1 << 0;
  static final int FLAG_SR = 1 << 1;
  static final int FLAG_SS = 1 << 2;
  static final int FLAG_CR = 1 << 3;
  static final int FLAG_LOCAL_ENDPOINT = 1 << 4;

  /** Serialize a span recorded from instrumentation into its binary form. */
  public static byte[] toJson(SimpleSpan span) {
    return SimpleSpanCodec.writeSpan(span);
  }

  /** Serialize a list of spans recorded from instrumentation into their binary form. */
  public static byte[] toJson(List<SimpleSpan> spans) {
    return SimpleSpanCodec.writeSpans(spans);
  }

  public static SimpleSpan fromJson(byte[] bytes) {
    return SimpleSpanCodec.readSpan(bytes);
  }

  public static List<SimpleSpan> fromJsonList(byte[] bytes) {
    return SimpleSpanCodec.readSpans(bytes);
  }

  public static List<SimpleSpan> toSimpleSpans(List<Span> in) {
    // TODO
    return null;
  }

  public static Span toSpan(SimpleSpan in) {
    Span.Builder result = Span.builder()
      .traceIdHigh(in.traceIdHigh())
      .traceId(in.traceId())
      .parentId(in.parentId())
      .id(in.id())
      .debug(in.debug())
      .name(in.name() == null ? "" : in.name()); // avoid a NPE
    Endpoint endpoint = in.localEndpoint();
    Endpoint remoteEndpoint = in.remoteEndpoint();
    SimpleSpan.Kind kind = in.kind();
    int flags = 0;

    for (int i = 0, length = in.annotations().size(); i < length; i++) {
      Annotation a = in.annotations().get(i);
      String value = a.value;
      result.addAnnotation(Annotation.create(a.timestamp, value, endpoint));
      flags |= FLAG_LOCAL_ENDPOINT;
      if (value.length() != 2) continue;
      if (value.equals(Constants.CLIENT_SEND)) {
        flags |= FLAG_CS;
        kind = SimpleSpan.Kind.CLIENT;
      } else if (value.equals(Constants.SERVER_RECV)) {
        flags |= FLAG_SR;
        kind = SimpleSpan.Kind.SERVER;
      } else if (value.equals(Constants.SERVER_SEND)) {
        flags |= FLAG_SS;
        kind = SimpleSpan.Kind.SERVER;
      } else if (value.equals(Constants.CLIENT_RECV)) {
        flags |= FLAG_CR;
        kind = SimpleSpan.Kind.CLIENT;
      }
    }

    for (Map.Entry<String, String> tag : in.tags().entrySet()) {
      result.addBinaryAnnotation(BinaryAnnotation.create(tag.getKey(), tag.getValue(), endpoint));
      flags |= FLAG_LOCAL_ENDPOINT;
    }

    long startTimestamp = in.startTimestamp() == null ? 0L : in.startTimestamp();
    Long finishTimestamp = in.finishTimestamp();
    if (startTimestamp != 0) {
      result.timestamp(startTimestamp);
      if (finishTimestamp != null) {
        result.duration(Math.max(finishTimestamp - startTimestamp, 1));
      }
    }

    if (kind != null) {
      String remoteEndpointType;
      String startAnnotation;
      String finishAnnotation;
      switch (kind) {
        case CLIENT:
          remoteEndpointType = Constants.SERVER_ADDR;
          startAnnotation = (flags & FLAG_CS) == 0 ? Constants.CLIENT_SEND : null;
          finishAnnotation = (flags & FLAG_CR) == 0 ? Constants.CLIENT_RECV : null;
          break;
        case SERVER:
          remoteEndpointType = Constants.CLIENT_ADDR;
          startAnnotation = (flags & FLAG_SR) == 0 ? Constants.SERVER_RECV : null;
          finishAnnotation = (flags & FLAG_SS) == 0 ? Constants.SERVER_SEND : null;
          break;
        default:
          throw new AssertionError("update kind mapping");
      }
      if (remoteEndpoint != null) {
        result.addBinaryAnnotation(BinaryAnnotation.address(remoteEndpointType, remoteEndpoint));
      }
      if (startAnnotation != null && startTimestamp != 0) {
        if (startAnnotation.equals(Constants.SERVER_RECV)) flags |= FLAG_SR;
        if (startAnnotation.equals(Constants.CLIENT_SEND)) flags |= FLAG_CS;
        result.addAnnotation(Annotation.create(startTimestamp, startAnnotation, endpoint));
      }
      if (finishAnnotation != null && finishTimestamp != null) {
        result.addAnnotation(Annotation.create(finishTimestamp, finishAnnotation, endpoint));
      }
      flags |= FLAG_LOCAL_ENDPOINT;
    }
    // don't report server-side timestamp on shared or incomplete spans
    if (in.shared() && (flags & FLAG_SR) != 0) {
      result.timestamp(null).duration(null);
    }
    // don't report client span.timestamp if unfinished.
    // This allows one-way to be modeled as span.kind(serverOrClient).start().flush()
    if ((flags & (FLAG_CS | FLAG_SR)) != 0 && finishTimestamp == null) {
      result.timestamp(null);
    }
    if ((flags & FLAG_LOCAL_ENDPOINT) == 0) { // create a small dummy annotation
      result.addBinaryAnnotation(BinaryAnnotation.create(LOCAL_COMPONENT, "", endpoint));
    }
    return result.build();
  }
}
