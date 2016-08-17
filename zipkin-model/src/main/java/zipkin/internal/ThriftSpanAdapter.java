/**
 * Copyright 2015-2016 The OpenZipkin Authors
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
package zipkin.internal;

import java.nio.ByteBuffer;
import java.util.List;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Endpoint;
import zipkin.Span;

public final class ThriftSpanAdapter extends ThriftAdapter<Span> {

  static final ThriftAdapter<Endpoint> ENDPOINT_ADAPTER = new ThriftAdapter<Endpoint>() {

    final Field IPV4 = new Field(TYPE_I32, 1);
    final Field PORT = new Field(TYPE_I16, 2);
    final Field SERVICE_NAME = new Field(TYPE_STRING, 3);
    final Field IPV6 = new Field(TYPE_STRING, 4);

    @Override
    public Endpoint read(ByteBuffer bytes) {
      Endpoint.Builder result = Endpoint.builder();
      Field field;

      while (true) {
        field = Field.read(bytes);
        if (field.type == TYPE_STOP) break;

        if (field.isEqualTo(IPV4)) {
          result.ipv4(bytes.getInt());
        } else if (field.isEqualTo(PORT)) {
          result.port(bytes.getShort());
        } else if (field.isEqualTo(SERVICE_NAME)) {
          result.serviceName(readUtf8(bytes));
        } else if (field.isEqualTo(IPV6)) {
          result.ipv6(readByteArray(bytes));
        } else {
          skip(bytes, field.type);
        }
      }
      return result.build();
    }

    @Override
    public void write(Endpoint value, OutputBuffer buffer) {
      IPV4.write(buffer);
      buffer.writeInt(value.ipv4);

      PORT.write(buffer);
      buffer.writeShort(value.port == null ? 0 : value.port);

      SERVICE_NAME.write(buffer);
      buffer.writeLengthPrefixedUtf8(value.serviceName);

      if (value.ipv6 != null) {
        IPV6.write(buffer);
        assert value.ipv6.length == 16;
        buffer.writeInt(value.ipv6.length);
        buffer.write(value.ipv6);
      }

      buffer.writeByte(TYPE_STOP);
    }
  };

  static final ThriftAdapter<Annotation> ANNOTATION_ADAPTER = new ThriftAdapter<Annotation>() {

    final Field TIMESTAMP = new Field(TYPE_I64, 1);
    final Field VALUE = new Field(TYPE_STRING, 2);
    final Field ENDPOINT = new Field(TYPE_STRUCT, 3);

    @Override
    public Annotation read(ByteBuffer bytes) {
      Annotation.Builder result = Annotation.builder();
      Field field;
      while (true) {
        field = Field.read(bytes);
        if (field.type == TYPE_STOP) break;

        if (field.isEqualTo(TIMESTAMP)) {
          result.timestamp(bytes.getLong());
        } else if (field.isEqualTo(VALUE)) {
          result.value(readUtf8(bytes));
        } else if (field.isEqualTo(ENDPOINT)) {
          result.endpoint(ENDPOINT_ADAPTER.read(bytes));
        } else {
          skip(bytes, field.type);
        }
      }
      return result.build();
    }

    @Override
    public void write(Annotation value, OutputBuffer buffer) {
      TIMESTAMP.write(buffer);
      buffer.writeLong(value.timestamp);

      if (value.value != null) {
        VALUE.write(buffer);
        buffer.writeLengthPrefixedUtf8(value.value);
      }

      if (value.endpoint != null) {
        ENDPOINT.write(buffer);
        ENDPOINT_ADAPTER.write(value.endpoint, buffer);
      }
      buffer.writeByte(TYPE_STOP);
    }
  };
  static final ThriftAdapter<BinaryAnnotation> BINARY_ANNOTATION_ADAPTER =
      new ThriftAdapter<BinaryAnnotation>() {

        final Field KEY = new Field(TYPE_STRING, 1);
        final Field VALUE = new Field(TYPE_STRING, 2);
        final Field TYPE = new Field(TYPE_I32, 3);
        final Field ENDPOINT = new Field(TYPE_STRUCT, 4);

        @Override
        public BinaryAnnotation read(ByteBuffer bytes) {
          BinaryAnnotation.Builder result = BinaryAnnotation.builder();
          Field field;

          while (true) {
            field = Field.read(bytes);
            if (field.type == TYPE_STOP) break;

            if (field.isEqualTo(KEY)) {
              result.key(readUtf8(bytes));
            } else if (field.isEqualTo(VALUE)) {
              result.value(readByteArray(bytes));
            } else if (field.isEqualTo(TYPE)) {
              result.type(BinaryAnnotation.Type.fromValue(bytes.getInt()));
            } else if (field.isEqualTo(ENDPOINT)) {
              result.endpoint(ENDPOINT_ADAPTER.read(bytes));
            } else {
              skip(bytes, field.type);
            }
          }
          return result.build();
        }

        @Override
        public void write(BinaryAnnotation value, OutputBuffer buffer) {
          KEY.write(buffer);
          buffer.writeLengthPrefixedUtf8(value.key);

          VALUE.write(buffer);
          buffer.writeInt(value.value.length);
          buffer.write(value.value);

          TYPE.write(buffer);
          buffer.writeInt(value.type.value);

          if (value.endpoint != null) {
            ENDPOINT.write(buffer);
            ENDPOINT_ADAPTER.write(value.endpoint, buffer);
          }

          buffer.writeByte(TYPE_STOP);
        }
      };
  static final ThriftAdapter<List<Annotation>> ANNOTATIONS_ADAPTER =
      ANNOTATION_ADAPTER.toListAdapter();
  static final ThriftAdapter<List<BinaryAnnotation>> BINARY_ANNOTATIONS_ADAPTER =
      BINARY_ANNOTATION_ADAPTER.toListAdapter();
  final Field TRACE_ID = new Field(TYPE_I64, 1);
  final Field NAME = new Field(TYPE_STRING, 3);
  final Field ID = new Field(TYPE_I64, 4);
  final Field PARENT_ID = new Field(TYPE_I64, 5);
  final Field ANNOTATIONS = new Field(TYPE_LIST, 6);
  final Field BINARY_ANNOTATIONS = new Field(TYPE_LIST, 8);
  final Field DEBUG = new Field(TYPE_BOOL, 9);
  final Field TIMESTAMP = new Field(TYPE_I64, 10);
  final Field DURATION = new Field(TYPE_I64, 11);

  @Override
  public Span read(ByteBuffer bytes) {
    Span.Builder result = Span.builder();
    Field field;

    while (true) {
      field = Field.read(bytes);
      if (field.type == TYPE_STOP) break;

      if (field.isEqualTo(TRACE_ID)) {
        result.traceId(bytes.getLong());
      } else if (field.isEqualTo(NAME)) {
        result.name(readUtf8(bytes));
      } else if (field.isEqualTo(ID)) {
        result.id(bytes.getLong());
      } else if (field.isEqualTo(PARENT_ID)) {
        result.parentId(bytes.getLong());
      } else if (field.isEqualTo(ANNOTATIONS)) {
        result.annotations(ANNOTATIONS_ADAPTER.read(bytes));
      } else if (field.isEqualTo(BINARY_ANNOTATIONS)) {
        result.binaryAnnotations(BINARY_ANNOTATIONS_ADAPTER.read(bytes));
      } else if (field.isEqualTo(DEBUG)) {
        result.debug(bytes.get() == 1);
      } else if (field.isEqualTo(TIMESTAMP)) {
        result.timestamp(bytes.getLong());
      } else if (field.isEqualTo(DURATION)) {
        result.duration(bytes.getLong());
      } else {
        skip(bytes, field.type);
      }
    }

    return result.build();
  }

  @Override
  public void write(Span value, OutputBuffer buffer) {

    TRACE_ID.write(buffer);
    buffer.writeLong(value.traceId);

    NAME.write(buffer);
    buffer.writeLengthPrefixedUtf8(value.name);

    ID.write(buffer);
    buffer.writeLong(value.id);

    if (value.parentId != null) {
      PARENT_ID.write(buffer);
      buffer.writeLong(value.parentId);
    }

    ANNOTATIONS.write(buffer);
    ANNOTATIONS_ADAPTER.write(value.annotations, buffer);

    BINARY_ANNOTATIONS.write(buffer);
    BINARY_ANNOTATIONS_ADAPTER.write(value.binaryAnnotations, buffer);

    if (value.debug != null) {
      DEBUG.write(buffer);
      buffer.writeByte(value.debug ? 1 : 0);
    }

    if (value.timestamp != null) {
      TIMESTAMP.write(buffer);
      buffer.writeLong(value.timestamp);
    }

    if (value.duration != null) {
      DURATION.write(buffer);
      buffer.writeLong(value.duration);
    }

    buffer.writeByte(TYPE_STOP);
  }

  @Override
  public String toString() {
    return "Span";
  }
}
