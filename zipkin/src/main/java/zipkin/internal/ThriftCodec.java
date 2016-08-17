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
import okio.Buffer;
import zipkin.Codec;
import zipkin.DependencyLink;
import zipkin.Span;
import zipkin.internal.ThriftAdapter.OutputBuffer;

import static zipkin.internal.ThriftAdapter.read;
import static zipkin.internal.ThriftAdapter.write;

/**
 * This is a hard-coded thrift codec, which allows us to include thrift marshalling in a minified
 * core jar. The hard coding not only keeps us with a single data-model, it also allows the minified
 * core jar free of SLFJ classes otherwise included in generated types.
 *
 * <p> This directly implements TBinaryProtocol so as to reduce dependencies and array duplication.
 * While reads internally use {@link ByteBuffer}, writes use {@link Buffer} as the latter can grow.
 */
public final class ThriftCodec implements Codec {
  static final ThriftAdapter<Span> SPAN_ADAPTER = new ThriftSpanAdapter();
  static final ThriftAdapter<List<Span>> SPANS_ADAPTER = SPAN_ADAPTER.toListAdapter();
  static final ThriftAdapter<List<List<Span>>> TRACES_ADAPTER = SPANS_ADAPTER.toListAdapter();

  /**
   * Added for DataStax Cassandra driver, which returns data in ByteBuffers. The implementation
   * takes care not to re-buffer the data.
   *
   * @throws {@linkplain IllegalArgumentException} if the span couldn't be decoded
   */
  public Span readSpan(ByteBuffer bytes) {
    return read(SPAN_ADAPTER, bytes);
  }

  @Override
  public Span readSpan(byte[] bytes) {
    return read(SPAN_ADAPTER, ByteBuffer.wrap(bytes));
  }

  @Override
  public byte[] writeSpan(Span value) {
    return write(SPAN_ADAPTER, value, new OkioOutputBuffer());
  }

  @Override
  public List<Span> readSpans(byte[] bytes) {
    return read(SPANS_ADAPTER, ByteBuffer.wrap(bytes));
  }

  @Override
  public byte[] writeSpans(List<Span> value) {
    return write(SPANS_ADAPTER, value, new OkioOutputBuffer());
  }

  @Override
  public byte[] writeTraces(List<List<Span>> value) {
    return write(TRACES_ADAPTER, value, new OkioOutputBuffer());
  }

  static final ThriftAdapter<DependencyLink> DEPENDENCY_LINK_ADAPTER = new ThriftAdapter<DependencyLink>() {

    final Field PARENT = new Field(TYPE_STRING, 1);
    final Field CHILD = new Field(TYPE_STRING, 2);
    final Field CALL_COUNT = new Field(TYPE_I64, 4);

    @Override
    public DependencyLink read(ByteBuffer bytes) {
      DependencyLink.Builder result = DependencyLink.builder();
      Field field;

      while (true) {
        field = Field.read(bytes);
        if (field.type == TYPE_STOP) break;

        if (field.isEqualTo(PARENT)) {
          result.parent(readUtf8(bytes));
        } else if (field.isEqualTo(CHILD)) {
          result.child(readUtf8(bytes));
        } else if (field.isEqualTo(CALL_COUNT)) {
          result.callCount(bytes.getLong());
        } else {
          skip(bytes, field.type);
        }
      }

      return result.build();
    }

    @Override
    public void write(DependencyLink value, OutputBuffer buffer) {
      PARENT.write(buffer);
      buffer.writeLengthPrefixedUtf8(value.parent);

      CHILD.write(buffer);
      buffer.writeLengthPrefixedUtf8(value.child);

      CALL_COUNT.write(buffer);
      buffer.writeLong(value.callCount);

      buffer.writeByte(TYPE_STOP);
    }

    @Override
    public String toString() {
      return "DependencyLink";
    }
  };

  static final ThriftAdapter<List<DependencyLink>> DEPENDENCY_LINKS_ADAPTER =
      DEPENDENCY_LINK_ADAPTER.toListAdapter();

  @Override
  public DependencyLink readDependencyLink(byte[] bytes) {
    return read(DEPENDENCY_LINK_ADAPTER, ByteBuffer.wrap(bytes));
  }

  @Override
  public byte[] writeDependencyLink(DependencyLink value) {
    return write(DEPENDENCY_LINK_ADAPTER, value, new OkioOutputBuffer());
  }

  /**
   * Added for DataStax Cassandra driver, which returns data in ByteBuffers. The implementation
   * takes care not to re-buffer the data.
   *
   * @throws {@linkplain IllegalArgumentException} if the links couldn't be decoded
   */
  public List<DependencyLink> readDependencyLinks(ByteBuffer bytes) {
    return read(DEPENDENCY_LINKS_ADAPTER, bytes);
  }

  @Override
  public List<DependencyLink> readDependencyLinks(byte[] bytes) {
    return read(DEPENDENCY_LINKS_ADAPTER, ByteBuffer.wrap(bytes));
  }

  @Override
  public byte[] writeDependencyLinks(List<DependencyLink> value) {
    return write(DEPENDENCY_LINKS_ADAPTER, value, new OkioOutputBuffer());
  }


  static final class OkioOutputBuffer implements OutputBuffer {
    private final Buffer buffer = new Buffer();

    @Override
    public void writeByte(int value) {
      buffer.writeByte(value);
    }

    @Override
    public void writeShort(int v) {
      buffer.writeShort(v);
    }

    @Override
    public void writeInt(int v) {
      buffer.writeInt(v);
    }

    @Override
    public void writeLong(long v) {
      buffer.writeLong(v);
    }

    @Override
    public void writeLengthPrefixedUtf8(String value) {
      Buffer temp = new Buffer().writeUtf8(value);
      buffer.writeInt((int) temp.size());
      buffer.write(temp, temp.size());
    }

    @Override
    public void write(byte[] value) {
      buffer.write(value);
    }

    @Override
    public byte[] readByteArray() {
      return buffer.readByteArray();
    }
  }
}
