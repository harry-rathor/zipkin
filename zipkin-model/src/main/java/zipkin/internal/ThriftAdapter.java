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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static zipkin.internal.Util.UTF_8;
import static zipkin.internal.Util.checkArgument;

/**
 * This is a hard-coded thrift adapter, which allows us to include thrift marshalling in a minified
 * core jar. The hard coding not only keeps us with a single data-model, it also allows the minified
 * core jar free of SLFJ classes otherwise included in generated types.
 *
 * <p> This directly implements TBinaryProtocol so as to reduce dependencies and array duplication.
 * While reads internally use {@link ByteBuffer}, writes use {@link OutputBuffer} as the latter can
 * grow.
 */
abstract class ThriftAdapter<T> {

  // break vs decode huge structs, like > 1MB strings or 10k spans in a trace.
  static final int STRING_LENGTH_LIMIT = 1 * 1024 * 1024;
  static final int CONTAINER_LENGTH_LIMIT = 10 * 1000;
  // taken from org.apache.thrift.protocol.TType
  static final byte TYPE_STOP = 0;
  static final byte TYPE_BOOL = 2;
  static final byte TYPE_BYTE = 3;
  static final byte TYPE_DOUBLE = 4;
  static final byte TYPE_I16 = 6;
  static final byte TYPE_I32 = 8;
  static final byte TYPE_I64 = 10;
  static final byte TYPE_STRING = 11;
  static final byte TYPE_STRUCT = 12;
  static final byte TYPE_MAP = 13;
  static final byte TYPE_SET = 14;
  static final byte TYPE_LIST = 15;
  // break vs recursing infinitely when skipping data
  private static int MAX_SKIP_DEPTH = 2147483647;

  static <T> List<T> readList(ThriftAdapter<T> reader, ByteBuffer bytes) {
    byte ignoredType = bytes.get();
    int length = guardLength(bytes, CONTAINER_LENGTH_LIMIT);
    if (length == 0) return Collections.emptyList();
    if (length == 1) return Collections.singletonList(reader.read(bytes));
    List<T> result = new ArrayList<T>(length);
    for (int i = 0; i < length; i++) {
      result.add(reader.read(bytes));
    }
    return result;
  }

  static <T> void writeList(ThriftAdapter<T> writer, List<T> value, OutputBuffer buffer) {
    int length = value.size();
    writeListBegin(buffer, length);
    for (int i = 0; i < length; i++) {
      writer.write(value.get(i), buffer);
    }
  }

  static void skip(ByteBuffer bytes, byte type) {
    skip(bytes, type, MAX_SKIP_DEPTH);
  }

  static void skip(ByteBuffer bytes, byte type, int maxDepth) {
    if (maxDepth <= 0) throw new IllegalStateException("Maximum skip depth exceeded");
    switch (type) {
      case TYPE_BOOL:
      case TYPE_BYTE:
        skip(bytes, 1);
        break;
      case TYPE_I16:
        skip(bytes, 2);
        break;
      case TYPE_I32:
        skip(bytes, 4);
        break;
      case TYPE_DOUBLE:
      case TYPE_I64:
        skip(bytes, 8);
        break;
      case TYPE_STRING:
        int size = guardLength(bytes, STRING_LENGTH_LIMIT);
        skip(bytes, size);
        break;
      case TYPE_STRUCT:
        while (true) {
          Field field = Field.read(bytes);
          if (field.type == TYPE_STOP) return;
          skip(bytes, field.type, maxDepth - 1);
        }
      case TYPE_MAP:
        byte keyType = bytes.get();
        byte valueType = bytes.get();
        for (int i = 0, length = guardLength(bytes, CONTAINER_LENGTH_LIMIT); i < length; i++) {
          skip(bytes, keyType, maxDepth - 1);
          skip(bytes, valueType, maxDepth - 1);
        }
        break;
      case TYPE_SET:
      case TYPE_LIST:
        byte elemType = bytes.get();
        for (int i = 0, length = guardLength(bytes, CONTAINER_LENGTH_LIMIT); i < length; i++) {
          skip(bytes, elemType, maxDepth - 1);
        }
        break;
      default: // types that don't need explicit skipping
        break;
    }
  }

  static void skip(ByteBuffer bytes, int count) {
    bytes.position(bytes.position() + count);
  }

  static byte[] readByteArray(ByteBuffer bytes) {
    byte[] result = new byte[guardLength(bytes, STRING_LENGTH_LIMIT)];
    bytes.get(result);
    return result;
  }

  static String readUtf8(ByteBuffer bytes) {
    return new String(readByteArray(bytes), UTF_8);
  }

  static int guardLength(ByteBuffer bytes, int limit) {
    int length = bytes.getInt();
    if (length > limit) { // don't allocate massive arrays
      throw new IllegalStateException(length + " > " + limit + ": possibly malformed thrift");
    }
    return length;
  }

  static void writeListBegin(OutputBuffer buffer, int size) {
    buffer.writeByte(TYPE_STRUCT);
    buffer.writeInt(size);
  }

  static <T> T read(ThriftAdapter<T> reader, ByteBuffer bytes) {
    checkArgument(bytes.remaining() > 0, "Empty input reading %s", reader);
    try {
      return reader.read(bytes);
    } catch (RuntimeException e) {
      throw exceptionReading(reader.toString(), bytes, e);
    }
  }

  /**
   * Inability to encode is a programming bug.
   */
  static <T> byte[] write(ThriftAdapter<T> writer, T value, OutputBuffer buffer) {
    try {
      writer.write(value, buffer);
    } catch (RuntimeException e) {
      throw new AssertionError("Could not write " + value + " as TBinary: " + e.getMessage());
    }
    return buffer.readByteArray();
  }

  static IllegalArgumentException exceptionReading(String type, ByteBuffer bytes, Exception e) {
    String cause = e.getMessage() == null ? "Error" : e.getMessage();
    if (e instanceof EOFException) cause = "EOF";
    if (e instanceof IllegalStateException || e instanceof BufferUnderflowException) {
      cause = "Malformed";
    }
    String message = String.format("%s reading %s from TBinary: ", cause, type, bytes);
    throw new IllegalArgumentException(message, e);
  }

  public byte[] write(T value) {
    ByteArrayOutputBuffer result = new ByteArrayOutputBuffer();
    write(value, result);
    return result.readByteArray();
  }

  abstract void write(T value, OutputBuffer buffer);

  abstract T read(ByteBuffer bytes);

  ListAdapter<T> toListAdapter() {
    return new ListAdapter<T>(this);
  }

  interface OutputBuffer {

    void writeByte(int value);

    void writeShort(int value);

    void writeInt(int value);

    void writeLong(long value);

    void write(byte[] value);

    void writeLengthPrefixedUtf8(String value);

    byte[] readByteArray();
  }

  static final class ListAdapter<T> extends ThriftAdapter<List<T>> {
    final ThriftAdapter<T> adapter;

    ListAdapter(ThriftAdapter<T> adapter) {
      this.adapter = adapter;
    }

    @Override
    public List<T> read(ByteBuffer bytes) {
      return readList(adapter, bytes);
    }

    @Override
    public void write(List<T> value, OutputBuffer buffer) {
      writeList(adapter, value, buffer);
    }

    @Override
    public String toString() {
      return "List<" + adapter + ">";
    }
  }

  static final class Field {
    final byte type;
    final int id;

    Field(byte type, int id) {
      this.type = type;
      this.id = id;
    }

    static Field read(ByteBuffer bytes) {
      byte type = bytes.get();
      return new Field(type, type == TYPE_STOP ? TYPE_STOP : bytes.getShort());
    }

    void write(OutputBuffer buffer) {
      buffer.writeByte(type);
      buffer.writeShort(id);
    }

    boolean isEqualTo(Field that) {
      return this.type == that.type && this.id == that.id;
    }
  }

  static final class ByteArrayOutputBuffer implements OutputBuffer {
    private final ByteArrayOutputStream input = new ByteArrayOutputStream();

    @Override
    public void writeByte(int value) {
      input.write(value);
    }

    @Override
    public void writeShort(int v) {
      writeByte((v >>> 8L) & 0xff);
      writeByte(v & 0xff);
    }

    @Override
    public void writeInt(int v) {
      writeByte((v >>> 24L) & 0xff);
      writeByte((v >>> 16L) & 0xff);
      writeByte((v >>> 8L) & 0xff);
      writeByte(v & 0xff);
    }

    @Override
    public void writeLong(long v) {
      writeByte((int) ((v >>> 56L) & 0xff));
      writeByte((int) ((v >>> 48L) & 0xff));
      writeByte((int) ((v >>> 40L) & 0xff));
      writeByte((int) ((v >>> 32L) & 0xff));
      writeByte((int) ((v >>> 24L) & 0xff));
      writeByte((int) ((v >>> 16L) & 0xff));
      writeByte((int) ((v >>> 8L) & 0xff));
      writeByte((int) (v & 0xff));
    }

    @Override
    public void writeLengthPrefixedUtf8(String value) {
      byte[] temp = value.getBytes(Util.UTF_8);
      writeInt(temp.length);
      write(temp);
    }

    @Override
    public void write(byte[] value) {
      try {
        input.write(value);
      } catch (IOException e) {
        throw new AssertionError(e); // in-memory so no chance of IOE
      }
    }

    @Override
    public byte[] readByteArray() {
      return input.toByteArray();
    }
  }
}
