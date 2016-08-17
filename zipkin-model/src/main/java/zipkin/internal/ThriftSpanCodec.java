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
import zipkin.Span;
import zipkin.SpanCodec;

import static zipkin.internal.ThriftAdapter.read;
import static zipkin.internal.ThriftAdapter.write;

public final class ThriftSpanCodec implements SpanCodec {
  static final ThriftAdapter<Span> SPAN_ADAPTER = new ThriftSpanAdapter();
  static final ThriftAdapter<List<Span>> SPANS_ADAPTER = SPAN_ADAPTER.toListAdapter();

  @Override
  public Span readSpan(byte[] bytes) {
    return read(SPAN_ADAPTER, ByteBuffer.wrap(bytes));
  }

  @Override
  public byte[] writeSpan(Span value) {
    return write(SPAN_ADAPTER, value, new ThriftAdapter.ByteArrayOutputBuffer());
  }

  @Override
  public List<Span> readSpans(byte[] bytes) {
    return read(SPANS_ADAPTER, ByteBuffer.wrap(bytes));
  }

  @Override
  public byte[] writeSpans(List<Span> value) {
    return write(SPANS_ADAPTER, value, new ThriftAdapter.ByteArrayOutputBuffer());
  }
}
