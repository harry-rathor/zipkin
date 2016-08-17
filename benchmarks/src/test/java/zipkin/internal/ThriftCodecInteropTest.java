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

import com.twitter.zipkin.thriftjava.Annotation;
import com.twitter.zipkin.thriftjava.Endpoint;
import com.twitter.zipkin.thriftjava.Span;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;
import zipkin.SpanCodec;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin.Constants.SERVER_RECV;
import static zipkin.Constants.SERVER_SEND;

public class ThriftCodecInteropTest {
  TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
  TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

  @Test
  public void spanSerializationIsCompatible() throws UnknownHostException, TException {

    zipkin.Endpoint zipkinEndpoint = zipkin.Endpoint.builder()
        .serviceName("web")
        .ipv4(124 << 24 | 13 << 16 | 90 << 8 | 3)
        .ipv6(Inet6Address.getByName("2001:db8::c001").getAddress())
        .port((short) 80).build();

    zipkin.Span zipkinSpan = zipkin.Span.builder().traceId(1L).id(1L).name("get")
        .addAnnotation(zipkin.Annotation.create(1000, SERVER_RECV, zipkinEndpoint))
        .addAnnotation(zipkin.Annotation.create(1350, SERVER_SEND, zipkinEndpoint))
        .build();

    Endpoint thriftEndpoint = new Endpoint()
        .setService_name("web")
        .setIpv4(124 << 24 | 13 << 16 | 90 << 8 | 3)
        .setIpv6(Inet6Address.getByName("2001:db8::c001").getAddress())
        .setPort((short) 80);

    Span thriftSpan = new Span(1L, "get", 1L, asList(
        new Annotation(1000, SERVER_RECV).setHost(thriftEndpoint),
        new Annotation(1350, SERVER_SEND).setHost(thriftEndpoint)), asList());

    assertThat(serializer.serialize(thriftSpan))
        .isEqualTo(SpanCodec.THRIFT.writeSpan(zipkinSpan));

    assertThat(SpanCodec.THRIFT.writeSpan(zipkinSpan))
        .isEqualTo(serializer.serialize(thriftSpan));

    Span deserializedThrift = new Span();
    deserializer.deserialize(deserializedThrift, SpanCodec.THRIFT.writeSpan(zipkinSpan));
    assertThat(deserializedThrift)
        .isEqualTo(thriftSpan);

    assertThat(SpanCodec.THRIFT.readSpan(serializer.serialize(thriftSpan)))
        .isEqualTo(zipkinSpan);
  }
}
