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
package zipkin;

import java.util.Arrays;
import java.util.Locale;

import zipkin.internal.Nullable;
import zipkin.internal.Util;

import static zipkin.internal.Util.checkArgument;
import static zipkin.internal.Util.checkNotNull;

/**
 * Indicates the network context of a service recording an annotation with two exceptions.
 *
 * <p>When a BinaryAnnotation, and key is {@link Constants#CLIENT_ADDR} or {@link
 * Constants#SERVER_ADDR}, the endpoint indicates the source or destination of an RPC. This
 * exception allows zipkin to display network context of uninstrumented services, or clients such as
 * web browsers.
 */
public final class Endpoint {

  public static Endpoint create(String serviceName, int ipv4, int port) {
    return new Endpoint(serviceName, ipv4, null, port == 0 ? null : (short) (port & 0xffff));
  }

  public static Endpoint create(String serviceName, int ipv4) {
    return new Endpoint(serviceName, ipv4, null, null);
  }

  /**
   * Classifier of a source or destination in lowercase, such as "zipkin-server".
   *
   * <p>This is the primary parameter for trace lookup, so should be intuitive as possible, for
   * example, matching names in service discovery.
   *
   * <p>Conventionally, when the service name isn't known, service_name = "unknown". However, it is
   * also permissible to set service_name = "" (empty string). The difference in the latter usage is
   * that the span will not be queryable by service name unless more information is added to the
   * span with non-empty service name, e.g. an additional annotation from the server.
   *
   * <p>Particularly clients may not have a reliable service name at ingest. One approach is to set
   * service_name to "" at ingest, and later assign a better label based on binary annotations, such
   * as user agent.
   */
  public final String serviceName;

  /**
   * IPv4 endpoint address packed into 4 bytes or zero if unknown.
   *
   * <p>Ex for the IP 1.2.3.4, it would be {@code (1 << 24) | (2 << 16) | (3 << 8) | 4}
   *
   * @see java.net.Inet4Address#getAddress()
   */
  public final int ipv4;

  /**
   * IPv6 endpoint address packed into 16 bytes or null if unknown.
   *
   * @see java.net.Inet6Address#getAddress()
   * @since Zipkin 1.4
   */
  @Nullable
  public final byte[] ipv6;

  /**
   * Port of the IP's socket or null, if not known.
   *
   * <p>Note: this is to be treated as an unsigned integer, so watch for negatives.
   *
   * @see java.net.InetSocketAddress#getPort()
   */
  @Nullable
  public final Short port;

  Endpoint(String serviceName, int ipv4, byte[] ipv6, Short port) {
    this.serviceName = checkNotNull(serviceName, "serviceName").isEmpty() ? ""
        : serviceName.toLowerCase(Locale.ROOT);
    this.ipv4 = ipv4;
    this.ipv6 = ipv6;
    this.port = port;
  }

  public Builder toBuilder(){
    return new Builder(this);
  }

  public static Builder builder(){
    return new Builder();
  }

  public static final class Builder {
    private String serviceName;
    private Integer ipv4;
    private byte[] ipv6;
    private Short port;

    Builder() {
    }

    Builder(Endpoint source) {
      this.serviceName = source.serviceName;
      this.ipv4 = source.ipv4;
      this.ipv6 = source.ipv6;
      this.port = source.port;
    }

    /** @see Endpoint#serviceName */
    public Builder serviceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    /** @see Endpoint#ipv4 */
    public Builder ipv4(int ipv4) {
      this.ipv4 = ipv4;
      return this;
    }

    /** @see Endpoint#ipv6 */
    public Builder ipv6(byte[] ipv6) {
      if (ipv6 != null) {
        checkArgument(ipv6.length == 16, "ipv6 addresses are 16 bytes: " + ipv6.length);
        this.ipv6 = ipv6;
      }
      return this;
    }

    /** @see Endpoint#port */
    public Builder port(Short port) {
      if (port == null || port != 0) {
        this.port = port;
      }
      return this;
    }

    public Endpoint build() {
      return new Endpoint(serviceName, ipv4 == null ? 0 : ipv4, ipv6, port);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Endpoint) {
      Endpoint that = (Endpoint) o;
      return (this.serviceName.equals(that.serviceName))
          && (this.ipv4 == that.ipv4)
          && (Arrays.equals(this.ipv6, that.ipv6))
          && Util.equal(this.port, that.port);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= serviceName.hashCode();
    h *= 1000003;
    h ^= ipv4;
    h *= 1000003;
    h ^= Arrays.hashCode(ipv6);
    h *= 1000003;
    h ^= (port == null) ? 0 : port.hashCode();
    return h;
  }
}
