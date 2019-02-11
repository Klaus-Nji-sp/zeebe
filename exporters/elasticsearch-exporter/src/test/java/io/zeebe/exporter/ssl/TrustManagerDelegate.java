/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter.ssl;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
import org.apache.http.ssl.TrustStrategy;

public class TrustManagerDelegate implements X509TrustManager {

  private final X509TrustManager manager;
  private final TrustStrategy strategy;

  public TrustManagerDelegate(X509TrustManager manager, TrustStrategy strategy) {
    this.manager = manager;
    this.strategy = strategy;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] x509Certificates, String authType)
      throws CertificateException {
    strategy.isTrusted(x509Certificates, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] x509Certificates, String authType)
      throws CertificateException {
    strategy.isTrusted(x509Certificates, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return manager.getAcceptedIssuers();
  }
}