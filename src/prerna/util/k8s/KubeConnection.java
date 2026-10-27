/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.k8s;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Where a Kubernetes API server is and how to authenticate to it: the server
 * URL, a TLS context trusting the cluster's CA (and presenting the client
 * certificate, if any), and the bearer token, if any.
 */
public final class KubeConnection {

	/** Projected ServiceAccount tokens are rotated by the kubelet; re-read the file this often. */
	private static final Duration TOKEN_FILE_REFRESH = Duration.ofSeconds(60);
	/** Where Kubernetes mounts a pod's ServiceAccount credentials */
	static final Path SERVICE_ACCOUNT = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");

	private final URI server;
	private final SSLContext sslContext;
	private final Supplier<String> bearerToken;
	private final String namespace;
	private final String description;

	KubeConnection(URI server, SSLContext sslContext, Supplier<String> bearerToken, String namespace,
			String description) {
		this.server = server;
		this.sslContext = sslContext;
		this.bearerToken = bearerToken == null ? () -> null : bearerToken;
		this.namespace = namespace;
		this.description = description;
	}

	public static KubeConnection fromKubeConfig(KubeConfig config) {
		URI server;
		try {
			server = URI.create(config.getServer());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid server URL in " + config.describe() + ": " + config.getServer(),
					e);
		}
		SSLContext ssl = null;
		if ("https".equalsIgnoreCase(server.getScheme())) {
			ssl = sslContext(config.getCertificateAuthorityPem(), config.getClientCertificatePem(),
					config.getClientKeyPem(), config.describe());
		}
		return new KubeConnection(server, ssl, tokenSupplier(config.getToken(), config.getTokenFile()),
				config.getNamespace(), config.describe());
	}

	/** @return whether this JVM runs in a pod with its ServiceAccount token mounted */
	public static boolean isInCluster() {
		String host = System.getenv("KUBERNETES_SERVICE_HOST");
		return host != null && !host.isBlank() && Files.isRegularFile(SERVICE_ACCOUNT.resolve("token"));
	}

	/** The pod's own ServiceAccount, as client-go's in-cluster configuration reads it. */
	public static KubeConnection inCluster() {
		return inCluster(System.getenv("KUBERNETES_SERVICE_HOST"), System.getenv("KUBERNETES_SERVICE_PORT"),
				SERVICE_ACCOUNT);
	}

	static KubeConnection inCluster(String host, String port, Path account) {
		if (host == null || host.isBlank()) {
			throw new IllegalStateException("KUBERNETES_SERVICE_HOST is not set, so this is not a pod of the cluster");
		}
		Path token = account.resolve("token");
		if (!Files.isRegularFile(token)) {
			throw new IllegalStateException("No ServiceAccount token at " + token
					+ "; is automountServiceAccountToken turned off for this pod?");
		}
		String what = "the pod's ServiceAccount (" + account + ")";
		String authority = host.strip().contains(":") ? "[" + host.strip() + "]" : host.strip();
		URI server = URI.create(
				"https://" + authority + ":" + (port == null || port.isBlank() ? "443" : port.strip()));
		return new KubeConnection(server, sslContext(read(account.resolve("ca.crt")), null, null, what),
				tokenSupplier(null, token), read(account.resolve("namespace")), what);
	}

	private static String read(Path file) {
		try {
			return Files.isRegularFile(file) ? Files.readString(file, StandardCharsets.UTF_8).strip() : null;
		} catch (IOException e) {
			throw new IllegalStateException("Cannot read " + file, e);
		}
	}

	static SSLContext sslContext(String caPem, String clientCertPem, String clientKeyPem, String what) {
		try {
			char[] password = UUID.randomUUID().toString().toCharArray();
			KeyManager[] keyManagers = null;
			if (clientCertPem != null && clientKeyPem != null) {
				List<X509Certificate> chain = PemKeys.certificates(clientCertPem, "client certificate of " + what);
				KeyStore keyStore = KeyStore.getInstance("PKCS12");
				keyStore.load(null, null);
				keyStore.setKeyEntry("client", PemKeys.privateKey(clientKeyPem, "client key of " + what), password,
						chain.toArray(new X509Certificate[0]));
				KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				kmf.init(keyStore, password);
				keyManagers = kmf.getKeyManagers();
			}
			TrustManager[] trustManagers = null;
			if (caPem != null) {
				KeyStore trustStore = KeyStore.getInstance("PKCS12");
				trustStore.load(null, null);
				int index = 0;
				for (X509Certificate ca : PemKeys.certificates(caPem, "certificate authority of " + what)) {
					trustStore.setCertificateEntry("ca-" + index++, ca);
				}
				TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
				tmf.init(trustStore);
				trustManagers = tmf.getTrustManagers();
			}
			SSLContext context = SSLContext.getInstance("TLS");
			context.init(keyManagers, trustManagers, null);
			return context;
		} catch (GeneralSecurityException | IOException e) {
			throw new IllegalArgumentException("Cannot set up TLS for " + what + ": " + e.getMessage(), e);
		}
	}

	static Supplier<String> tokenSupplier(String token, Path tokenFile) {
		if (token != null) {
			return () -> token;
		}
		if (tokenFile == null) {
			return () -> null;
		}
		return new Supplier<>() {
			private String cached;
			private Instant readAt = Instant.MIN;

			@Override
			public synchronized String get() {
				Instant now = Instant.now();
				if (this.cached == null || now.isAfter(this.readAt.plus(TOKEN_FILE_REFRESH))) {
					try {
						this.cached = Files.readString(tokenFile, StandardCharsets.UTF_8).strip();
						this.readAt = now;
					} catch (IOException e) {
						throw new KubeApiException("Cannot read the token file " + tokenFile, e);
					}
				}
				return this.cached;
			}
		};
	}

	public URI getServer() {
		return this.server;
	}

	/** @return the TLS context, or null for a plain-http server */
	public SSLContext getSslContext() {
		return this.sslContext;
	}

	/** @return the bearer token to send, or null when authenticating by client certificate */
	public String bearerToken() {
		return this.bearerToken.get();
	}

	/** @return the credentials' default namespace, or null */
	public String getNamespace() {
		return this.namespace;
	}

	public String describe() {
		return this.description;
	}
}
