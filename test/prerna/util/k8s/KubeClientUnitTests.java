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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLParameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

class KubeClientUnitTests {

	@TempDir
	Path dir;

	private HttpServer server;
	private final AtomicReference<HttpExchange> lastExchange = new AtomicReference<>();
	private final AtomicReference<String> lastBody = new AtomicReference<>();

	@AfterEach
	void stopServer() {
		if (this.server != null) {
			this.server.stop(0);
		}
	}

	private KubeClient plainClient(int status, String contentType, String body) throws IOException {
		this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		this.server.createContext("/", exchange -> respond(exchange, status, contentType, body));
		this.server.start();
		URI uri = URI.create("http://127.0.0.1:" + this.server.getAddress().getPort());
		return new KubeClient(new KubeConnection(uri, null, () -> "test-token", null, "test server"));
	}

	private void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
		this.lastExchange.set(exchange);
		this.lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().add("Content-Type", contentType);
		exchange.sendResponseHeaders(status, bytes.length);
		try (OutputStream out = exchange.getResponseBody()) {
			out.write(bytes);
		}
	}

	@Test
	void getsJsonWithEncodedPathAndQueryAndBearerToken() throws Exception {
		try (KubeClient client = plainClient(200, "application/json", "{\"kind\":\"PodList\",\"items\":[]}")) {
			JsonObject pods = client.get(KubeClient.path("api", "v1", "namespaces", "semoss", "pods")
					.query("labelSelector", "serving.kserve.io/inferenceservice=olmo-2-1b-instruct")
					.query("ignored", null));

			assertEquals("PodList", KubeJson.str(pods, "kind"));
			HttpExchange exchange = this.lastExchange.get();
			assertEquals("GET", exchange.getRequestMethod());
			assertEquals("/api/v1/namespaces/semoss/pods", exchange.getRequestURI().getPath());
			assertEquals("labelSelector=serving.kserve.io/inferenceservice=olmo-2-1b-instruct",
					exchange.getRequestURI().getQuery());
			assertEquals("Bearer test-token", exchange.getRequestHeaders().getFirst("Authorization"));
			assertEquals("application/json", exchange.getRequestHeaders().getFirst("Accept"));
		}
	}

	@Test
	void segmentsCannotChangeThePath() throws Exception {
		try (KubeClient client = plainClient(200, "application/json", "{}")) {
			client.get(KubeClient.path("api", "v1", "namespaces", "semoss", "pods", "../../secrets"));
			assertEquals("/api/v1/namespaces/semoss/pods/..%2F..%2Fsecrets",
					this.lastExchange.get().getRequestURI().getRawPath());
		}
	}

	@Test
	void sendsMergePatch() throws Exception {
		try (KubeClient client = plainClient(200, "application/json", "{\"metadata\":{\"name\":\"x\"}}")) {
			JsonObject patch = new JsonObject();
			patch.addProperty("spec", "value");
			client.mergePatch(KubeClient.path("apis", "serving.kserve.io", "v1beta1", "namespaces", "semoss",
					"inferenceservices", "x"), patch);

			HttpExchange exchange = this.lastExchange.get();
			assertEquals("PATCH", exchange.getRequestMethod());
			assertTrue(exchange.getRequestHeaders().getFirst("Content-Type").startsWith("application/merge-patch+json"));
			assertEquals("{\"spec\":\"value\"}", this.lastBody.get());
		}
	}

	@Test
	void readsText() throws Exception {
		try (KubeClient client = plainClient(200, "text/plain", "line one\nline two\n")) {
			assertEquals("line one\nline two\n",
					client.getText(KubeClient.path("api", "v1", "namespaces", "semoss", "pods", "p", "log")));
			assertEquals("*/*", this.lastExchange.get().getRequestHeaders().getFirst("Accept"));
		}
	}

	@Test
	void mapsStatusErrors() throws Exception {
		String status = "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\","
				+ "\"message\":\"inferenceservices.serving.kserve.io \\\"nope\\\" not found\",\"reason\":\"NotFound\",\"code\":404}";
		try (KubeClient client = plainClient(404, "application/json", status)) {
			KubeApiException error = assertThrows(KubeApiException.class,
					() -> client.get(KubeClient.path("apis", "serving.kserve.io", "v1beta1", "namespaces", "semoss",
							"inferenceservices", "nope")));
			assertEquals(404, error.getCode());
			assertEquals("NotFound", error.getReason());
			assertTrue(error.isNotFound());
			assertEquals("inferenceservices.serving.kserve.io \"nope\" not found", error.getMessage());
		}
	}

	@Test
	void mapsNonJsonErrors() throws Exception {
		try (KubeClient client = plainClient(502, "text/html", "<html>bad gateway</html>")) {
			KubeApiException error = assertThrows(KubeApiException.class,
					() -> client.get(KubeClient.path("api", "v1")));
			assertEquals(502, error.getCode());
			assertEquals("<html>bad gateway</html>", error.getMessage());
		}
	}

	@Test
	void reportsUnreachableServer() throws Exception {
		int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		try (KubeClient client = new KubeClient(
				new KubeConnection(URI.create("http://127.0.0.1:" + port), null, null, null, "closed port"))) {
			KubeApiException error = assertThrows(KubeApiException.class,
					() -> client.get(KubeClient.path("api", "v1")));
			assertTrue(error.isUnreachable());
			assertTrue(error.getMessage().contains("Cannot reach the Kubernetes API"), error.getMessage());
		}
	}

	/** The k3s setup end to end: TLS to an EC server certificate, an EC client certificate for auth. */
	@Test
	void authenticatesWithClientCertificateFromKubeconfig() throws Exception {
		TestPki pki = new TestPki();
		TestPki.Issued serverCert = pki.issue("CN=k3s", "127.0.0.1");
		TestPki.Issued clientCert = pki.issue("CN=system:admin,O=system:masters", null);
		HttpsServer https = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		https.setHttpsConfigurator(new HttpsConfigurator(pki.serverContext(serverCert)) {
			@Override
			public void configure(HttpsParameters params) {
				SSLParameters parameters = getSSLContext().getDefaultSSLParameters();
				parameters.setNeedClientAuth(true);
				params.setSSLParameters(parameters);
			}
		});
		https.createContext("/version", exchange -> {
			X509Certificate peer = (X509Certificate) ((HttpsExchange) exchange).getSSLSession()
					.getPeerCertificates()[0];
			respond(exchange, 200, "application/json",
					"{\"client\":\"" + peer.getSubjectX500Principal().getName() + "\"}");
		});
		https.start();
		this.server = https;

		String kubeconfig = """
				current-context: default
				clusters: [{name: default, cluster: {server: "https://127.0.0.1:%d", certificate-authority-data: %s}}]
				contexts: [{name: default, context: {cluster: default, user: default}}]
				users: [{name: default, user: {client-certificate-data: %s, client-key-data: %s}}]
				""".formatted(https.getAddress().getPort(), TestPki.base64(TestPki.certificatePem(pki.ca)),
				TestPki.base64(TestPki.certificatePem(clientCert.certificate())),
				TestPki.base64(TestPki.sec1Pem(clientCert.keys())));
		Path file = this.dir.resolve("kubeconfig");
		Files.writeString(file, kubeconfig);

		try (KubeClient client = new KubeClient(KubeConnection.fromKubeConfig(KubeConfig.load(file, null)))) {
			JsonObject version = client.get(KubeClient.path("version"));
			assertTrue(KubeJson.str(version, "client").contains("CN=system:admin"), version.toString());
		}
	}

	/** In a pod: TLS trusting the mounted CA, the mounted token as the bearer token. */
	@Test
	void authenticatesWithTheServiceAccountToken() throws Exception {
		TestPki pki = new TestPki();
		TestPki.Issued serverCert = pki.issue("CN=kubernetes", "127.0.0.1");
		HttpsServer https = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		https.setHttpsConfigurator(new HttpsConfigurator(pki.serverContext(serverCert)));
		https.createContext("/version", exchange -> respond(exchange, 200, "application/json",
				"{\"authorization\":\"" + exchange.getRequestHeaders().getFirst("Authorization") + "\"}"));
		https.start();
		this.server = https;
		Path account = Files.createDirectories(this.dir.resolve("serviceaccount"));
		Files.writeString(account.resolve("token"), "eyJ.service-account\n");
		Files.writeString(account.resolve("ca.crt"), TestPki.certificatePem(pki.ca));

		try (KubeClient client = new KubeClient(
				KubeConnection.inCluster("127.0.0.1", String.valueOf(https.getAddress().getPort()), account))) {
			assertEquals("Bearer eyJ.service-account",
					KubeJson.str(client.get(KubeClient.path("version")), "authorization"));
		}
	}
}
