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
package prerna.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.Configurable;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import prerna.util.Constants;
import prerna.util.DIHelper;

class HttpHelperUtilityUnitTests {

	private static final Duration LIMIT = Duration.ofSeconds(10);

	private final AtomicInteger requests = new AtomicInteger();
	private HttpServer server;
	private String baseUrl;
	private String namedUrl;
	private CountDownLatch release;
	private Properties savedCoreProp;
	private Properties rdfMap;

	@BeforeEach
	void setRdfMap() {
		this.savedCoreProp = DIHelper.getInstance().getCoreProp();
		this.rdfMap = new Properties();
		DIHelper.getInstance().setCoreProp(this.rdfMap);
	}

	@AfterEach
	void restoreRdfMap() {
		DIHelper.getInstance().setCoreProp(this.savedCoreProp);
	}

	@BeforeEach
	void startServer() throws IOException {
		release = new CountDownLatch(1);
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/header",
				exchange -> reply(exchange, 200, exchange.getRequestHeaders().getFirst("X-Probe")));
		server.createContext("/echo",
				exchange -> reply(exchange, 200, exchange.getRequestHeaders().getFirst("Content-Type") + "|"
						+ new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
		server.createContext("/busy", exchange -> reply(exchange, 429, "slow down"));
		server.createContext("/count", exchange -> {
			requests.incrementAndGet();
			reply(exchange, 200, "counted");
		});
		server.createContext("/redirect", exchange -> {
			exchange.getResponseHeaders().add("Location", "http://blocked.invalid/next");
			exchange.sendResponseHeaders(302, -1);
			exchange.close();
		});
		server.createContext("/slow", exchange -> {
			try {
				release.await(30, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			reply(exchange, 200, "late");
		});
		server.start();
		baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
		namedUrl = "http://localhost:" + server.getAddress().getPort();
	}

	@AfterEach
	void stopServer() {
		release.countDown();
		server.stop(0);
	}

	@Test
	void aClientWaitsWithoutLimitUnlessATimeoutIsGiven() throws Exception {
		try (CloseableHttpClient unlimited = HttpHelperUtility.getCustomClient(null, null, null, null);
				CloseableHttpClient limited = HttpHelperUtility.getCustomClient(null, null, null, null,
						Timeout.ofSeconds(5))) {
			assertEquals(Timeout.DISABLED, ((Configurable) unlimited).getConfig().getResponseTimeout());
			assertEquals(Timeout.ofSeconds(5), ((Configurable) limited).getConfig().getResponseTimeout());
		}
	}

	@Test
	void aGetWithADurationSendsTheHeadersAndReturnsTheBody() {
		assertEquals("seen", HttpHelperUtility.getRequest(baseUrl + "/header", Map.of("X-Probe", "seen"), LIMIT));
	}

	@Test
	void aGetWithNoDurationStillReturnsTheBody() {
		assertEquals("seen", HttpHelperUtility.getRequest(baseUrl + "/header", Map.of("X-Probe", "seen"), null));
	}

	@Test
	void aPostWithATextContentTypeSendsTheBodyAsUtf8() {
		String body = "{\"q\":\"caf\u00e9 \u6771\u4eac\"}";

		String echoed = HttpHelperUtility.postRequestStringBody(baseUrl + "/echo", null, body, "application/json",
				LIMIT);

		assertEquals("application/json; charset=UTF-8|" + body, echoed);
	}

	@Test
	void aPostKeepsTheCharsetTheContentTypeNames() {
		String echoed = HttpHelperUtility.postRequestStringBody(baseUrl + "/echo", null, "plain",
				"text/plain; charset=ISO-8859-1", LIMIT);

		assertEquals("text/plain; charset=ISO-8859-1|plain", echoed);
	}

	@Test
	void aPostWithoutAContentTypeIsRefused() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.postRequestStringBody(baseUrl + "/echo", null, "{}", null, LIMIT));

		assertEquals("Must provide the content type of the request body", error.getMessage());
	}

	@Test
	void aFailedStatusCarriesItsCode() {
		HttpStatusException get = assertThrows(HttpStatusException.class,
				() -> HttpHelperUtility.getRequest(baseUrl + "/busy", null, LIMIT));
		HttpStatusException post = assertThrows(HttpStatusException.class, () -> HttpHelperUtility
				.postRequestStringBody(baseUrl + "/busy", null, "{}", "application/json", LIMIT));

		assertEquals(429, get.getStatusCode());
		assertEquals(429, post.getStatusCode());
		assertTrue(get.getMessage().contains("returned HTTP 429"));
		assertTrue(get.getMessage().contains("slow down"));
	}

	@Test
	void aSilentServerFailsOnceTheDurationPasses() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(baseUrl + "/slow", null, Duration.ofMillis(300)));

		assertFalse(error instanceof HttpStatusException);
		assertInstanceOf(IOException.class, error.getCause());
	}

	@Test
	void aBlacklistedHostIsNotCalled() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "localhost");

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(namedUrl + "/count", null, LIMIT));

		assertTrue(refused.getMessage().contains("You are not allowed to make requests to the host: localhost"));
		assertEquals("counted", HttpHelperUtility.getRequest(baseUrl + "/count", null, LIMIT));
		assertEquals(1, requests.get());
	}

	@Test
	void aHostThatResolvesIntoABlacklistedRangeIsNotCalled() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "127.0.0.0/8, ::1");

		assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(namedUrl + "/count", null, LIMIT));
		assertThrows(IllegalArgumentException.class, () -> HttpHelperUtility.postRequestStringBody(baseUrl + "/count",
				null, "{}", "application/json", LIMIT));
		assertEquals(0, requests.get());
	}

	@Test
	void aRedirectToABlacklistedHostIsNotFollowed() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "blocked.invalid");

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(baseUrl + "/redirect", null, LIMIT));

		assertTrue(refused.getMessage().contains("blocked.invalid"), refused.getMessage());
	}

	@Test
	void withAWhitelistOnlyItsDomainsAreCalled() {
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS, "example.com");

		assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(baseUrl + "/count", null, LIMIT));
		assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(namedUrl + "/count", null, LIMIT));
		assertEquals(0, requests.get());
	}

	@Test
	void aWhitelistCanNameAnInternalHost() {
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS, "example.com, localhost");

		assertEquals("counted", HttpHelperUtility.getRequest(namedUrl + "/count", null, LIMIT));
		assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(baseUrl + "/count", null, LIMIT));
		assertEquals(1, requests.get());
	}

	@Test
	void theSchedulerReachesItsOwnEndpointWhenTheListsCloseLocalhost() throws Exception {
		this.rdfMap.setProperty(Constants.SCHEDULER_ENDPOINT, namedUrl + "/Monolith/api");
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "localhost, 127.0.0.0/8, ::1");
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS, "example.com");

		try (CloseableHttpClient scheduler = HttpHelperUtility.getSchedulerClient(null, null, null, null);
				CloseableHttpClient other = HttpHelperUtility.getCustomClient(null, null, null, null)) {
			assertEquals("counted", scheduler.execute(new HttpGet(namedUrl + "/count"),
					response -> EntityUtils.toString(response.getEntity())));
			assertThrows(IllegalArgumentException.class, () -> scheduler.execute(new HttpGet(baseUrl + "/count"),
					response -> EntityUtils.toString(response.getEntity())));
			assertThrows(IllegalArgumentException.class, () -> other.execute(new HttpGet(namedUrl + "/count"),
					response -> EntityUtils.toString(response.getEntity())));
		}
		assertEquals(1, requests.get());
	}

	@Test
	void theListsApplyToTheOlderRequestMethodsToo() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "localhost, 127.0.0.0/8, ::1");

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequestReader(namedUrl + "/count", null, LIMIT));
		HttpHelperUtility.makePostCall(baseUrl + "/count", "token", Map.of("a", "b"), true);

		assertTrue(refused.getMessage().contains("You are not allowed to make requests"), refused.getMessage());
		assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getAccessToken(baseUrl + "/count", Map.of("code", "abc"), true, true));
		assertEquals(0, requests.get());
	}

	@Test
	void withNoListsTheOlderRequestMethodsStillReachTheServer() throws IOException {
		try (BufferedReader reader = HttpHelperUtility.getRequestReader(baseUrl + "/count", null, LIMIT)) {
			reader.readLine();
		}
		HttpHelperUtility.makePostCall(baseUrl + "/count", "token", Map.of("a", "b"), true);

		assertEquals(2, requests.get());
	}

	@Test
	void aSystemClientWaitsWithoutLimitUnlessATimeoutIsGiven() throws Exception {
		try (CloseableHttpClient unlimited = HttpHelperUtility.getSystemClient();
				CloseableHttpClient limited = HttpHelperUtility.getSystemClient(null, null, null, null,
						Timeout.ofSeconds(5))) {
			assertEquals(Timeout.DISABLED, ((Configurable) unlimited).getConfig().getResponseTimeout());
			assertEquals(Timeout.ofSeconds(5), ((Configurable) limited).getConfig().getResponseTimeout());
		}
	}

	@Test
	void theSystemClientReachesAServerOverPlainHttp() {
		assertEquals("counted", HttpHelperUtility.getRequest(HttpHelperUtility.getSystemClient(), baseUrl + "/count",
				null));
	}

	@Test
	void theListsApplyToTheSystemClient() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "localhost, 127.0.0.0/8, ::1");

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequest(HttpHelperUtility.getSystemClient(), namedUrl + "/count", null));

		assertTrue(refused.getMessage().contains("You are not allowed to make requests"), refused.getMessage());
		assertEquals(0, requests.get());
	}

	@Test
	void onlyTheCustomClientTrustsASelfSignedCertificate(@TempDir Path folder) throws Exception {
		Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
		assumeTrue(Files.isExecutable(keytool), "keytool is not available");
		Path keyStore = folder.resolve("server.p12");
		Process process = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "server", "-keyalg", "RSA",
				"-keysize", "2048", "-validity", "1", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
				"-storetype", "PKCS12", "-keystore", keyStore.toString(), "-storepass", "changeit", "-keypass",
				"changeit").redirectErrorStream(true).start();
		process.getInputStream().readAllBytes();
		assumeTrue(process.waitFor(60, TimeUnit.SECONDS) && process.exitValue() == 0,
				"keytool could not make a certificate");
		KeyStore store = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(keyStore)) {
			store.load(in, "changeit".toCharArray());
		}
		KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keys.init(store, "changeit".toCharArray());
		SSLContext context = SSLContext.getInstance("TLS");
		context.init(keys.getKeyManagers(), null, null);
		HttpsServer secure = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		secure.setHttpsConfigurator(new HttpsConfigurator(context));
		secure.createContext("/secure", exchange -> reply(exchange, 200, "secure"));
		secure.start();
		try {
			String url = "https://localhost:" + secure.getAddress().getPort() + "/secure";

			assertEquals("secure",
					HttpHelperUtility.getRequest(HttpHelperUtility.getCustomClient(null, null, null, null), url, null));
			assertThrows(IllegalArgumentException.class,
					() -> HttpHelperUtility.getRequest(HttpHelperUtility.getSystemClient(), url, null));
		} finally {
			secure.stop(0);
		}
	}

	@Test
	void theReaderStreamsTheBodyWithTheHeadersGiven() throws IOException {
		try (BufferedReader reader = HttpHelperUtility.getRequestReader(baseUrl + "/header",
				Map.of("X-Probe", "probe-value"), LIMIT)) {
			assertEquals("probe-value", reader.readLine());
		}
	}

	@Test
	void theReaderRefusesAStatusThatIsNotASuccess() {
		HttpStatusException refused = assertThrows(HttpStatusException.class,
				() -> HttpHelperUtility.getRequestReader(baseUrl + "/busy", null, LIMIT));

		assertEquals(429, refused.getStatusCode());
	}

	@Test
	void theReaderDoesNotFollowARedirectToABlacklistedHost() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "blocked.invalid");

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> HttpHelperUtility.getRequestReader(baseUrl + "/redirect", null, LIMIT));

		assertTrue(refused.getMessage().contains("blocked.invalid"), refused.getMessage());
	}

	@Test
	void queryParametersAreEncodedAndAddedAfterAnyQueryTheUrlHas() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("q", "a b&c");
		params.put("page size", 10);

		assertEquals("http://example.com/find?q=a+b%26c&page+size=10",
				HttpHelperUtility.withQueryParameters("http://example.com/find", params));
		assertEquals("http://example.com/find?sort=name&q=a+b%26c&page+size=10",
				HttpHelperUtility.withQueryParameters("http://example.com/find?sort=name", params));
		assertEquals("http://example.com/find", HttpHelperUtility.withQueryParameters("http://example.com/find", null));
	}

	private static void reply(HttpExchange exchange, int status, String body) throws IOException {
		byte[] bytes = String.valueOf(body).getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(status, bytes.length);
		try (OutputStream out = exchange.getResponseBody()) {
			out.write(bytes);
		}
	}
}
