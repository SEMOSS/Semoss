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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import prerna.reactor.automation.AutomationConstants;

public class AutomationApiRequestExecutorUnitTests {

	private static HttpServer server;
	private static String base;

	@BeforeAll
	public static void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/json", exchange -> {
			exchange.getResponseHeaders().add("Content-Type", "application/json");
			exchange.getResponseHeaders().add("Set-Cookie", "session=secret");
			exchange.getResponseHeaders().add("X-Echo-Method", exchange.getRequestMethod());
			respond(exchange, 200, "{\"ok\":true,\"items\":[1,2]}");
		});
		server.createContext("/missing", exchange -> respond(exchange, 404, "nope"));
		server.createContext("/boom", exchange -> respond(exchange, 500, "broken"));
		server.createContext("/redirect", exchange -> {
			exchange.getResponseHeaders().add("Location", "/json");
			respond(exchange, 302, "");
		});
		server.createContext("/echo", exchange -> {
			byte[] body = exchange.getRequestBody().readAllBytes();
			exchange.getResponseHeaders().add("Content-Type", "text/plain");
			respond(exchange, 200, exchange.getRequestHeaders().getFirst("Content-Type") + "|"
					+ new String(body, StandardCharsets.UTF_8));
		});
		server.createContext("/big", exchange -> {
			exchange.getResponseHeaders().add("Content-Type", "text/plain");
			respond(exchange, 200, "x".repeat(AutomationConstants.API_RESPONSE_MAX_BYTES + 500));
		});
		server.start();
		base = "http://127.0.0.1:" + server.getAddress().getPort();
	}

	@AfterAll
	public static void stopServer() {
		server.stop(0);
	}

	private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body)
			throws IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
		if (bytes.length > 0) {
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(bytes);
			}
		}
		exchange.close();
	}

	private static AutomationApiRequestExecutor.PreparedRequest request(String method, String path,
			Map<String, String> headers, byte[] body) {
		return new AutomationApiRequestExecutor.PreparedRequest(method, URI.create(base + path),
				new LinkedHashMap<>(headers), "json", body, Duration.ofSeconds(5));
	}

	@Test
	public void returnsParsedJsonAndHidesCookies() {
		Map<String, Object> out = AutomationApiRequestExecutor.send(request("GET", "/json", Map.of(), null));
		assertEquals(200, out.get("status"));
		assertEquals(Boolean.FALSE, out.get("truncated"));
		@SuppressWarnings("unchecked")
		Map<String, Object> body = (Map<String, Object>) out.get("body");
		assertEquals(Boolean.TRUE, body.get("ok"));
		@SuppressWarnings("unchecked")
		Map<String, String> headers = (Map<String, String>) out.get("headers");
		assertFalse(headers.containsKey("set-cookie"));
		assertEquals("GET", headers.get("x-echo-method"));
	}

	@Test
	public void clientAndServerErrorsAreOrdinaryResponses() {
		assertEquals(404, AutomationApiRequestExecutor.send(request("GET", "/missing", Map.of(), null)).get("status"));
		Map<String, Object> boom = AutomationApiRequestExecutor.send(request("GET", "/boom", Map.of(), null));
		assertEquals(500, boom.get("status"));
		assertEquals("broken", boom.get("body"));
	}

	@Test
	public void doesNotFollowRedirects() {
		Map<String, Object> out = AutomationApiRequestExecutor.send(request("GET", "/redirect", Map.of(), null));
		assertEquals(302, out.get("status"));
		@SuppressWarnings("unchecked")
		Map<String, String> headers = (Map<String, String>) out.get("headers");
		assertEquals("/json", headers.get("location"));
	}

	@Test
	public void sendsPatchWithBody() {
		byte[] body = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
		Map<String, Object> out = AutomationApiRequestExecutor
				.send(request("PATCH", "/echo", Map.of("Content-Type", "application/json"), body));
		assertEquals("application/json|{\"a\":1}", out.get("body"));
	}

	@Test
	public void headReturnsNoBody() {
		Map<String, Object> out = AutomationApiRequestExecutor.send(request("HEAD", "/json", Map.of(), null));
		assertEquals(200, out.get("status"));
		assertNull(out.get("body"));
	}

	@Test
	public void truncatesLargeBodies() {
		Map<String, Object> out = AutomationApiRequestExecutor.send(request("GET", "/big", Map.of(), null));
		assertEquals(Boolean.TRUE, out.get("truncated"));
		assertEquals(AutomationConstants.API_RESPONSE_MAX_BYTES, ((String) out.get("body")).length());
	}

	@Test
	public void unreachableHostFailsTheNode() {
		AutomationApiRequestExecutor.PreparedRequest unreachable = new AutomationApiRequestExecutor.PreparedRequest(
				"GET", URI.create("http://127.0.0.1:1/"), new LinkedHashMap<>(), "none", null, Duration.ofSeconds(2));
		assertThrows(IllegalStateException.class, () -> AutomationApiRequestExecutor.send(unreachable));
	}

	@Test
	public void blocksPrivateAndInternalAddresses() throws Exception {
		for (String literal : List.of("127.0.0.1", "10.1.2.3", "172.16.0.9", "192.168.1.1", "169.254.169.254",
				"100.64.0.1", "0.0.0.0", "::1", "fc00::1", "fe80::1")) {
			assertFalse(AutomationApiRequestExecutor.isPublic(InetAddress.getByName(literal)), literal);
		}
		assertTrue(AutomationApiRequestExecutor.isPublic(InetAddress.getByName("8.8.8.8")));
		assertTrue(AutomationApiRequestExecutor.isPublic(InetAddress.getByName("2606:4700:4700::1111")));
	}

	@Test
	public void rejectsHostThatResolvesToAnInternalAddressEvenAmongPublicOnes() throws Exception {
		InetAddress[] mixed = { InetAddress.getByName("8.8.8.8"), InetAddress.getByName("10.0.0.5") };
		assertThrows(IllegalArgumentException.class,
				() -> AutomationApiRequestExecutor.checkAddresses("rebind.example.com", host -> mixed, "node"));
		InetAddress[] publicOnly = { InetAddress.getByName("8.8.8.8") };
		AutomationApiRequestExecutor.checkAddresses("example.com", host -> publicOnly, "node");
	}

	@Test
	public void visibleHeadersDropSecretsAndPseudoHeaders() {
		Map<String, List<String>> raw = new LinkedHashMap<>();
		raw.put(":status", List.of("200"));
		raw.put("Content-Type", List.of("application/json"));
		raw.put("Set-Cookie", List.of("a=b"));
		raw.put("Authorization", List.of("Bearer x"));
		raw.put("X-Multi", List.of("1", "2"));
		Map<String, String> headers = AutomationApiRequestExecutor.visibleHeaders(raw);
		assertEquals(Map.of("content-type", "application/json", "x-multi", "1, 2"), headers);
	}

	@Test
	public void interpolatesEmbeddedReferences() {
		Map<String, Object> scope = Map.of("token", "abc", "item", Map.of("id", 7));
		assertEquals("Bearer abc",
				AutomationApiRequestExecutor.interpolate("Bearer ${token}", scope, "header"));
		assertEquals("https://api.example.com/items/7",
				AutomationApiRequestExecutor.interpolate("https://api.example.com/items/${item.id}", scope, "url"));
	}

	@Test
	public void unresolvedReferenceIsAnError() {
		assertThrows(IllegalArgumentException.class,
				() -> AutomationApiRequestExecutor.interpolate("/items/${missing}", Map.of(), "url"));
	}

	@Test
	public void listReferenceCannotBeEmbeddedInText() {
		assertThrows(IllegalArgumentException.class, () -> AutomationApiRequestExecutor
				.interpolate("/items/${ids}", Map.of("ids", Arrays.asList(1, 2)), "url"));
	}

	@Test
	public void exactReferenceKeepsItsType() {
		Object resolved = AutomationApiRequestExecutor.resolve("${payload}", Map.of("payload", Map.of("a", 1)),
				"body");
		assertEquals(Map.of("a", 1), resolved);
	}

	@Test
	public void jsonThatCannotBeParsedIsReturnedAsText() {
		assertEquals("{not json", AutomationApiRequestExecutor
				.parseBody("{not json".getBytes(StandardCharsets.UTF_8), "application/json", false));
	}
}
