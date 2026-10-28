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
package prerna.engine.impl.model.serving;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static prerna.engine.impl.model.serving.GpuFixtures.OLMO_GPU;
import static prerna.engine.impl.model.serving.GpuFixtures.OLMO_POD;
import static prerna.engine.impl.model.serving.GpuFixtures.matrix;
import static prerna.engine.impl.model.serving.GpuFixtures.metric;
import static prerna.engine.impl.model.serving.GpuFixtures.sample;
import static prerna.engine.impl.model.serving.GpuFixtures.series;
import static prerna.engine.impl.model.serving.GpuFixtures.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import prerna.engine.impl.model.serving.ServingPrometheus.QueryException;
import prerna.engine.impl.model.serving.ServingPrometheus.Sample;
import prerna.engine.impl.model.serving.ServingPrometheus.Series;
import prerna.util.k8s.KubeApiException;
import prerna.util.k8s.KubeClient;
import prerna.util.k8s.KubeConfig;
import prerna.util.k8s.KubeConnection;

class ServingPrometheusUnitTests {

	private static final String PROXY = "/api/v1/namespaces/monitoring/services/monitoring-kube-prometheus-prometheus:9090/proxy";

	@TempDir
	Path dir;

	/** what the local server was asked: the decoded path, the query, and the Authorization header */
	private record Asked(String path, Map<String, String> query, String authorization) {
	}

	private HttpServer server;
	private final List<Asked> asked = new CopyOnWriteArrayList<>();
	private volatile int status = 200;
	private volatile String body = vector();

	@BeforeEach
	void startServer() throws IOException {
		this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		this.server.createContext("/", this::handle);
		this.server.start();
	}

	@AfterEach
	void stopServer() {
		this.server.stop(0);
	}

	private void handle(HttpExchange exchange) throws IOException {
		Map<String, String> query = new LinkedHashMap<>();
		String raw = exchange.getRequestURI().getRawQuery();
		if (raw != null) {
			for (String pair : raw.split("&")) {
				int equals = pair.indexOf('=');
				query.put(URLDecoder.decode(pair.substring(0, equals), StandardCharsets.UTF_8),
						URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8));
			}
		}
		this.asked.add(new Asked(exchange.getRequestURI().getPath(), query,
				exchange.getRequestHeaders().getFirst("Authorization")));
		byte[] bytes = this.body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().add("Content-Type", "application/json");
		exchange.sendResponseHeaders(this.status, bytes.length);
		try (OutputStream out = exchange.getResponseBody()) {
			out.write(bytes);
		}
	}

	private String url() {
		return "http://127.0.0.1:" + this.server.getAddress().getPort();
	}

	private KubeClient client() throws IOException {
		Path kubeconfig = this.dir.resolve("kubeconfig");
		Files.writeString(kubeconfig, """
				current-context: test
				clusters: [{name: test, cluster: {server: "%s"}}]
				contexts: [{name: test, context: {cluster: test, user: test}}]
				users: [{name: test, user: {token: test-token}}]
				""".formatted(url()));
		return new KubeClient(KubeConnection.fromKubeConfig(KubeConfig.load(kubeconfig, null)));
	}

	@Test
	void readsTheRecordedDcgmSample() {
		List<Sample> samples = ServingPrometheus
				.parseVector(vector(sample(metric("DCGM_FI_DEV_GPU_UTIL", OLMO_GPU), 1_793_054_000L, 2)));

		assertEquals(1, samples.size());
		assertEquals(2.0, samples.get(0).value());
		assertEquals("DCGM_FI_DEV_GPU_UTIL", samples.get(0).labels().get("__name__"));
		assertEquals(OLMO_POD, samples.get(0).labels().get("pod"));
		assertEquals("NVIDIA GB10", samples.get(0).labels().get("modelName"));
	}

	@Test
	void leavesOutValuesThatAreNotNumbers() {
		String gpu = metric("DCGM_FI_DEV_GPU_UTIL", OLMO_GPU);
		List<Sample> samples = ServingPrometheus.parseVector(vector("{\"metric\":" + gpu + ",\"value\":[1,\"NaN\"]}",
				"{\"metric\":" + gpu + ",\"value\":[1,\"+Inf\"]}", "{\"metric\":" + gpu + ",\"value\":[1]}",
				sample(gpu, 1, 7)));

		assertEquals(1, samples.size());
		assertEquals(7.0, samples.get(0).value());
	}

	@Test
	void readsSeriesPointByPoint() {
		List<Series> series = ServingPrometheus
				.parseMatrix(matrix(series(metric("DCGM_FI_DEV_POWER_USAGE", OLMO_GPU), 60, 9.666, 120, 31.5)));

		assertEquals(1, series.size());
		assertEquals(List.of(new ServingPrometheus.Point(60, 9.666), new ServingPrometheus.Point(120, 31.5)),
				series.get(0).points());
	}

	@Test
	void refusalsCarryPrometheusOwnWords() {
		QueryException refused = assertThrows(QueryException.class, () -> ServingPrometheus.parseVector(
				"{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"1:5: parse error: unexpected end\"}"));
		assertEquals("Prometheus refused the query: 1:5: parse error: unexpected end", refused.getMessage());

		QueryException html = assertThrows(QueryException.class,
				() -> ServingPrometheus.parseVector("<html>Bad Gateway</html>"));
		assertTrue(html.getMessage().startsWith("Prometheus did not answer with JSON"), html.getMessage());

		QueryException type = assertThrows(QueryException.class, () -> ServingPrometheus.parseVector(matrix()));
		assertTrue(type.getMessage().contains("matrix where a vector"), type.getMessage());
	}

	@Test
	void aUrlGetsTheQueriesDirectly() {
		ServingPrometheus prometheus = ServingPrometheus.at(url());
		this.body = vector(sample(metric("DCGM_FI_DEV_GPU_UTIL", OLMO_GPU), 1000, 2));
		assertEquals(1, prometheus.instant("{__name__=~\"A|B\"}", Instant.ofEpochSecond(1000)).size());

		this.body = matrix();
		prometheus.range("avg_over_time(DCGM_FI_DEV_GPU_UTIL[60s])", Instant.ofEpochSecond(0),
				Instant.ofEpochSecond(3600), Duration.ofMinutes(1));

		assertEquals("/api/v1/query", this.asked.get(0).path());
		assertEquals(Map.of("query", "{__name__=~\"A|B\"}", "time", "1000"), this.asked.get(0).query());
		assertEquals("/api/v1/query_range", this.asked.get(1).path());
		assertEquals(Map.of("query", "avg_over_time(DCGM_FI_DEV_GPU_UTIL[60s])", "start", "0", "end", "3600", "step",
				"60s"), this.asked.get(1).query());
		assertNull(this.asked.get(0).authorization(), "Prometheus needs no credentials");
		assertEquals("Prometheus at " + url(), prometheus.getSource());
	}

	@Test
	void aUrlSaysWhyItGaveNoAnswer() throws IOException {
		this.status = 422;
		this.body = "{\"status\":\"error\",\"errorType\":\"execution\",\"error\":\"query timed out\"}";
		QueryException failed = assertThrows(QueryException.class,
				() -> ServingPrometheus.at(url()).instant("up", Instant.EPOCH));
		assertEquals("Prometheus at " + url() + " answered HTTP 422: query timed out", failed.getMessage());

		int closed;
		try (ServerSocket socket = new ServerSocket(0)) {
			closed = socket.getLocalPort();
		}
		QueryException refused = assertThrows(QueryException.class,
				() -> ServingPrometheus.at("http://127.0.0.1:" + closed).instant("up", Instant.EPOCH));
		assertTrue(refused.getMessage().startsWith("Prometheus did not answer at http://127.0.0.1:" + closed + ": "),
				refused.getMessage());
	}

	@Test
	void onTheHostTheQueriesGoThroughTheKubernetesApi() throws IOException {
		ServingPrometheus prometheus = ServingPrometheus.throughKubernetes(client());
		this.body = vector();
		prometheus.instant("up", Instant.ofEpochSecond(5));
		this.body = matrix();
		prometheus.range("up", Instant.ofEpochSecond(0), Instant.ofEpochSecond(60), Duration.ofSeconds(15));

		assertEquals(PROXY + "/api/v1/query", this.asked.get(0).path());
		assertEquals(Map.of("query", "up", "time", "5"), this.asked.get(0).query());
		assertEquals("Bearer test-token", this.asked.get(0).authorization(), "SEMOSS's own credentials");
		assertEquals(PROXY + "/api/v1/query_range", this.asked.get(1).path());
		assertEquals("15s", this.asked.get(1).query().get("step"));
	}

	@Test
	void aRefusedProxySaysWhatToDo() throws IOException {
		this.status = 403;
		this.body = "{\"kind\":\"Status\",\"reason\":\"Forbidden\",\"code\":403,\"message\":\"services "
				+ "\\\"monitoring-kube-prometheus-prometheus:9090\\\" is forbidden: User "
				+ "\\\"system:serviceaccount:semoss:semoss\\\" cannot get resource \\\"services/proxy\\\"\"}";
		QueryException forbidden = assertThrows(QueryException.class,
				() -> ServingPrometheus.throughKubernetes(client()).instant("up", Instant.EPOCH));
		assertTrue(forbidden.getMessage().startsWith("SEMOSS may not reach Prometheus through the Kubernetes API "
				+ "(HTTP 403), as with the semoss ServiceAccount's token. Set KSERVE_PROMETHEUS_URL"),
				forbidden.getMessage());

		assertTrue(ServingPrometheus.describeProxyFailure(new KubeApiException(404, "NotFound", "not found"))
				.contains("Apply dgx/monitoring.yml"));
		assertTrue(ServingPrometheus
				.describeProxyFailure(new KubeApiException(503, "ServiceUnavailable",
						"no endpoints available for service \"monitoring-kube-prometheus-prometheus:9090\""))
				.startsWith("Prometheus monitoring/monitoring-kube-prometheus-prometheus:9090 is not running (HTTP 503)"));
		assertEquals("Cannot reach the Kubernetes API",
				ServingPrometheus.describeProxyFailure(new KubeApiException("Cannot reach the Kubernetes API", null)));
	}

	@Test
	void prometheusIsFoundWhereSemossRuns() throws IOException {
		KubeClient client = client();
		assertEquals("Prometheus at http://monitoring-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090",
				ServingPrometheus.choose(null, client, ServedModelEndpoints.Mode.CLUSTER).getSource());
		assertEquals("Prometheus monitoring/monitoring-kube-prometheus-prometheus:9090 through the Kubernetes API",
				ServingPrometheus.choose(null, client, ServedModelEndpoints.Mode.HOST).getSource());
		assertEquals("Prometheus at http://10.43.12.7:9090",
				ServingPrometheus.choose("http://10.43.12.7:9090", client, ServedModelEndpoints.Mode.HOST).getSource());

		ServingPrometheus off = ServingPrometheus.choose(ServingSettings.OFF, client, ServedModelEndpoints.Mode.HOST);
		assertTrue(off.isOff());
		assertNull(off.getSource());
		assertTrue(this.asked.isEmpty(), "choosing asks nothing");
	}

	@Test
	void aMalformedSettingIsAProblemNotAnOff() {
		ServingPrometheus broken = new ServingPrometheus(null, null,
				"KSERVE_PROMETHEUS_URL must be auto, off or an http(s) URL");
		assertFalse(broken.isOff());
		QueryException failed = assertThrows(QueryException.class, () -> broken.instant("up", Instant.EPOCH));
		assertEquals("KSERVE_PROMETHEUS_URL must be auto, off or an http(s) URL", failed.getMessage());
		assertEquals("Prometheus is turned off (KSERVE_PROMETHEUS_URL=off)",
				assertThrows(QueryException.class, () -> ServingPrometheus.off().instant("up", Instant.EPOCH))
						.getMessage());
	}
}
