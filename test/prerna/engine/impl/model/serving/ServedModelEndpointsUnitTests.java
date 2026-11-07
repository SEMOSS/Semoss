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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.impl.model.serving.ServedModelEndpoints.Endpoint;
import prerna.engine.impl.model.serving.ServedModelEndpoints.Mode;
import prerna.util.k8s.KubeJson;

/**
 * The Services are the namespace's as kserve-verify.sh listed them (sections V3
 * and V4) with the sample running: its KServe predictor Service, its
 * LoadBalancer on port 8000, and the stack's own Services, which select other
 * pods.
 */
class ServedModelEndpointsUnitTests {

	private static final ServedModelEndpoints HOST = new ServedModelEndpoints(Mode.HOST, "localhost");
	private static final ServedModelEndpoints CLUSTER = new ServedModelEndpoints(Mode.CLUSTER, "localhost");

	private static List<JsonObject> running() {
		return Fixtures.items("services-running.json");
	}

	private static List<JsonObject> without(List<JsonObject> services, String name) {
		return services.stream().filter(service -> !name.equals(KubeJson.str(service, "metadata", "name"))).toList();
	}

	private static JsonObject service(String json) {
		return JsonParser.parseString(json).getAsJsonObject();
	}

	@Test
	void hostUsesThePublishedPort() {
		Endpoint endpoint = HOST.resolve(Fixtures.json("ready-isvc.json"), running());
		assertEquals("http://localhost:8000/v1", endpoint.url());
		assertNull(endpoint.note());

		Endpoint lan = new ServedModelEndpoints(Mode.HOST, "10.0.0.218").resolve(Fixtures.json("ready-isvc.json"),
				running());
		assertEquals("http://10.0.0.218:8000/v1", lan.url());
	}

	@Test
	void hostKeepsThePublishedPortWhileStopped() {
		Endpoint endpoint = HOST.resolve(Fixtures.json("stopped-isvc.json"), Fixtures.items("services-stopped.json"));
		assertEquals("http://localhost:8000/v1", endpoint.url(), "the LoadBalancer outlives KServe's Service");
	}

	@Test
	void hostWarnsWhenThePortIsNotBound() {
		List<JsonObject> services = running();
		services.stream().filter(service -> "olmo-2-1b-instruct-lb".equals(KubeJson.str(service, "metadata", "name")))
				.forEach(service -> service.getAsJsonObject("status").add("loadBalancer", new JsonObject()));
		Endpoint endpoint = HOST.resolve(Fixtures.json("ready-isvc.json"), services);
		assertEquals("http://localhost:8000/v1", endpoint.url());
		assertTrue(endpoint.note().contains("port 8000 may already be taken"), endpoint.note());
	}

	@Test
	void hostFallsBackToKServesClusterIpWithAWarning() {
		Endpoint endpoint = HOST.resolve(Fixtures.json("ready-isvc.json"), without(running(), "olmo-2-1b-instruct-lb"));
		assertEquals("http://10.43.146.239/v1", endpoint.url());
		assertTrue(endpoint.note().contains("new ClusterIP on every start"), endpoint.note());
	}

	@Test
	void hostPrefersSemossOwnedClusterIp() {
		List<JsonObject> services = new java.util.ArrayList<>(without(running(), "olmo-2-1b-instruct-lb"));
		services.add(service("""
				{"metadata": {"name": "olmo-2-1b-instruct-endpoint", "labels": {"app.kubernetes.io/managed-by": "semoss"}},
				 "spec": {"type": "ClusterIP", "clusterIP": "10.43.9.9",
				          "selector": {"serving.kserve.io/inferenceservice": "olmo-2-1b-instruct", "component": "predictor"},
				          "ports": [{"name": "http", "port": 80, "targetPort": 8080}]}}"""));
		Endpoint endpoint = HOST.resolve(Fixtures.json("ready-isvc.json"), services);
		assertEquals("http://10.43.9.9/v1", endpoint.url());
		assertNull(endpoint.note());
	}

	@Test
	void hostHasNoEndpointForAStoppedModelWithoutItsOwnService() {
		Endpoint endpoint = HOST.resolve(Fixtures.json("stopped-isvc.json"),
				without(Fixtures.items("services-stopped.json"), "olmo-2-1b-instruct-lb"));
		assertNull(endpoint.url());
		assertTrue(endpoint.note().contains("while it is stopped"), endpoint.note());
	}

	@Test
	void clusterUsesDnsNames() {
		assertEquals("http://olmo-2-1b-instruct-predictor.semoss.svc.cluster.local/v1",
				CLUSTER.resolve(Fixtures.json("ready-isvc.json"), running()).url(), "status.address.url");
		assertEquals("http://olmo-2-1b-instruct-predictor.semoss.svc.cluster.local/v1",
				CLUSTER.resolve(Fixtures.json("stopped-isvc.json"), Fixtures.items("services-stopped.json")).url(),
				"computed when KServe has cleared the address");

		List<JsonObject> services = new java.util.ArrayList<>(running());
		services.add(service("""
				{"metadata": {"name": "olmo-2-1b-instruct-endpoint", "labels": {"app.kubernetes.io/managed-by": "semoss"}},
				 "spec": {"type": "LoadBalancer", "clusterIP": "10.43.9.9",
				          "selector": {"serving.kserve.io/inferenceservice": "olmo-2-1b-instruct"},
				          "ports": [{"name": "http", "port": 8001, "targetPort": 8080}]}}"""));
		assertEquals("http://olmo-2-1b-instruct-endpoint.semoss.svc.cluster.local:8001/v1",
				CLUSTER.resolve(Fixtures.json("ready-isvc.json"), services).url());
	}

	@Test
	void onlyServicesSelectingTheModelsPodsCount() {
		List<JsonObject> selecting = ServedModelEndpoints.selecting(Fixtures.json("ready-isvc.json"), running());
		assertEquals(List.of("olmo-2-1b-instruct-lb", "olmo-2-1b-instruct-predictor"),
				selecting.stream().map(service -> KubeJson.str(service, "metadata", "name")).toList());

		List<JsonObject> otherModel = ServedModelEndpoints.selecting(Fixtures.json("created-stopped-isvc.json"),
				running());
		assertTrue(otherModel.isEmpty());
	}

	@Test
	void theStatusShowsTheNodePortAModelIsPublishedOn() {
		ServedModelStatus sample = new ServedModelStatus();
		HOST.apply(sample, Fixtures.json("ready-isvc.json"), Fixtures.items("services-running.json"), null);
		assertEquals(8000, sample.getHostPort(), "the sample's -lb Service");

		ServedModelStatus internal = new ServedModelStatus();
		HOST.apply(internal, Fixtures.json("ready-isvc.json"),
				without(Fixtures.items("services-running.json"), "olmo-2-1b-instruct-lb"), null);
		assertNull(internal.getHostPort());
	}

	@Test
	void aPublishedModelIsReachedOnItsPortInTheCluster() {
		JsonObject isvc = Fixtures.json("ready-isvc.json");
		String name = KubeJson.str(isvc, "metadata", "name");
		JsonObject published = ServedModelTemplate.endpointService(name, "semoss", "uid-1", 8001);
		published.getAsJsonObject("spec").addProperty("clusterIP", "10.43.0.91");

		assertEquals("http://olmo-2-1b-instruct-endpoint.semoss.svc.cluster.local:8001/v1",
				CLUSTER.resolve(isvc, List.of(published)).url());
		assertTrue(HOST.resolve(isvc, List.of(published)).url().equals("http://localhost:8001/v1"));
	}

	@Test
	void theProbeCountsAnyAnswerAndOnlyRefusalsAgainst() throws Exception {
		com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer
				.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/up/v1/models", exchange -> {
			exchange.sendResponseHeaders(200, -1);
			exchange.close();
		});
		server.createContext("/failing/v1/models", exchange -> {
			exchange.sendResponseHeaders(500, -1);
			exchange.close();
		});
		server.start();
		int closed;
		try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
			closed = socket.getLocalPort();
		}
		try {
			String base = "http://127.0.0.1:" + server.getAddress().getPort();
			assertNull(ServedModelEndpoints.HTTP_PROBE.problem(base + "/up/v1"));
			assertNull(ServedModelEndpoints.HTTP_PROBE.problem(base + "/failing/v1"), "an error answer still routes");
			String refused = ServedModelEndpoints.HTTP_PROBE.problem("http://127.0.0.1:" + closed + "/v1");
			assertTrue(refused != null && refused.startsWith("ConnectException"), refused);
		} finally {
			server.stop(0);
		}
	}
}
