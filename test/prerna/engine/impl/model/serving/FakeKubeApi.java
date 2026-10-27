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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import prerna.util.k8s.KubeClient;
import prerna.util.k8s.KubeConfig;
import prerna.util.k8s.KubeConnection;
import prerna.util.k8s.KubeJson;

/**
 * Just enough of the Kubernetes API for {@link ServedModelService}: the
 * InferenceServices, pods, events and logs of namespace semoss, with merge
 * patches applied to InferenceService annotations. Every request is recorded.
 */
final class FakeKubeApi implements AutoCloseable {

	private static final String ISVC_PATH = "/apis/serving.kserve.io/v1beta1/namespaces/semoss/inferenceservices";
	private static final String PODS_PATH = "/api/v1/namespaces/semoss/pods";
	private static final String EVENTS_PATH = "/api/v1/namespaces/semoss/events";
	private static final String SERVICES_PATH = "/api/v1/namespaces/semoss/services";
	private static final String CONFIGMAPS_PATH = "/api/v1/namespaces/semoss/configmaps/";
	private static final String NODES_PATH = "/api/v1/nodes";
	private static final String RUNTIMES_PATH = "/apis/serving.kserve.io/v1alpha1/clusterservingruntimes/";

	record Request(String method, String path, Map<String, String> query, String contentType, String body) {
	}

	final List<JsonObject> inferenceServices = new CopyOnWriteArrayList<>();
	final List<JsonObject> pods = new CopyOnWriteArrayList<>();
	final List<JsonObject> events = new CopyOnWriteArrayList<>();
	final List<JsonObject> services = new CopyOnWriteArrayList<>();
	/** when set, listing Services answers 403 with this message */
	volatile String servicesForbidden;
	/** when set, creating a Service answers 403 with this message */
	volatile String serviceCreateForbidden;
	/** when true, a LoadBalancer Service created gets no address, as when its port is taken on the node */
	volatile boolean loadBalancerUnbound;
	/** the serving catalog's YAML, served as ConfigMap semoss-serving-catalog; null for none */
	volatile String catalog;
	/** the cluster's nodes: one Ready node with one GPU, like the DGX Spark */
	final List<JsonObject> nodes = new CopyOnWriteArrayList<>(List.of(node("spark-cbde", 1)));
	/** when set, listing nodes answers 403 with this message */
	volatile String nodesForbidden;
	/** ClusterServingRuntimes by name */
	final Map<String, JsonObject> clusterRuntimes = new java.util.concurrent.ConcurrentHashMap<>();
	/**
	 * When 0 or more, stopping a model marks its pods as terminating and removes
	 * them after that many more pod lists, as KServe does; -1 leaves them be
	 */
	volatile int stopTakesPodLists = -1;
	private final Map<String, Integer> stopping = new java.util.concurrent.ConcurrentHashMap<>();
	private final java.util.concurrent.atomic.AtomicInteger uids = new java.util.concurrent.atomic.AtomicInteger();
	final List<Request> requests = new CopyOnWriteArrayList<>();
	volatile String log = "";

	private final HttpServer server;

	FakeKubeApi() throws IOException {
		this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		this.server.createContext("/", this::handle);
		this.server.start();
	}

	static JsonObject node(String name, int gpus) {
		return JsonParser.parseString("""
				{"metadata": {"name": "%s"}, "spec": {},
				 "status": {"allocatable": {"nvidia.com/gpu": "%d"}, "conditions": [{"type": "Ready", "status": "True"}]}}
				""".formatted(name, gpus)).getAsJsonObject();
	}

	ServedModelService service(Path dir, Instant now) throws IOException {
		return service(dir, now, new ServedModelEndpoints(ServedModelEndpoints.Mode.HOST, "localhost"));
	}

	ServedModelService service(Path dir, Instant now, ServedModelEndpoints endpoints) throws IOException {
		Path kubeconfig = dir.resolve("kubeconfig");
		Files.writeString(kubeconfig, """
				current-context: test
				clusters: [{name: test, cluster: {server: "http://127.0.0.1:%d"}}]
				contexts: [{name: test, context: {cluster: test, user: test}}]
				users: [{name: test, user: {token: test-token}}]
				""".formatted(this.server.getAddress().getPort()));
		ServedModelService service = new ServedModelService(
				new KubeClient(KubeConnection.fromKubeConfig(KubeConfig.load(kubeconfig, null))), "semoss", endpoints,
				"configmap:semoss-serving-catalog", java.util.Set.of("nvidia-vllm"), Clock.fixed(now, ZoneOffset.UTC));
		// every model answers at its endpoint unless a test says otherwise
		service.setProbe(endpoint -> null);
		return service;
	}

	List<Request> requests(String method, String pathPrefix) {
		List<Request> matching = new ArrayList<>();
		for (Request request : this.requests) {
			if (request.method().equals(method) && request.path().startsWith(pathPrefix)) {
				matching.add(request);
			}
		}
		return matching;
	}

	private void handle(HttpExchange exchange) throws IOException {
		String method = exchange.getRequestMethod();
		String path = exchange.getRequestURI().getPath();
		Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
		String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
		this.requests.add(new Request(method, path, query, exchange.getRequestHeaders().getFirst("Content-Type"), body));

		if (path.equals(ISVC_PATH) && method.equals("GET")) {
			json(exchange, 200, list(this.inferenceServices));
		} else if (path.equals(ISVC_PATH) && method.equals("POST")) {
			JsonObject isvc = JsonParser.parseString(body).getAsJsonObject();
			if (find(KubeJson.str(isvc, "metadata", "name")) != null) {
				json(exchange, 409, status(409, "AlreadyExists", "inferenceservices already exists"));
				return;
			}
			isvc.getAsJsonObject("metadata").addProperty("uid", "uid-" + this.uids.incrementAndGet());
			isvc.getAsJsonObject("metadata").addProperty("creationTimestamp", "2026-10-27T02:00:00Z");
			this.inferenceServices.add(isvc);
			json(exchange, 201, isvc);
		} else if (path.startsWith(ISVC_PATH + "/")) {
			JsonObject isvc = find(path.substring(ISVC_PATH.length() + 1));
			if (isvc == null) {
				json(exchange, 404, status(404, "NotFound", "not found"));
			} else if (method.equals("DELETE")) {
				this.inferenceServices.remove(isvc);
				json(exchange, 200, status(200, null, "deleted"));
			} else if (method.equals("PATCH")) {
				JsonObject annotations = KubeJson.obj(JsonParser.parseString(body).getAsJsonObject(), "metadata",
						"annotations");
				for (Map.Entry<String, JsonElement> entry : annotations.entrySet()) {
					isvc.getAsJsonObject("metadata").getAsJsonObject("annotations").add(entry.getKey(), entry.getValue());
				}
				String name = KubeJson.str(isvc, "metadata", "name");
				if (this.stopTakesPodLists >= 0 && "true".equals(KubeJson.str(annotations, ServedModelStateResolver.STOP_ANNOTATION))) {
					for (JsonObject pod : this.pods) {
						if (name.equals(KubeJson.str(pod, "metadata", "labels", ServedModelStateResolver.ISVC_LABEL))) {
							pod.getAsJsonObject("metadata").addProperty("deletionTimestamp", "2026-10-27T17:20:01Z");
						}
					}
					this.stopping.put(name, this.stopTakesPodLists);
				}
				json(exchange, 200, isvc);
			} else {
				json(exchange, 200, isvc);
			}
		} else if (path.equals(PODS_PATH)) {
			advanceStops();
			String selector = query.get("labelSelector");
			List<JsonObject> selected = new ArrayList<>();
			for (JsonObject pod : this.pods) {
				if (selects(selector, KubeJson.str(pod, "metadata", "labels", ServedModelStateResolver.ISVC_LABEL))) {
					selected.add(pod);
				}
			}
			json(exchange, 200, list(selected));
		} else if (path.equals(NODES_PATH)) {
			if (this.nodesForbidden != null) {
				json(exchange, 403, status(403, "Forbidden", this.nodesForbidden));
			} else {
				json(exchange, 200, list(this.nodes));
			}
		} else if (path.startsWith(RUNTIMES_PATH)) {
			JsonObject runtime = this.clusterRuntimes.get(path.substring(RUNTIMES_PATH.length()));
			if (runtime == null) {
				json(exchange, 404, status(404, "NotFound", "clusterservingruntimes not found"));
			} else {
				json(exchange, 200, runtime);
			}
		} else if (path.startsWith(PODS_PATH + "/") && path.endsWith("/log")) {
			byte[] bytes = this.log.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, bytes.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(bytes);
			}
		} else if (path.equals(SERVICES_PATH) && method.equals("POST")) {
			if (this.serviceCreateForbidden != null) {
				json(exchange, 403, status(403, "Forbidden", this.serviceCreateForbidden));
			} else {
				JsonObject service = JsonParser.parseString(body).getAsJsonObject();
				// the API server assigns the ClusterIP, and ServiceLB the address of a LoadBalancer
				service.getAsJsonObject("spec").addProperty("clusterIP", "10.43.0." + (90 + this.services.size()));
				if ("LoadBalancer".equals(KubeJson.str(service, "spec", "type")) && !this.loadBalancerUnbound) {
					service.add("status", JsonParser.parseString("{\"loadBalancer\": {\"ingress\": [{\"ip\": \"10.0.0.218\"}]}}"));
				}
				this.services.add(service);
				json(exchange, 201, service);
			}
		} else if (path.startsWith(CONFIGMAPS_PATH)) {
			if (this.catalog == null || !path.equals(CONFIGMAPS_PATH + "semoss-serving-catalog")) {
				json(exchange, 404, status(404, "NotFound", "configmaps not found"));
			} else {
				JsonObject data = new JsonObject();
				data.addProperty("catalog.yaml", this.catalog);
				JsonObject configMap = new JsonObject();
				configMap.add("data", data);
				json(exchange, 200, configMap);
			}
		} else if (path.equals(SERVICES_PATH)) {
			if (this.servicesForbidden != null) {
				JsonObject status = new JsonObject();
				status.addProperty("kind", "Status");
				status.addProperty("reason", "Forbidden");
				status.addProperty("code", 403);
				status.addProperty("message", this.servicesForbidden);
				json(exchange, 403, status);
			} else {
				String name = query.getOrDefault("fieldSelector", "").replace("metadata.name=", "");
				json(exchange, 200, list(this.services.stream()
						.filter(service -> name.isEmpty() || name.equals(KubeJson.str(service, "metadata", "name")))
						.toList()));
			}
		} else if (path.equals(EVENTS_PATH)) {
			Map<String, String> fields = new LinkedHashMap<>();
			for (String term : query.getOrDefault("fieldSelector", "").split(",")) {
				int equals = term.indexOf('=');
				fields.put(term.substring(0, equals), term.substring(equals + 1));
			}
			List<JsonObject> selected = new ArrayList<>();
			for (JsonObject event : this.events) {
				if (fields.get("involvedObject.name").equals(KubeJson.str(event, "involvedObject", "name"))
						&& (!fields.containsKey("involvedObject.uid")
								|| fields.get("involvedObject.uid").equals(KubeJson.str(event, "involvedObject", "uid")))) {
					selected.add(event);
				}
			}
			json(exchange, 200, list(selected));
		} else {
			exchange.sendResponseHeaders(404, -1);
			exchange.close();
		}
	}

	/** the label selectors the service sends: the label, label=value and label in (a,b) */
	private static boolean selects(String selector, String owner) {
		String label = ServedModelStateResolver.ISVC_LABEL;
		if (selector == null) {
			return true;
		}
		if (selector.equals(label)) {
			return owner != null;
		}
		if (selector.startsWith(label + "=")) {
			return selector.substring(label.length() + 1).equals(owner);
		}
		String in = label + " in (";
		if (selector.startsWith(in) && selector.endsWith(")")) {
			return owner != null && List.of(selector.substring(in.length(), selector.length() - 1).split(",")).contains(owner);
		}
		throw new IllegalArgumentException("Unexpected label selector " + selector);
	}

	/** a pod list after a stop: its pods go after the set number of lists */
	private void advanceStops() {
		for (Map.Entry<String, Integer> entry : this.stopping.entrySet()) {
			if (entry.getValue() <= 0) {
				this.pods.removeIf(pod -> entry.getKey()
						.equals(KubeJson.str(pod, "metadata", "labels", ServedModelStateResolver.ISVC_LABEL)));
				this.stopping.remove(entry.getKey());
			} else {
				entry.setValue(entry.getValue() - 1);
			}
		}
	}

	private static JsonObject status(int code, String reason, String message) {
		JsonObject status = new JsonObject();
		status.addProperty("kind", "Status");
		status.addProperty("code", code);
		if (reason != null) {
			status.addProperty("reason", reason);
		}
		status.addProperty("message", message);
		return status;
	}

	private JsonObject find(String name) {
		for (JsonObject isvc : this.inferenceServices) {
			if (name.equals(KubeJson.str(isvc, "metadata", "name"))) {
				return isvc;
			}
		}
		return null;
	}

	private static JsonObject list(List<JsonObject> items) {
		JsonArray array = new JsonArray();
		items.forEach(array::add);
		JsonObject list = new JsonObject();
		list.add("items", array);
		return list;
	}

	private static void json(HttpExchange exchange, int status, JsonObject body) throws IOException {
		byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().add("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length);
		try (OutputStream out = exchange.getResponseBody()) {
			out.write(bytes);
		}
	}

	private static Map<String, String> query(String raw) {
		Map<String, String> query = new LinkedHashMap<>();
		if (raw != null) {
			for (String pair : raw.split("&")) {
				int equals = pair.indexOf('=');
				String key = URLDecoder.decode(equals < 0 ? pair : pair.substring(0, equals), StandardCharsets.UTF_8);
				String value = equals < 0 ? "" : URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8);
				query.put(key, value);
			}
		}
		return query;
	}

	@Override
	public void close() {
		this.server.stop(0);
	}
}
