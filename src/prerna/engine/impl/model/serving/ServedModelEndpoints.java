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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.util.k8s.KubeJson;

/**
 * The OpenAI-compatible base URL at which this SEMOSS reaches a served model.
 *
 * In a pod, cluster DNS works and names are stable: SEMOSS's own Service for
 * the model if there is one, else KServe's predictor Service. On the host, DNS
 * names do not resolve, so a LoadBalancer Service selecting the model's pods
 * (published on the node by k3s's ServiceLB) is used, else a stable ClusterIP
 * (ClusterIPs are routable from the node itself). KServe's own predictor Service
 * is the last resort there: it is deleted on stop and comes back with a new
 * ClusterIP on every start.
 */
public final class ServedModelEndpoints {

	public enum Mode {
		/** SEMOSS runs in a pod of the cluster */
		CLUSTER,
		/** SEMOSS runs on the node itself (Tomcat on the host) */
		HOST
	}

	/** vLLM's port in the runtime's container, named http1 */
	private static final int MODEL_PORT = 8080;
	private static final String MODEL_PORT_NAME = "http1";

	private final Mode mode;
	private final String hostAddress;

	public ServedModelEndpoints(Mode mode, String hostAddress) {
		this.mode = mode;
		this.hostAddress = hostAddress;
	}

	public Mode getMode() {
		return this.mode;
	}

	/** An endpoint, or none, with what the caller should know about it. */
	public record Endpoint(String url, String note) {
	}

	/** Whether this SEMOSS can call a model's endpoint; tests replace it */
	interface Probe {
		/** @return null when the endpoint answers at all, else why it does not */
		String problem(String endpoint);
	}

	private static final HttpClient PROBE_CLIENT = HttpClient.newBuilder().proxy(HttpClient.Builder.NO_PROXY)
			.connectTimeout(Duration.ofSeconds(1)).build();

	/**
	 * GET {endpoint}/models, which vLLM answers at once. Any HTTP answer means the
	 * route works; only a refused or timed-out connection counts. Never through a
	 * proxy: a model's endpoint is always on the node or in the cluster.
	 */
	static final Probe HTTP_PROBE = endpoint -> {
		try {
			PROBE_CLIENT.send(HttpRequest.newBuilder(URI.create(endpoint + "/models")).timeout(Duration.ofSeconds(2))
					.GET().build(), HttpResponse.BodyHandlers.discarding());
			return null;
		} catch (IOException e) {
			return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : " (" + e.getMessage() + ")");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return "interrupted";
		} catch (IllegalArgumentException e) {
			return "not a URL: " + endpoint;
		}
	};

	/**
	 * @param services the Services of the model's namespace, or null when they
	 *                 could not be listed
	 */
	void apply(ServedModelStatus status, JsonObject isvc, List<JsonObject> services, String servicesError) {
		Endpoint endpoint = services == null
				? new Endpoint(this.mode == Mode.CLUSTER ? clusterUrl(isvc, List.of()) : null,
						"Cannot list Services: " + servicesError)
				: resolve(isvc, services);
		status.endpoint = endpoint.url();
		status.endpointNote = endpoint.note();
		if (services != null) {
			for (JsonObject service : selecting(isvc, services)) {
				if ("LoadBalancer".equals(KubeJson.str(service, "spec", "type"))) {
					status.hostPort = modelPort(service);
					break;
				}
			}
		}
	}

	public Endpoint resolve(JsonObject isvc, List<JsonObject> services) {
		List<JsonObject> selecting = selecting(isvc, services);
		if (this.mode == Mode.CLUSTER) {
			return new Endpoint(clusterUrl(isvc, selecting), null);
		}
		String name = KubeJson.str(isvc, "metadata", "name");
		for (JsonObject service : selecting) {
			if ("LoadBalancer".equals(KubeJson.str(service, "spec", "type"))) {
				int port = modelPort(service);
				String note = null;
				if (KubeJson.objects(service, "status", "loadBalancer", "ingress").isEmpty()) {
					note = "LoadBalancer Service " + KubeJson.str(service, "metadata", "name")
							+ " has no address yet; port " + port + " may already be taken on the node";
				}
				return new Endpoint("http://" + this.hostAddress + ":" + port + "/v1", note);
			}
		}
		for (JsonObject service : selecting) {
			String clusterIp = clusterIp(service);
			if (clusterIp != null && isSemossOwned(service)) {
				return new Endpoint("http://" + clusterIp + portSuffix(modelPort(service)) + "/v1", null);
			}
		}
		for (JsonObject service : selecting) {
			String clusterIp = clusterIp(service);
			if (clusterIp != null && (name + "-predictor").equals(KubeJson.str(service, "metadata", "name"))) {
				return new Endpoint("http://" + clusterIp + portSuffix(modelPort(service)) + "/v1",
						"KServe gives its predictor Service a new ClusterIP on every start; "
								+ "a LoadBalancer Service for the model gives a stable endpoint");
			}
		}
		return new Endpoint(null, "No Service reaches " + name + " from the host while it is stopped");
	}

	private static String clusterUrl(JsonObject isvc, List<JsonObject> selecting) {
		String name = KubeJson.str(isvc, "metadata", "name");
		String namespace = KubeJson.str(isvc, "metadata", "namespace");
		for (JsonObject service : selecting) {
			if (isSemossOwned(service)) {
				return "http://" + KubeJson.str(service, "metadata", "name") + "." + namespace + ".svc.cluster.local"
						+ portSuffix(modelPort(service)) + "/v1";
			}
		}
		String address = KubeJson.str(isvc, "status", "address", "url");
		if (address != null) {
			return (address.endsWith("/") ? address.substring(0, address.length() - 1) : address) + "/v1";
		}
		return "http://" + name + "-predictor." + namespace + ".svc.cluster.local/v1";
	}

	/** The Services whose selector picks the model's predictor pods, by name. */
	static List<JsonObject> selecting(JsonObject isvc, List<JsonObject> services) {
		Map<String, String> labels = predictorPodLabels(isvc);
		return services.stream().filter(service -> selects(service, labels))
				.sorted(Comparator.comparing(service -> String.valueOf(KubeJson.str(service, "metadata", "name"))))
				.toList();
	}

	/** The labels KServe puts on the model's predictor pods, plus the predictor's own. */
	static Map<String, String> predictorPodLabels(JsonObject isvc) {
		String name = KubeJson.str(isvc, "metadata", "name");
		Map<String, String> labels = new LinkedHashMap<>();
		JsonObject extra = KubeJson.obj(isvc, "spec", "predictor", "labels");
		if (extra != null) {
			for (Map.Entry<String, JsonElement> entry : extra.entrySet()) {
				if (entry.getValue().isJsonPrimitive()) {
					labels.put(entry.getKey(), entry.getValue().getAsString());
				}
			}
		}
		labels.put(ServedModelStateResolver.ISVC_LABEL, name);
		labels.put("component", "predictor");
		labels.put("app", "isvc." + name + "-predictor");
		return labels;
	}

	private static boolean selects(JsonObject service, Map<String, String> labels) {
		JsonObject selector = KubeJson.obj(service, "spec", "selector");
		if (selector == null || selector.isEmpty()) {
			return false;
		}
		for (Map.Entry<String, JsonElement> entry : selector.entrySet()) {
			if (!entry.getValue().isJsonPrimitive() || !entry.getValue().getAsString().equals(labels.get(entry.getKey()))) {
				return false;
			}
		}
		return true;
	}

	private static boolean isSemossOwned(JsonObject service) {
		return ServedModelStateResolver.MANAGED_BY_SEMOSS
				.equals(KubeJson.str(service, "metadata", "labels", ServedModelStateResolver.MANAGED_BY_LABEL));
	}

	private static String clusterIp(JsonObject service) {
		String clusterIp = KubeJson.str(service, "spec", "clusterIP");
		return clusterIp == null || clusterIp.isEmpty() || "None".equals(clusterIp) ? null : clusterIp;
	}

	/** The Service port that forwards to vLLM's, else its first port. */
	static int modelPort(JsonObject service) {
		List<JsonObject> ports = KubeJson.objects(service, "spec", "ports");
		for (JsonObject port : ports) {
			String target = KubeJson.str(port, "targetPort");
			if (String.valueOf(MODEL_PORT).equals(target) || MODEL_PORT_NAME.equals(target)) {
				Integer number = KubeJson.integer(port, "port");
				if (number != null) {
					return number;
				}
			}
		}
		Integer first = ports.isEmpty() ? null : KubeJson.integer(ports.get(0), "port");
		return first == null ? 80 : first;
	}

	private static String portSuffix(int port) {
		return port == 80 ? "" : ":" + port;
	}
}
