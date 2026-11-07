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
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.util.k8s.KubeApiException;
import prerna.util.k8s.KubeClient;
import prerna.util.k8s.KubeJson;

/**
 * Reads the cluster's Prometheus: kube-prometheus-stack, release
 * {@code monitoring} (dgx/monitoring.yml), which scrapes the GPU Operator's
 * DCGM exporter and node-exporter every 15 s. SEMOSS in a pod reaches it by
 * cluster DNS. SEMOSS on the host goes through the Kubernetes API's service
 * proxy, which an admin kubeconfig may use; the semoss ServiceAccount may not,
 * so with its token {@value ServingSettings#KSERVE_PROMETHEUS_URL} names
 * Prometheus's address instead.
 */
final class ServingPrometheus {

	static final String NAMESPACE = "monitoring";
	static final String SERVICE = "monitoring-kube-prometheus-prometheus";
	static final int PORT = 9090;
	/** Prometheus by cluster DNS, for SEMOSS in a pod */
	static final String CLUSTER_URL = "http://" + SERVICE + "." + NAMESPACE + ".svc.cluster.local:" + PORT;

	/** Never through a proxy: Prometheus is in the cluster or on the node, like the models */
	private static final HttpClient HTTP = HttpClient.newBuilder().proxy(HttpClient.Builder.NO_PROXY)
			.connectTimeout(Duration.ofSeconds(2)).build();
	private static final Duration TIMEOUT = Duration.ofSeconds(5);
	private static final int MAX_ERROR = 300;

	/** One sample of an instant query */
	record Sample(Map<String, String> labels, double value) {
	}

	/** One point of a range query, at seconds since the epoch */
	record Point(long time, double value) {
	}

	/** One series of a range query, its points in time order */
	record Series(Map<String, String> labels, List<Point> points) {
	}

	/** Prometheus gave no usable answer; the message says what to do about it */
	static final class QueryException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		QueryException(String message) {
			super(message);
		}
	}

	/** One GET of Prometheus's HTTP API, such as {@code api/v1/query}; tests replace it */
	interface Transport {
		/**
		 * @return the body of a successful answer
		 * @throws QueryException when there is none
		 */
		String get(String api, Map<String, String> parameters);
	}

	private final Transport transport;
	private final String source;
	private final String problem;

	/**
	 * @param transport null for no Prometheus
	 * @param source    where the metrics come from, for the GPU card
	 * @param problem   why there is no Prometheus when that is a mistake, such as
	 *                  a malformed setting; null otherwise
	 */
	ServingPrometheus(Transport transport, String source, String problem) {
		this.transport = transport;
		this.source = source;
		this.problem = problem;
	}

	/**
	 * Prometheus as {@value ServingSettings#KSERVE_PROMETHEUS_URL} says: off, at a
	 * URL, or found where SEMOSS runs (by cluster DNS in a pod, through the
	 * Kubernetes API on the host). A malformed setting costs the GPU metrics only.
	 */
	static ServingPrometheus fromSettings(KubeClient client, ServedModelEndpoints.Mode mode) {
		try {
			return choose(ServingSettings.getPrometheusUrl(), client, mode);
		} catch (IllegalStateException e) {
			return new ServingPrometheus(null, null, e.getMessage());
		}
	}

	/** @param url as {@link ServingSettings#getPrometheusUrl()} gives it: a URL, null for auto, or off */
	static ServingPrometheus choose(String url, KubeClient client, ServedModelEndpoints.Mode mode) {
		if (ServingSettings.OFF.equals(url)) {
			return off();
		}
		if (url != null) {
			return at(url);
		}
		return mode == ServedModelEndpoints.Mode.CLUSTER ? at(CLUSTER_URL) : throughKubernetes(client);
	}

	/** No metrics, on purpose */
	static ServingPrometheus off() {
		return new ServingPrometheus(null, null, null);
	}

	/** Prometheus at its own address: cluster DNS, a ClusterIP, or anything else */
	static ServingPrometheus at(String url) {
		return new ServingPrometheus(direct(url), "Prometheus at " + url, null);
	}

	/** Prometheus through the Kubernetes API's service proxy, with SEMOSS's own credentials */
	static ServingPrometheus throughKubernetes(KubeClient client) {
		return new ServingPrometheus(proxy(client),
				"Prometheus " + NAMESPACE + "/" + SERVICE + ":" + PORT + " through the Kubernetes API", null);
	}

	/** Turned off on purpose, which is not worth a warning */
	boolean isOff() {
		return this.transport == null && this.problem == null;
	}

	/** Where the metrics come from; null without Prometheus */
	String getSource() {
		return this.source;
	}

	/** The samples of an instant query, at the given moment */
	List<Sample> instant(String promql, Instant time) {
		Map<String, String> parameters = new LinkedHashMap<>();
		parameters.put("query", promql);
		parameters.put("time", Long.toString(time.getEpochSecond()));
		return parseVector(get("api/v1/query", parameters));
	}

	/** The series of a range query, evaluated every step from start to end */
	List<Series> range(String promql, Instant start, Instant end, Duration step) {
		Map<String, String> parameters = new LinkedHashMap<>();
		parameters.put("query", promql);
		parameters.put("start", Long.toString(start.getEpochSecond()));
		parameters.put("end", Long.toString(end.getEpochSecond()));
		parameters.put("step", step.toSeconds() + "s");
		return parseMatrix(get("api/v1/query_range", parameters));
	}

	private String get(String api, Map<String, String> parameters) {
		if (this.transport == null) {
			throw new QueryException(this.problem != null ? this.problem
					: "Prometheus is turned off (" + ServingSettings.KSERVE_PROMETHEUS_URL + "=off)");
		}
		return this.transport.get(api, parameters);
	}

	/** An instant query's samples; NaN and infinite values are left out */
	static List<Sample> parseVector(String body) {
		List<Sample> samples = new ArrayList<>();
		for (JsonObject result : KubeJson.objects(data(body, "vector"), "result")) {
			Point point = point(KubeJson.element(result, "value"));
			if (point != null) {
				samples.add(new Sample(labels(result), point.value()));
			}
		}
		return samples;
	}

	/** A range query's series; NaN and infinite points are left out */
	static List<Series> parseMatrix(String body) {
		List<Series> series = new ArrayList<>();
		for (JsonObject result : KubeJson.objects(data(body, "matrix"), "result")) {
			List<Point> points = new ArrayList<>();
			JsonElement values = KubeJson.element(result, "values");
			if (values != null && values.isJsonArray()) {
				for (JsonElement value : values.getAsJsonArray()) {
					Point point = point(value);
					if (point != null) {
						points.add(point);
					}
				}
			}
			series.add(new Series(labels(result), points));
		}
		return series;
	}

	/** The {@code data} of a successful answer of the expected type */
	private static JsonObject data(String body, String resultType) {
		JsonObject reply = null;
		try {
			JsonElement parsed = JsonParser.parseString(body);
			if (parsed.isJsonObject()) {
				reply = parsed.getAsJsonObject();
			}
		} catch (RuntimeException e) {
			// reported below
		}
		if (reply == null) {
			throw new QueryException("Prometheus did not answer with JSON: " + shorten(body));
		}
		if (!"success".equals(KubeJson.str(reply, "status"))) {
			String error = KubeJson.str(reply, "error");
			throw new QueryException("Prometheus refused the query: " + (error == null ? shorten(body) : error));
		}
		JsonObject data = KubeJson.obj(reply, "data");
		String type = data == null ? null : KubeJson.str(data, "resultType");
		if (!resultType.equals(type)) {
			throw new QueryException("Prometheus answered with a " + type + " where a " + resultType + " was expected");
		}
		return data;
	}

	/** A {@code [seconds, "value"]} pair; null when malformed, NaN or infinite */
	private static Point point(JsonElement pair) {
		if (pair == null || !pair.isJsonArray() || pair.getAsJsonArray().size() != 2) {
			return null;
		}
		JsonArray array = pair.getAsJsonArray();
		try {
			double value = Double.parseDouble(array.get(1).getAsString());
			if (Double.isNaN(value) || Double.isInfinite(value)) {
				return null;
			}
			return new Point(array.get(0).getAsBigDecimal().longValue(), value);
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static Map<String, String> labels(JsonObject result) {
		Map<String, String> labels = new LinkedHashMap<>();
		JsonObject metric = KubeJson.obj(result, "metric");
		if (metric != null) {
			for (Map.Entry<String, JsonElement> label : metric.entrySet()) {
				if (label.getValue().isJsonPrimitive()) {
					labels.put(label.getKey(), label.getValue().getAsString());
				}
			}
		}
		return labels;
	}

	private static Transport direct(String url) {
		return (api, parameters) -> {
			URI uri = URI.create(url + "/" + api + "?" + parameters.entrySet().stream()
					.map(parameter -> parameter.getKey() + "=" + URLEncoder.encode(parameter.getValue(), StandardCharsets.UTF_8))
					.collect(Collectors.joining("&")));
			HttpResponse<String> response;
			try {
				response = HTTP.send(HttpRequest.newBuilder(uri).timeout(TIMEOUT).header("Accept", "application/json")
						.GET().build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			} catch (IOException e) {
				throw new QueryException("Prometheus did not answer at " + url + ": " + e.getClass().getSimpleName()
						+ (e.getMessage() == null ? "" : " (" + e.getMessage() + ")")
						+ (url.equals(CLUSTER_URL) ? "; is dgx/monitoring.yml applied?" : ""));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new QueryException("Interrupted while reading Prometheus at " + url);
			}
			if (response.statusCode() < 200 || response.statusCode() >= 300) {
				throw new QueryException("Prometheus at " + url + " answered HTTP " + response.statusCode() + ": "
						+ errorOf(response.body()));
			}
			return response.body();
		};
	}

	private static Transport proxy(KubeClient client) {
		return (api, parameters) -> {
			List<String> segments = new ArrayList<>(
					List.of("api", "v1", "namespaces", NAMESPACE, "services", SERVICE + ":" + PORT, "proxy"));
			segments.addAll(List.of(api.split("/")));
			KubeClient.Request request = KubeClient.path(segments.toArray(String[]::new));
			parameters.forEach(request::query);
			try {
				return client.getText(request);
			} catch (KubeApiException e) {
				throw new QueryException(describeProxyFailure(e));
			}
		};
	}

	/** Why the Kubernetes API did not pass the query on, and what to do about it */
	static String describeProxyFailure(KubeApiException e) {
		String where = NAMESPACE + "/" + SERVICE + ":" + PORT;
		if (e.isUnreachable()) {
			return e.getMessage();
		}
		if (e.isUnauthorized()) {
			return "SEMOSS may not reach Prometheus through the Kubernetes API (HTTP " + e.getCode()
					+ "), as with the semoss ServiceAccount's token. Set " + ServingSettings.KSERVE_PROMETHEUS_URL
					+ " to Prometheus's address, such as http://<its ClusterIP>:" + PORT + " (kubectl -n " + NAMESPACE
					+ " get service " + SERVICE + ")";
		}
		if (e.isNotFound()) {
			return "There is no Prometheus at " + where + " (HTTP 404). Apply dgx/monitoring.yml, or set "
					+ ServingSettings.KSERVE_PROMETHEUS_URL + " to where it is";
		}
		if (e.getCode() == 503) {
			return "Prometheus " + where + " is not running (HTTP 503): " + e.getMessage();
		}
		return "Prometheus " + where + " answered HTTP " + e.getCode() + ": " + errorOf(e.getMessage());
	}

	/** Prometheus's own error message from an error answer, else the answer shortened */
	private static String errorOf(String body) {
		try {
			JsonElement parsed = JsonParser.parseString(body);
			if (parsed.isJsonObject()) {
				String error = KubeJson.str(parsed.getAsJsonObject(), "error");
				if (error != null) {
					return error;
				}
			}
		} catch (RuntimeException e) {
			// not JSON
		}
		return shorten(body);
	}

	private static String shorten(String text) {
		if (text == null || text.isBlank()) {
			return "an empty answer";
		}
		String stripped = text.strip();
		return stripped.length() > MAX_ERROR ? stripped.substring(0, MAX_ERROR) + "..." : stripped;
	}
}
