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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.JsonObject;

import prerna.engine.impl.model.serving.ServingPrometheus.Point;
import prerna.engine.impl.model.serving.ServingPrometheus.Sample;
import prerna.engine.impl.model.serving.ServingPrometheus.Series;
import prerna.util.k8s.KubeJson;

/**
 * What the GPUs are doing, for the GPU card of the Model Serving page: each
 * GPU's utilization, power, temperature and SM clock from the GPU Operator's
 * DCGM exporter, the pods using it, its model, driver and CUDA version from the
 * node labels of GPU feature discovery, and each node's memory from
 * node-exporter, all with a history for sparklines.
 *
 * The GB10 of the DGX Spark has no memory of its own: it allocates from the
 * node's unified memory, and DCGM reports no framebuffer figures for it. The
 * node's memory in use is therefore the GPU's memory figure as well.
 *
 * A pure computation over what {@link ServingGpuMonitor} read.
 */
public final class ServingGpuStatus {

	/** The DCGM fields the card shows, by the key it shows them under */
	enum GpuMetric {
		UTILIZATION("utilization", "DCGM_FI_DEV_GPU_UTIL"),
		POWER("power", "DCGM_FI_DEV_POWER_USAGE"),
		TEMPERATURE("temperature", "DCGM_FI_DEV_GPU_TEMP"),
		SM_CLOCK("smClock", "DCGM_FI_DEV_SM_CLOCK");

		final String key;
		final String field;

		GpuMetric(String key, String field) {
			this.key = key;
			this.field = field;
		}

		static GpuMetric ofField(String field) {
			for (GpuMetric metric : values()) {
				if (metric.field.equals(field)) {
					return metric;
				}
			}
			return null;
		}
	}

	/** How far back the history goes, in 60 steps */
	public enum Window {
		MINUTES_15("15m", Duration.ofMinutes(15)),
		HOUR("1h", Duration.ofHours(1)),
		HOURS_6("6h", Duration.ofHours(6)),
		DAY("24h", Duration.ofHours(24));

		private static final int STEPS = 60;

		final String label;
		final Duration length;

		Window(String label, Duration length) {
			this.label = label;
			this.length = length;
		}

		Duration step() {
			return this.length.dividedBy(STEPS);
		}

		/**
		 * The span each history point averages: a step, and at least a minute so a
		 * 15 s step still covers several of DCGM's 15 s scrapes
		 */
		Duration lookback() {
			Duration step = step();
			return step.compareTo(Duration.ofMinutes(1)) < 0 ? Duration.ofMinutes(1) : step;
		}

		/** The last step boundary, so the points stay put from one read to the next */
		Instant end(Instant now) {
			long step = step().toSeconds();
			return Instant.ofEpochSecond(Math.floorDiv(now.getEpochSecond(), step) * step);
		}

		/** @return the window named 15m, 1h (also for null or blank), 6h or 24h */
		public static Window parse(String value) {
			if (value == null || value.isBlank()) {
				return HOUR;
			}
			for (Window window : values()) {
				if (window.label.equalsIgnoreCase(value.strip())) {
					return window;
				}
			}
			throw new IllegalArgumentException("The window must be 15m, 1h, 6h or 24h; it is " + value);
		}
	}

	/** Where the metrics came from */
	enum Metrics {
		PROMETHEUS, UNAVAILABLE, OFF
	}

	/**
	 * What Prometheus answered: the current DCGM samples, their history by
	 * field, the current node-exporter samples, and the history of the nodes'
	 * available memory
	 */
	record Readings(List<Sample> gpus, Map<GpuMetric, List<Series>> gpuHistory, List<Sample> nodes,
			List<Series> availableHistory) {
	}

	/** A pod using a GPU, and its served model when it is one */
	public record Workload(String namespace, String pod, String container, String servedModel) {
	}

	/**
	 * A GPU. The live values and history are empty without DCGM, and the labels
	 * of GPU feature discovery (CUDA, family, sharing) are null when SEMOSS may
	 * not list the nodes.
	 */
	public record Gpu(String node, String index, String uuid, String product, String driver, String cuda,
			String family, String sharing, Map<GpuMetric, Double> now, List<Workload> usedBy,
			Map<GpuMetric, List<Point>> history) {
	}

	/** A node's memory in bytes, and the history of what is in use */
	public record NodeMemory(String node, long total, long available, List<Point> usedHistory) {
	}

	/** The container of the GPU Operator's DCGM exporter, and the prefix of its pods */
	static final String EXPORTER = "nvidia-dcgm-exporter";
	/**
	 * NVIDIA's DCGM Exporter Dashboard (grafana.com dashboard 12239, revision 2),
	 * which dgx/monitoring.yml provisions with its own uid
	 */
	static final String GRAFANA_DASHBOARD = "/d/Oxed_c6Wz/nvidia-dcgm-exporter-dashboard";
	/** Where k3s's ServiceLB publishes Grafana on every node address (dgx/monitoring.yml) */
	static final int GRAFANA_PORT = 3000;

	static final String GPU_PRODUCT_LABEL = "nvidia.com/gpu.product";
	static final String GPU_COUNT_LABEL = "nvidia.com/gpu.count";
	static final String DRIVER_LABEL = "nvidia.com/cuda.driver-version.full";
	static final String CUDA_LABEL = "nvidia.com/cuda.runtime-version.full";
	static final String FAMILY_LABEL = "nvidia.com/gpu.family";
	static final String SHARING_LABEL = "nvidia.com/gpu.sharing-strategy";

	static final String MEMORY_TOTAL = "node_memory_MemTotal_bytes";
	static final String MEMORY_AVAILABLE = "node_memory_MemAvailable_bytes";
	static final String UNAME = "node_uname_info";

	/** The current DCGM samples, by name, in one query */
	static final String GPUS_NOW = "{__name__=~\"" + String.join("|",
			List.of(GpuMetric.values()).stream().map(metric -> metric.field).toList()) + "\"}";
	/** The current memory of each node, and the node name behind each node-exporter */
	static final String NODES_NOW = "{__name__=~\"" + MEMORY_TOTAL + "|" + MEMORY_AVAILABLE + "|" + UNAME + "\"}";

	private final List<Gpu> gpus = new ArrayList<>();
	private final List<NodeMemory> memory = new ArrayList<>();
	private final Metrics metrics;
	private final String source;
	private final Window window;
	private final Instant now;
	private final String grafanaUrl;
	private final List<String> warnings;

	/**
	 * @param namespace    the namespace of the served models
	 * @param nodes        the cluster's nodes; null when they could not be listed
	 * @param servedModels the served model of each predictor pod in the namespace
	 * @param readings     what Prometheus answered; null without it
	 * @param source       where the metrics come from; null without Prometheus
	 * @param grafanaUrl   Grafana's address; null for port {@value #GRAFANA_PORT} of
	 *                     the address the browser used, {@link ServingSettings#OFF}
	 *                     for no link
	 */
	ServingGpuStatus(String namespace, List<JsonObject> nodes, Map<String, String> servedModels, Readings readings,
			Metrics metrics, String source, Window window, Instant now, String grafanaUrl, List<String> warnings) {
		this.metrics = metrics;
		this.source = source;
		this.window = window;
		this.now = now;
		this.grafanaUrl = grafanaUrl;
		this.warnings = new ArrayList<>(warnings);
		Map<String, JsonObject> nodeLabels = new LinkedHashMap<>();
		if (nodes != null) {
			for (JsonObject node : nodes) {
				JsonObject labels = KubeJson.obj(node, "metadata", "labels");
				nodeLabels.put(KubeJson.str(node, "metadata", "name"), labels == null ? new JsonObject() : labels);
			}
		}
		if (readings != null) {
			readGpus(namespace, readings, servedModels, nodeLabels);
			readMemory(readings);
			if (readings.gpus().isEmpty()) {
				this.warnings.add("Prometheus has no current GPU samples. Is the GPU Operator's DCGM exporter running? "
						+ "(kubectl -n gpu-operator get pods)");
			}
			if (this.memory.isEmpty()) {
				this.warnings.add("Prometheus has no memory samples from node-exporter, so the node's memory is not shown");
			}
		}
		if (this.gpus.isEmpty()) {
			gpusFromLabels(nodeLabels);
		}
	}

	/**
	 * Run the queries: the current values, and each field's history averaged per
	 * step, which keeps a short peak visible between steps.
	 *
	 * @throws ServingPrometheus.QueryException when Prometheus gives no answer
	 */
	static Readings read(ServingPrometheus prometheus, Window window, Instant now) {
		Instant end = window.end(now);
		Instant start = end.minus(window.length);
		String lookback = "[" + window.lookback().toSeconds() + "s]";
		List<Sample> gpus = prometheus.instant(GPUS_NOW, now);
		Map<GpuMetric, List<Series>> gpuHistory = new EnumMap<>(GpuMetric.class);
		for (GpuMetric metric : GpuMetric.values()) {
			gpuHistory.put(metric,
					prometheus.range("avg_over_time(" + metric.field + lookback + ")", start, end, window.step()));
		}
		List<Sample> nodes = prometheus.instant(NODES_NOW, now);
		List<Series> available = prometheus.range("avg_over_time(" + MEMORY_AVAILABLE + lookback + ")", start, end,
				window.step());
		return new Readings(gpus, gpuHistory, nodes, available);
	}

	/** The pods of the namespace that DCGM says use a GPU, whose served models are worth looking up */
	static Set<String> podsIn(Readings readings, String namespace) {
		Set<String> pods = new HashSet<>();
		for (Sample sample : readings.gpus()) {
			Workload workload = workload(sample.labels(), namespace, Map.of());
			if (workload != null && namespace.equals(workload.namespace())) {
				pods.add(workload.pod());
			}
		}
		return pods;
	}

	/**
	 * The pod a DCGM series is attributed to. DCGM labels a GPU in use with its
	 * pod, namespace and container; with honorLabels (dgx/monitoring.yml) they
	 * stay {@code pod}, {@code namespace} and {@code container}, otherwise
	 * Prometheus renames them {@code exported_*}. A GPU no pod uses gets the
	 * exporter's own pod from Prometheus instead.
	 *
	 * @return null for a GPU no pod uses
	 */
	static Workload workload(Map<String, String> labels, String namespace, Map<String, String> servedModels) {
		String pod = first(labels, "exported_pod", "pod");
		String container = first(labels, "exported_container", "container");
		String podNamespace = first(labels, "exported_namespace", "namespace");
		if (pod == null || EXPORTER.equals(container) || pod.startsWith(EXPORTER + "-")) {
			return null;
		}
		return new Workload(podNamespace, pod, container,
				namespace != null && namespace.equals(podNamespace) ? servedModels.get(pod) : null);
	}

	private void readGpus(String namespace, Readings readings, Map<String, String> servedModels,
			Map<String, JsonObject> nodeLabels) {
		Map<String, Map<String, String>> identities = new LinkedHashMap<>();
		Map<String, Map<GpuMetric, Double>> values = new LinkedHashMap<>();
		Map<String, Set<Workload>> workloads = new LinkedHashMap<>();
		for (Sample sample : readings.gpus()) {
			GpuMetric metric = GpuMetric.ofField(sample.labels().get("__name__"));
			if (metric == null) {
				continue;
			}
			String key = gpuKey(sample.labels());
			identities.putIfAbsent(key, sample.labels());
			// a GPU shared by time-slicing has a series per pod, all with the same value
			values.computeIfAbsent(key, k -> new EnumMap<>(GpuMetric.class)).merge(metric, sample.value(), Math::max);
			Workload workload = workload(sample.labels(), namespace, servedModels);
			Set<Workload> using = workloads.computeIfAbsent(key, k -> new LinkedHashSet<>());
			if (workload != null) {
				using.add(workload);
			}
		}
		// a series ends when the pod using the GPU changes, so the history joins them by GPU
		Map<String, Map<GpuMetric, TreeMap<Long, Double>>> history = new LinkedHashMap<>();
		for (Map.Entry<GpuMetric, List<Series>> entry : readings.gpuHistory().entrySet()) {
			for (Series series : entry.getValue()) {
				String key = gpuKey(series.labels());
				identities.putIfAbsent(key, series.labels());
				TreeMap<Long, Double> points = history.computeIfAbsent(key, k -> new EnumMap<>(GpuMetric.class))
						.computeIfAbsent(entry.getKey(), k -> new TreeMap<>());
				for (Point point : series.points()) {
					points.merge(point.time(), point.value(), Math::max);
				}
			}
		}
		for (Map.Entry<String, Map<String, String>> identity : identities.entrySet()) {
			Map<String, String> labels = identity.getValue();
			String node = first(labels, "Hostname", "hostname");
			JsonObject gfd = nodeLabels.get(node);
			Map<GpuMetric, List<Point>> series = new EnumMap<>(GpuMetric.class);
			history.getOrDefault(identity.getKey(), Map.of()).forEach((metric, points) -> series.put(metric,
					points.entrySet().stream().map(point -> new Point(point.getKey(), point.getValue())).toList()));
			this.gpus.add(new Gpu(node, first(labels, "gpu"), first(labels, "UUID", "uuid"),
					orLabel(first(labels, "modelName"), gfd, GPU_PRODUCT_LABEL),
					orLabel(first(labels, "DCGM_FI_DRIVER_VERSION"), gfd, DRIVER_LABEL), label(gfd, CUDA_LABEL),
					label(gfd, FAMILY_LABEL), label(gfd, SHARING_LABEL),
					values.getOrDefault(identity.getKey(), Map.of()),
					List.copyOf(workloads.getOrDefault(identity.getKey(), Set.of())), series));
		}
		this.gpus.sort(Comparator.comparing((Gpu gpu) -> gpu.node() == null ? "" : gpu.node())
				.thenComparing(gpu -> index(gpu.index())));
	}

	/** Without DCGM, the GPUs the node labels count, without live values */
	private void gpusFromLabels(Map<String, JsonObject> nodeLabels) {
		for (Map.Entry<String, JsonObject> node : nodeLabels.entrySet()) {
			JsonObject gfd = node.getValue();
			int count = count(label(gfd, GPU_COUNT_LABEL));
			for (int index = 0; index < count; index++) {
				this.gpus.add(new Gpu(node.getKey(), Integer.toString(index), null, label(gfd, GPU_PRODUCT_LABEL),
						label(gfd, DRIVER_LABEL), label(gfd, CUDA_LABEL), label(gfd, FAMILY_LABEL),
						label(gfd, SHARING_LABEL), Map.of(), List.of(), Map.of()));
			}
		}
	}

	private void readMemory(Readings readings) {
		Map<String, String> nodeOf = new LinkedHashMap<>();
		Map<String, Double> totals = new LinkedHashMap<>();
		Map<String, Double> available = new LinkedHashMap<>();
		for (Sample sample : readings.nodes()) {
			String instance = sample.labels().get("instance");
			if (instance == null) {
				continue;
			}
			switch (String.valueOf(sample.labels().get("__name__"))) {
			case UNAME -> {
				String nodename = sample.labels().get("nodename");
				if (nodename != null) {
					nodeOf.put(instance, nodename);
				}
			}
			case MEMORY_TOTAL -> totals.put(instance, sample.value());
			case MEMORY_AVAILABLE -> available.put(instance, sample.value());
			default -> {
				// not asked for
			}
			}
		}
		Map<String, TreeMap<Long, Double>> used = new LinkedHashMap<>();
		for (Series series : readings.availableHistory()) {
			String instance = series.labels().get("instance");
			Double total = totals.get(instance);
			if (total == null) {
				continue;
			}
			TreeMap<Long, Double> points = used.computeIfAbsent(nodeOf.getOrDefault(instance, instance),
					k -> new TreeMap<>());
			for (Point point : series.points()) {
				points.merge(point.time(), Math.max(0, total - point.value()), Math::max);
			}
		}
		for (Map.Entry<String, Double> total : totals.entrySet()) {
			Double free = available.get(total.getKey());
			if (free == null) {
				continue;
			}
			String node = nodeOf.getOrDefault(total.getKey(), total.getKey());
			List<Point> history = used.getOrDefault(node, new TreeMap<>()).entrySet().stream()
					.map(point -> new Point(point.getKey(), point.getValue())).toList();
			this.memory.add(new NodeMemory(node, Math.round(total.getValue()), Math.round(free), history));
		}
		this.memory.sort(Comparator.comparing(NodeMemory::node));
	}

	/** A GPU is its node and UUID; DCGM's index stands in for a missing UUID */
	private static String gpuKey(Map<String, String> labels) {
		String id = first(labels, "UUID", "uuid");
		return first(labels, "Hostname", "hostname") + "/" + (id == null ? first(labels, "gpu") : id);
	}

	private static String first(Map<String, String> labels, String... names) {
		for (String name : names) {
			String value = labels.get(name);
			if (value != null && !value.isBlank()) {
				return value;
			}
		}
		return null;
	}

	private static String label(JsonObject labels, String name) {
		String value = labels == null ? null : KubeJson.str(labels, name);
		return value == null || value.isBlank() ? null : value;
	}

	private static String orLabel(String value, JsonObject labels, String name) {
		return value != null ? value : label(labels, name);
	}

	private static int count(String value) {
		try {
			return value == null ? 0 : Math.max(0, Integer.parseInt(value.strip()));
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	/** GPU 10 after GPU 9 */
	private static long index(String value) {
		try {
			return value == null ? Long.MAX_VALUE : Long.parseLong(value);
		} catch (NumberFormatException e) {
			return Long.MAX_VALUE;
		}
	}

	private static Double round(Double value) {
		return value == null ? null : Math.round(value * 100) / 100.0;
	}

	private static List<List<Number>> points(List<Point> points) {
		List<List<Number>> list = new ArrayList<>();
		if (points != null) {
			for (Point point : points) {
				list.add(List.of(point.time(), round(point.value())));
			}
		}
		return list;
	}

	public List<Gpu> getGpus() {
		return List.copyOf(this.gpus);
	}

	public List<NodeMemory> getMemory() {
		return List.copyOf(this.memory);
	}

	public List<String> getWarnings() {
		return List.copyOf(this.warnings);
	}

	/**
	 * For GetServingGpuStatus: the GPUs and nodes, where the metrics came from,
	 * the window of the history ({@code [seconds, value]} pairs), and the
	 * Grafana dashboard to link to.
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("metrics", this.metrics.name().toLowerCase(Locale.ROOT));
		map.put("source", this.source);
		map.put("readAt", this.now.toString());
		map.put("window", this.window.label);
		map.put("stepSeconds", this.window.step().toSeconds());
		map.put("gpus", this.gpus.stream().map(gpu -> {
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("node", gpu.node());
			entry.put("index", gpu.index());
			entry.put("uuid", gpu.uuid());
			entry.put("product", gpu.product());
			entry.put("driver", gpu.driver());
			entry.put("cuda", gpu.cuda());
			entry.put("family", gpu.family());
			entry.put("sharing", gpu.sharing());
			for (GpuMetric metric : GpuMetric.values()) {
				entry.put(metric.key, round(gpu.now().get(metric)));
			}
			entry.put("usedBy", gpu.usedBy().stream().map(workload -> {
				Map<String, Object> use = new LinkedHashMap<>();
				use.put("namespace", workload.namespace());
				use.put("pod", workload.pod());
				use.put("container", workload.container());
				use.put("servedModel", workload.servedModel());
				return use;
			}).toList());
			Map<String, Object> history = new LinkedHashMap<>();
			for (GpuMetric metric : GpuMetric.values()) {
				history.put(metric.key, points(gpu.history().get(metric)));
			}
			entry.put("history", history);
			return entry;
		}).toList());
		map.put("nodes", this.memory.stream().map(node -> {
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("node", node.node());
			entry.put("memoryTotal", node.total());
			entry.put("memoryAvailable", node.available());
			entry.put("memoryUsed", Math.max(0, node.total() - node.available()));
			entry.put("history", Map.of("memoryUsed", points(node.usedHistory())));
			return entry;
		}).toList());
		if (ServingSettings.OFF.equals(this.grafanaUrl)) {
			map.put("grafana", null);
		} else {
			Map<String, Object> grafana = new LinkedHashMap<>();
			grafana.put("url", this.grafanaUrl);
			grafana.put("port", GRAFANA_PORT);
			grafana.put("path", GRAFANA_DASHBOARD);
			map.put("grafana", grafana);
		}
		map.put("warnings", List.copyOf(this.warnings));
		return map;
	}
}
