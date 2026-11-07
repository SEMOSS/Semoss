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

import static prerna.engine.impl.model.serving.ServedModelStateResolver.ISVC_LABEL;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.LAST_ACTION_ANNOTATION;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.MANAGED_BY_LABEL;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.MANAGED_BY_SEMOSS;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.START_STATS_ANNOTATION;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.STOP_ANNOTATION;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.util.k8s.KubeApiException;
import prerna.util.k8s.KubeClient;
import prerna.util.k8s.KubeConfig;
import prerna.util.k8s.KubeConnection;
import prerna.util.k8s.KubeJson;

/**
 * Lists, watches, starts and stops the models KServe serves in one namespace.
 *
 * A model is an InferenceService that stays defined while stopped: stopping
 * sets its {@code serving.kserve.io/stop} annotation to {@code "true"}, and KServe
 * then deletes the predictor's Deployment, pods and Service, which frees the
 * GPU; starting sets it back to {@code "false"}. Everything else is read from
 * the cluster on each call, so SEMOSS keeps no state of its own and a restart
 * loses nothing.
 */
public final class ServedModelService {

	private static final Logger classLogger = LogManager.getLogger(ServedModelService.class);

	public static final String MAIN_CONTAINER = ServedModelStateResolver.MAIN_CONTAINER;
	public static final String INIT_CONTAINER = ServedModelStateResolver.INIT_CONTAINER;
	public static final int DEFAULT_LOG_LINES = 200;
	public static final int MAX_LOG_LINES = 5000;

	/** InferenceService names: DNS-1035 labels */
	private static final Pattern NAME = Pattern.compile("[a-z]([-a-z0-9]{0,61}[a-z0-9])?");
	private static final int STARTUP_LOG_BYTES = 256 * 1024;
	private static final int LOG_BYTES = 1024 * 1024;
	private static final int FAILURE_LOG_LINES = 60;
	/** access lines are dropped after reading, so read more lines to still return the requested count */
	private static final int ACCESS_LOG_OVERREAD = 5;
	/** how long a start waits for stopping models to free the GPU: a stop takes about 4 s on this box */
	private static final int GONE_POLLS = 60;
	private static final long GONE_POLL_MILLIS = 1000;
	/** how long a define waits for ServiceLB to bind a published port: a few seconds on this box */
	private static final int ADDRESS_POLLS = 30;
	/**
	 * how long after KServe marks a model Ready an endpoint that refuses still means
	 * "not routed yet" (2 s on this box) rather than a network problem
	 */
	private static final int ROUTE_GRACE_SECONDS = 60;

	/** How the service waits between polls; tests replace it */
	interface Sleeper {
		void sleep(long millis) throws InterruptedException;
	}

	private static ServedModelService instance;

	private final KubeClient client;
	private final String namespace;
	private final ServedModelEndpoints endpoints;
	private final String catalogLocation;
	private final Set<String> runtimes;
	private final Set<String> weightVolumes;
	private final double memoryBudget;
	private final ServingSettings.PortRange hostPorts;
	private final Clock clock;
	private Sleeper sleeper = Thread::sleep;
	private ServedModelEndpoints.Probe probe = ServedModelEndpoints.HTTP_PROBE;
	/** Made from the settings on first use; tests set their own */
	private ServingGpuMonitor gpuMonitor;

	ServedModelService(KubeClient client, String namespace, ServedModelEndpoints endpoints, String catalogLocation,
			Set<String> runtimes, Clock clock) {
		this(client, namespace, endpoints, catalogLocation, runtimes,
				Set.of(ServingSettings.DEFAULT_WEIGHT_VOLUME), ServingSettings.DEFAULT_MEMORY_BUDGET,
				ServingSettings.DEFAULT_HOST_PORT_RANGE, clock);
	}

	/**
	 * @param weightVolumes the PersistentVolumeClaims pvc:// catalog entries may use
	 * @param memoryBudget  the share of the unified memory all models may use
	 * @param hostPorts     the node ports models may be published on
	 */
	ServedModelService(KubeClient client, String namespace, ServedModelEndpoints endpoints, String catalogLocation,
			Set<String> runtimes, Set<String> weightVolumes, double memoryBudget, ServingSettings.PortRange hostPorts,
			Clock clock) {
		this.client = client;
		this.namespace = namespace;
		this.endpoints = endpoints;
		this.catalogLocation = catalogLocation;
		this.runtimes = runtimes;
		this.weightVolumes = weightVolumes;
		this.memoryBudget = memoryBudget;
		this.hostPorts = hostPorts;
		this.clock = clock;
	}

	void setSleeper(Sleeper sleeper) {
		this.sleeper = sleeper;
	}

	void setProbe(ServedModelEndpoints.Probe probe) {
		this.probe = probe;
	}

	void setGpuMonitor(ServingGpuMonitor gpuMonitor) {
		this.gpuMonitor = gpuMonitor;
	}

	/**
	 * @return the service configured by {@link ServingSettings}, created on first
	 *         use
	 * @throws IllegalStateException    when model serving is not enabled
	 * @throws IllegalArgumentException when the credentials cannot be read
	 */
	public static synchronized ServedModelService getInstance() {
		if (instance == null) {
			instance = fromSettings();
		}
		return instance;
	}

	static synchronized void setInstance(ServedModelService service) {
		instance = service;
	}

	private static ServedModelService fromSettings() {
		if (!ServingSettings.isEnabled()) {
			throw new IllegalStateException("Model serving is not configured: set " + ServingSettings.KSERVE_ENABLED
					+ "=true in RDF_Map.prop or the environment");
		}
		KubeConnection connection = switch (ServingSettings.getAuth()) {
		case IN_CLUSTER -> KubeConnection.inCluster();
		case KUBECONFIG -> KubeConnection
				.fromKubeConfig(KubeConfig.load(ServingSettings.getKubeconfig(), ServingSettings.getKubeContext()));
		};
		String namespace = ServingSettings.getNamespace();
		ServedModelEndpoints endpoints = new ServedModelEndpoints(ServingSettings.getNetworkMode(),
				ServingSettings.getHostAddress());
		classLogger.info("Model serving uses {}, namespace {}, {} endpoints", connection.describe(), namespace,
				endpoints.getMode());
		return new ServedModelService(new KubeClient(connection), namespace, endpoints, ServingSettings.getCatalog(),
				ServingSettings.getRuntimes(), ServingSettings.getWeightVolumes(), ServingSettings.getMemoryBudget(),
				ServingSettings.getHostPortRange(), Clock.systemUTC());
	}

	public String getNamespace() {
		return this.namespace;
	}

	/**
	 * Every InferenceService of the namespace with a state derived from its pods
	 * alone (two API calls for all models); {@link #status(String)} adds events and
	 * logs for the finer phases.
	 */
	public List<ServedModelStatus> list() {
		List<JsonObject> services = KubeJson.objects(this.client.get(inferenceServices()), "items");
		Map<String, List<JsonObject>> podsByModel = new HashMap<>();
		for (JsonObject pod : KubeJson.objects(this.client.get(pods().query("labelSelector", ISVC_LABEL)), "items")) {
			String owner = KubeJson.str(pod, "metadata", "labels", ISVC_LABEL);
			if (owner != null) {
				podsByModel.computeIfAbsent(owner, key -> new ArrayList<>()).add(pod);
			}
		}
		Instant now = this.clock.instant();
		Services kubeServices = services();
		List<ServedModelStatus> result = new ArrayList<>();
		for (JsonObject service : services) {
			String name = KubeJson.str(service, "metadata", "name");
			ServedModelStatus status = ServedModelStateResolver.resolve(service,
					podsByModel.getOrDefault(name, List.of()), List.of(), null, now);
			this.endpoints.apply(status, service, kubeServices.items(), kubeServices.error());
			// a list probes only models that just turned ready, so it stays cheap
			confirmReachable(status, false);
			result.add(status);
		}
		result.sort(Comparator.comparing(ServedModelStatus::getServedModel));
		return result;
	}

	public ServedModelStatus status(String name) {
		return observe(getInferenceService(name));
	}

	public ServedModelChange start(String name, User user) {
		return start(name, user, false);
	}

	/**
	 * Start a model if the GPUs and the memory budget allow it
	 * ({@link ServingCapacity}); otherwise refuse, naming what is in its way, and
	 * leave it stopped. Models that are already stopping are waited for, up to a
	 * minute. A model that failed is stopped and started again.
	 *
	 * @param replace stop the models in its way first (admins swapping models); for
	 *                a model that waits for the GPU, stop what holds it
	 * @throws ServingCapacityException when the model cannot start now
	 */
	public ServedModelChange start(String name, User user, boolean replace) {
		JsonObject service = getInferenceService(name);
		if (KubeJson.str(service, "metadata", "deletionTimestamp") != null) {
			throw new IllegalArgumentException("Served model " + name + " is being removed");
		}
		boolean restarted = false;
		if (!isStopped(service)) {
			ServedModelStatus status = observe(service);
			if (status.getState() == ServedModelState.WAITING_FOR_GPU && replace) {
				// its pod is scheduled once what holds the GPU is gone; the model itself does not change
				List<String> replaced = stopAll(capacity().check(name).blocking(), user);
				return new ServedModelChange(status(name), false, replaced, false);
			}
			if (status.getState() != ServedModelState.FAILED) {
				return new ServedModelChange(status, false);
			}
			// a failed model starts again: stop it, and start it once its pods are gone
			setStopped(name, true, user);
			waitForPodsGone(List.of(name));
			restarted = true;
		}
		ServingCapacity.Decision decision = capacity().check(name);
		List<String> replaced = List.of();
		if (!decision.allowed() && (decision.waitable()
				|| (replace && decision.unmanaged().isEmpty() && !decision.blocking().isEmpty()))) {
			if (!decision.waitable()) {
				replaced = stopAll(decision.blocking(), user);
			}
			waitForPodsGone(decision.blocking());
			decision = capacity().check(name);
		}
		if (!decision.allowed()) {
			String message = decision.reason();
			if (!replaced.isEmpty()) {
				message = "Stopped " + String.join(", ", replaced) + " to make room, but " + name
						+ " cannot start yet: " + message;
			}
			if (restarted) {
				message = name + " had failed and is now stopped; it could not start again: " + message;
			}
			throw new ServingCapacityException(message, decision, replaced);
		}
		ServedModelChange change = setStopped(name, false, user);
		return new ServedModelChange(change.status(), true, replaced, restarted);
	}

	public ServedModelChange stop(String name, User user) {
		return setStopped(name, true, user);
	}

	/** The GPUs and the memory budget, and what holds them; see {@link ServingCapacity}. */
	public ServingCapacity capacity() {
		List<String> warnings = new ArrayList<>();
		List<ServingCapacity.Node> nodes = nodes(warnings);
		List<JsonObject> services = KubeJson.objects(this.client.get(inferenceServices()), "items");
		List<JsonObject> pods = KubeJson.objects(this.client.get(pods()), "items");
		return new ServingCapacity(this.namespace, nodes, services, pods, runtimeFractions(services, warnings),
				this.memoryBudget, this.clock.instant(), warnings);
	}

	/**
	 * What the GPUs are doing: DCGM's live values and history from Prometheus, the
	 * pods using each GPU, and the nodes' memory; see {@link ServingGpuStatus}.
	 */
	public ServingGpuStatus gpuStatus(ServingGpuStatus.Window window) {
		ServingGpuMonitor monitor;
		synchronized (this) {
			if (this.gpuMonitor == null) {
				this.gpuMonitor = ServingGpuMonitor.fromSettings(this.client, this.namespace, this.endpoints.getMode());
			}
			monitor = this.gpuMonitor;
		}
		return monitor.read(window, this.clock.instant());
	}

	/**
	 * The nodes' GPUs; null, with a warning, when SEMOSS may not list nodes (the
	 * ClusterRole in semoss-rbac.yml), which skips the GPU check rather than every
	 * start.
	 */
	private List<ServingCapacity.Node> nodes(List<String> warnings) {
		try {
			return ServingCapacity.nodes(KubeJson.objects(this.client.get(KubeClient.path("api", "v1", "nodes")), "items"));
		} catch (KubeApiException e) {
			if (!e.isUnauthorized()) {
				throw e;
			}
			classLogger.warn("Cannot list the nodes, so the GPU check is skipped: {}", e.getMessage());
			warnings.add("SEMOSS may not list the cluster's nodes (HTTP " + e.getCode()
					+ "), so it does not count GPUs before a start; apply semoss-rbac.yml");
			return null;
		}
	}

	/** The fraction each runtime sets, for the models that leave it to their runtime */
	private Map<String, Double> runtimeFractions(List<JsonObject> services, List<String> warnings) {
		Map<String, Double> fractions = new HashMap<>();
		for (JsonObject service : services) {
			String runtime = ServingCapacity.runtimeOf(service);
			if (runtime == null || fractions.containsKey(runtime) || ServingCapacity.ownFraction(service) != null) {
				continue;
			}
			try {
				fractions.put(runtime, ServingCapacity.runtimeFraction(this.client
						.get(KubeClient.path("apis", "serving.kserve.io", "v1alpha1", "clusterservingruntimes", runtime))));
			} catch (KubeApiException e) {
				if (!e.isNotFound() && !e.isUnauthorized()) {
					throw e;
				}
				fractions.put(runtime, null);
				warnings.add("SEMOSS cannot read the ClusterServingRuntime " + runtime + " (HTTP " + e.getCode()
						+ "), so models that leave --gpu-memory-utilization to it count as vLLM's default "
						+ ServingCapacity.VLLM_DEFAULT_FRACTION);
			}
		}
		return fractions;
	}

	/**
	 * A model is READY once this SEMOSS can call it at its endpoint. Within
	 * {@value #ROUTE_GRACE_SECONDS} s of KServe marking it Ready, an endpoint that
	 * refuses is still being routed, and the start shows as finishing. Later, the
	 * model stays READY with a warning, so a network problem shows instead of
	 * hiding the model.
	 *
	 * @param always probe even a model ready for a while, to warn when it is unreachable
	 */
	private void confirmReachable(ServedModelStatus status, boolean always) {
		if (status.getState() != ServedModelState.READY || status.getEndpoint() == null) {
			return;
		}
		boolean justReady = status.phaseSince == null
				|| status.phaseSince.plusSeconds(ROUTE_GRACE_SECONDS).isAfter(this.clock.instant());
		if (!justReady && !always) {
			return;
		}
		String problem = this.probe.problem(status.getEndpoint());
		if (problem == null) {
			return;
		}
		if (justReady) {
			ServedModelStateResolver.awaitRoute(status);
		} else {
			status.warnings.add("SEMOSS cannot reach the model at " + status.getEndpoint() + ": " + problem);
		}
	}

	/** Wait, up to {@value #ADDRESS_POLLS} s, until ServiceLB has bound the model's published port. */
	private boolean waitForAddress(String name) {
		KubeClient.Request request = KubeClient.path("api", "v1", "namespaces", this.namespace, "services")
				.query("fieldSelector", "metadata.name=" + name + ServedModelTemplate.ENDPOINT_SUFFIX);
		for (int poll = 0; poll < ADDRESS_POLLS; poll++) {
			for (JsonObject service : KubeJson.objects(this.client.get(request), "items")) {
				if (!KubeJson.objects(service, "status", "loadBalancer", "ingress").isEmpty()) {
					return true;
				}
			}
			try {
				this.sleeper.sleep(GONE_POLL_MILLIS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return false;
	}

	/** Stop these models to make room; returns those that were running */
	private List<String> stopAll(List<String> models, User user) {
		List<String> stopped = new ArrayList<>();
		for (String model : models) {
			if (setStopped(model, true, user).changed()) {
				stopped.add(model);
			}
		}
		return stopped;
	}

	/**
	 * Wait, up to {@value #GONE_POLLS} polls a second apart, until these models
	 * have no pods left, which frees their GPUs and memory.
	 */
	private boolean waitForPodsGone(List<String> models) {
		if (models.isEmpty()) {
			return true;
		}
		String selector = ISVC_LABEL + " in (" + String.join(",", models) + ")";
		for (int poll = 0; poll < GONE_POLLS; poll++) {
			boolean left = KubeJson.objects(this.client.get(pods().query("labelSelector", selector)), "items").stream()
					.anyMatch(pod -> !"Succeeded".equals(KubeJson.str(pod, "status", "phase"))
							&& !"Failed".equals(KubeJson.str(pod, "status", "phase")));
			if (!left) {
				return true;
			}
			try {
				this.sleeper.sleep(GONE_POLL_MILLIS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		classLogger.warn("The pods of {} in {} are still there after {} s", models, this.namespace, GONE_POLLS);
		return false;
	}

	private static boolean isStopped(JsonObject service) {
		return "true".equalsIgnoreCase(KubeJson.str(service, "metadata", "annotations", STOP_ANNOTATION));
	}

	private ServedModelChange setStopped(String name, boolean stop, User user) {
		JsonObject service = getInferenceService(name);
		if (KubeJson.str(service, "metadata", "deletionTimestamp") != null) {
			throw new IllegalArgumentException("Served model " + name + " is being removed");
		}
		if (isStopped(service) == stop) {
			return new ServedModelChange(observe(service), false);
		}
		JsonObject annotations = new JsonObject();
		annotations.addProperty(STOP_ANNOTATION, Boolean.toString(stop));
		annotations.addProperty(LAST_ACTION_ANNOTATION,
				lastAction(stop ? "stop" : "start", user, this.clock.instant()).toString());
		if (stop) {
			String stats = startStats(service);
			if (stats != null) {
				annotations.addProperty(START_STATS_ANNOTATION, stats);
			}
		}
		JsonObject metadata = new JsonObject();
		metadata.add("annotations", annotations);
		JsonObject patch = new JsonObject();
		patch.add("metadata", metadata);
		JsonObject updated = this.client.mergePatch(inferenceService(name), patch);
		classLogger.info("{} served model {}/{}", stop ? "Stopping" : "Starting", this.namespace, name);
		return new ServedModelChange(observe(updated), true);
	}

	/** The models admins approved for serving; see {@link ServingCatalog}. */
	public ServingCatalog catalog() {
		if (this.catalogLocation.startsWith("file:")) {
			Path file = Path.of(this.catalogLocation.substring("file:".length()));
			try {
				return ServingCatalog.parse(Files.readString(file, StandardCharsets.UTF_8), file.toString(),
						this.runtimes, this.weightVolumes);
			} catch (IOException e) {
				return ServingCatalog.empty(file.toString(), "Cannot read the serving catalog " + file + ": " + e.getMessage());
			}
		}
		String name = this.catalogLocation.startsWith("configmap:")
				? this.catalogLocation.substring("configmap:".length())
				: this.catalogLocation;
		String source = "ConfigMap " + this.namespace + "/" + name;
		try {
			JsonObject configMap = this.client
					.get(KubeClient.path("api", "v1", "namespaces", this.namespace, "configmaps", name));
			return ServingCatalog.parse(KubeJson.str(configMap, "data", ServingCatalog.CATALOG_KEY),
					source + " (key " + ServingCatalog.CATALOG_KEY + ")", this.runtimes, this.weightVolumes);
		} catch (KubeApiException e) {
			if (e.isNotFound()) {
				return ServingCatalog.empty(source, "There is no serving catalog: " + source + " does not exist");
			}
			throw e;
		}
	}

	public ServedModelStatus define(String name, String catalogEntry, Map<String, Object> overrides, boolean start,
			User user) {
		return define(name, catalogEntry, overrides, start, false, null, user);
	}

	/**
	 * Define a model from a catalog entry: its InferenceService, created stopped,
	 * and its own Service for a stable endpoint. With {@code start} it is then
	 * started like any other model; when the GPUs or the memory budget do not allow
	 * that, it stays defined and stopped, and its status says why.
	 *
	 * Published, its Service is a LoadBalancer on a node port, which makes the
	 * model's OpenAI API reachable from the LAN, without authentication until vLLM
	 * gets API keys. Publishing is decided here, once: SEMOSS may create Services
	 * but not change or delete them, so a model is published again by removing and
	 * defining it.
	 *
	 * @param overrides   gpuMemoryUtilization, maxModelLen, args, cpu, memory
	 * @param publish     give the model a node port
	 * @param publishPort that node port; null for the lowest free one of
	 *                    {@value ServingSettings#KSERVE_HOST_PORT_RANGE}
	 */
	public ServedModelStatus define(String name, String catalogEntry, Map<String, Object> overrides, boolean start,
			boolean publish, Integer publishPort, User user) {
		ServedModelTemplate.validateName(name);
		ServingCatalog.Entry entry = catalog().entry(catalogEntry);
		ServedModelTemplate.Overrides parsed = ServedModelTemplate.Overrides.from(overrides);
		Integer hostPort = null;
		if (publish || publishPort != null) {
			// chosen before anything is created, so a port that cannot be had leaves nothing behind
			Services services = services();
			if (services.error() != null) {
				throw new IllegalArgumentException("SEMOSS cannot list the Services of " + this.namespace
						+ ", so it cannot pick a free port to publish " + name + " on: " + services.error());
			}
			hostPort = ServedModelTemplate.hostPort(services.items(), this.hostPorts, publishPort);
		}
		JsonObject isvc = ServedModelTemplate.inferenceService(name, this.namespace, entry, parsed, false,
				lastAction("define", user, this.clock.instant()));
		JsonObject created;
		try {
			created = this.client.create(inferenceServices(), isvc);
		} catch (KubeApiException e) {
			if ("AlreadyExists".equals(e.getReason()) || e.getCode() == 409) {
				throw new IllegalArgumentException(
						"A served model named " + name + " already exists in namespace " + this.namespace);
			}
			throw e;
		}
		try {
			this.client.create(KubeClient.path("api", "v1", "namespaces", this.namespace, "services"), ServedModelTemplate
					.endpointService(name, this.namespace, KubeJson.str(created, "metadata", "uid"), hostPort));
		} catch (KubeApiException e) {
			// without its own Service the model has no endpoint that survives a restart: undo the definition
			try {
				this.client.delete(inferenceService(name));
			} catch (KubeApiException undo) {
				classLogger.error("Could not remove {}/{} again after its Service failed: {}", this.namespace, name,
						undo.getMessage());
			}
			throw new KubeApiException(e.getCode(), e.getReason(), "Creating the Service " + name
					+ ServedModelTemplate.ENDPOINT_SUFFIX + " failed, so " + name + " was not defined: " + e.getMessage());
		}
		classLogger.info("Defined served model {}/{} from catalog entry {}{}", this.namespace, name, catalogEntry,
				hostPort == null ? "" : ", published on port " + hostPort);
		boolean bound = hostPort == null || waitForAddress(name);
		ServedModelStatus status = observe(created);
		if (!bound) {
			status.warnings.add("Port " + hostPort + " got no address from the node's load balancer within "
					+ ADDRESS_POLLS + " s; something on the node may already use it. Remove " + name
					+ " and define it again with another publishPort.");
		}
		if (start) {
			try {
				ServedModelStatus started = start(name, user, false).status();
				started.warnings.addAll(status.warnings);
				return started;
			} catch (ServingCapacityException e) {
				status.warnings.add(name + " is defined but was not started: " + e.getMessage());
			}
		}
		return status;
	}

	/**
	 * Remove a model SEMOSS defined. Its Service goes with it (owner reference);
	 * models applied by other means, such as the sample, are refused.
	 */
	public ServedModelRemoval remove(String name, User user) {
		JsonObject service = getInferenceService(name);
		if (!MANAGED_BY_SEMOSS.equals(KubeJson.str(service, "metadata", "labels", MANAGED_BY_LABEL))) {
			throw new IllegalArgumentException(name + " was not defined by SEMOSS (it has no "
					+ MANAGED_BY_LABEL + "=" + MANAGED_BY_SEMOSS + " label), so SEMOSS leaves it alone; use kubectl");
		}
		try {
			this.client.delete(inferenceService(name).query("propagationPolicy", "Background"));
		} catch (KubeApiException e) {
			if (!e.isNotFound()) {
				throw e;
			}
		}
		classLogger.info("Removing served model {}/{}", this.namespace, name);
		try {
			return new ServedModelRemoval(name, false, observe(this.client.get(inferenceService(name))));
		} catch (KubeApiException e) {
			if (e.isNotFound()) {
				return new ServedModelRemoval(name, true, null);
			}
			throw e;
		}
	}

	/**
	 * @param container         {@value #MAIN_CONTAINER} (vLLM) or
	 *                          {@value #INIT_CONTAINER} (the weight download)
	 * @param tailLines         lines to return, 1 to {@value #MAX_LOG_LINES};
	 *                          null for {@value #DEFAULT_LOG_LINES}
	 * @param previous          read the container's previous instance (after a
	 *                          crash)
	 * @param includeAccessLogs keep the /health and /metrics access lines
	 */
	public ServedModelLogs logs(String name, String container, Integer tailLines, boolean previous,
			boolean includeAccessLogs) {
		if (!MAIN_CONTAINER.equals(container) && !INIT_CONTAINER.equals(container)) {
			throw new IllegalArgumentException("container must be " + MAIN_CONTAINER + " or " + INIT_CONTAINER);
		}
		int tail = tailLines == null ? DEFAULT_LOG_LINES : tailLines;
		if (tail < 1 || tail > MAX_LOG_LINES) {
			throw new IllegalArgumentException("tailLines must be between 1 and " + MAX_LOG_LINES);
		}
		getInferenceService(name);
		JsonObject pod = ServedModelStateResolver.newestLivePod(podsOf(name));
		if (pod == null) {
			throw new IllegalArgumentException(
					"Served model " + name + " has no pod: it is stopped, or its pod is not created yet");
		}
		String podName = KubeJson.str(pod, "metadata", "name");
		int read = includeAccessLogs ? tail : tail * ACCESS_LOG_OVERREAD;
		String body = this.client.getText(log(podName).query("container", container).query("tailLines", read)
				.query("timestamps", "true").query("previous", previous ? "true" : null)
				.query("limitBytes", LOG_BYTES));
		List<String> lines = VllmStartupLog.lines(body, includeAccessLogs);
		if (lines.size() > tail) {
			lines = new ArrayList<>(lines.subList(lines.size() - tail, lines.size()));
		}
		return new ServedModelLogs(name, podName, container, previous, lines);
	}

	private ServedModelStatus observe(JsonObject service) {
		String name = KubeJson.str(service, "metadata", "name");
		List<JsonObject> pods = podsOf(name);
		JsonObject pod = ServedModelStateResolver.newestLivePod(pods);
		String podName = pod == null ? null : KubeJson.str(pod, "metadata", "name");
		// by uid as well: a model removed and defined again under the same name keeps its events for an hour
		List<JsonObject> events = new ArrayList<>(eventsOf(name, KubeJson.str(service, "metadata", "uid")));
		VllmStartupLog startupLog = null;
		if (podName != null) {
			events.addAll(eventsOf(podName, null));
			JsonObject main = ServedModelStateResolver.containerStatus(pod, "containerStatuses", MAIN_CONTAINER);
			String startedAt = KubeJson.str(main, "state", "running", "startedAt");
			if (startedAt != null) {
				// the first bytes after the container started: the startup, not the request log that follows
				startupLog = VllmStartupLog.parse(readLines(log(podName).query("container", MAIN_CONTAINER)
						.query("sinceTime", startedAt).query("timestamps", "true").query("limitBytes", STARTUP_LOG_BYTES)));
			}
		}
		ServedModelStatus status = ServedModelStateResolver.resolve(service, pods, events, startupLog,
				this.clock.instant());
		Services kubeServices = services();
		this.endpoints.apply(status, service, kubeServices.items(), kubeServices.error());
		confirmReachable(status, true);
		ServedModelFailure failure = status.getFailure();
		if (failure != null && failure.getContainer() != null && podName != null) {
			ServedModelStateResolver.explainFailure(status,
					readLines(log(podName).query("container", failure.getContainer())
							.query("tailLines", FAILURE_LOG_LINES).query("timestamps", "true")
							.query("previous", failure.isPreviousInstance() ? "true" : null)
							.query("limitBytes", LOG_BYTES)));
		}
		if (status.getState() == ServedModelState.WAITING_FOR_GPU) {
			ServedModelStateResolver.explainGpuWait(status, gpuHolders(name));
		}
		return status;
	}

	/**
	 * How long each phase of the running pod's start took, measured before the
	 * stop deletes the pod, for the next start's ETA. Failing to measure only costs
	 * that better ETA.
	 */
	private String startStats(JsonObject service) {
		try {
			return ServedModelStateResolver.startStats(observe(service),
					KubeJson.str(service, "metadata", "annotations", START_STATS_ANNOTATION), this.clock.instant());
		} catch (KubeApiException e) {
			classLogger.warn("Cannot measure the last start of {}/{}: {}", this.namespace,
					KubeJson.str(service, "metadata", "name"), e.getMessage());
			return null;
		}
	}

	private record Services(List<JsonObject> items, String error) {
	}

	/** The namespace's Services, for the endpoints; without them a status only lacks its endpoint. */
	private Services services() {
		try {
			return new Services(KubeJson.objects(
					this.client.get(KubeClient.path("api", "v1", "namespaces", this.namespace, "services")), "items"),
					null);
		} catch (KubeApiException e) {
			classLogger.warn("Cannot list the Services of {}: {}", this.namespace, e.getMessage());
			return new Services(null, e.getMessage());
		}
	}

	/** A log that cannot be read (a container that never started) only means fewer details. */
	private List<String> readLines(KubeClient.Request request) {
		try {
			return VllmStartupLog.lines(this.client.getText(request), false);
		} catch (KubeApiException e) {
			classLogger.debug("Cannot read {}: {}", request, e.getMessage());
			return List.of();
		}
	}

	private List<JsonObject> eventsOf(String objectName, String uid) {
		String selector = "involvedObject.name=" + objectName + (uid == null ? "" : ",involvedObject.uid=" + uid);
		try {
			return KubeJson.objects(this.client.get(KubeClient.path("api", "v1", "namespaces", this.namespace, "events")
					.query("fieldSelector", selector)), "items");
		} catch (KubeApiException e) {
			classLogger.warn("Cannot read the events of {}/{}: {}", this.namespace, objectName, e.getMessage());
			return List.of();
		}
	}

	/** Who holds the GPU: the scheduled pods of the namespace that request one. */
	private List<String> gpuHolders(String waiting) {
		List<String> holders = new ArrayList<>();
		try {
			for (JsonObject pod : KubeJson.objects(this.client.get(pods()), "items")) {
				String phase = KubeJson.str(pod, "status", "phase");
				if (KubeJson.str(pod, "spec", "nodeName") == null || "Succeeded".equals(phase) || "Failed".equals(phase)
						|| !requestsGpu(pod)) {
					continue;
				}
				String owner = KubeJson.str(pod, "metadata", "labels", ISVC_LABEL);
				if (waiting.equals(owner)) {
					continue;
				}
				String holder = owner != null ? owner : "pod " + KubeJson.str(pod, "metadata", "name");
				if (!holders.contains(holder)) {
					holders.add(holder);
				}
			}
		} catch (KubeApiException e) {
			classLogger.warn("Cannot list the pods holding the GPU in {}: {}", this.namespace, e.getMessage());
		}
		return holders;
	}

	private static boolean requestsGpu(JsonObject pod) {
		for (JsonObject container : KubeJson.objects(pod, "spec", "containers")) {
			String gpus = KubeJson.str(container, "resources", "limits", "nvidia.com/gpu");
			if (gpus != null && !"0".equals(gpus)) {
				return true;
			}
		}
		return false;
	}

	private JsonObject getInferenceService(String name) {
		validateName(name);
		try {
			return this.client.get(inferenceService(name));
		} catch (KubeApiException e) {
			if (e.isNotFound()) {
				throw new IllegalArgumentException(
						"No served model named " + name + " in namespace " + this.namespace);
			}
			throw e;
		}
	}

	static void validateName(String name) {
		if (name == null || !NAME.matcher(name).matches()) {
			throw new IllegalArgumentException("Invalid served model name " + name
					+ ": InferenceService names are lowercase letters, digits and '-', starting with a letter");
		}
	}

	private static JsonObject lastAction(String action, User user, Instant at) {
		JsonObject json = new JsonObject();
		json.addProperty("action", action);
		AccessToken token = user == null ? null : user.getPrimaryLoginToken();
		if (token != null) {
			json.addProperty("user", token.getId());
			json.addProperty("userName", token.getName());
		}
		json.addProperty("at", at.toString());
		json.addProperty("instance", System.getenv("KUBERNETES_SERVICE_HOST") == null ? "host" : "cluster");
		return json;
	}

	private KubeClient.Request inferenceServices() {
		return KubeClient.path("apis", "serving.kserve.io", "v1beta1", "namespaces", this.namespace,
				"inferenceservices");
	}

	private KubeClient.Request inferenceService(String name) {
		return KubeClient.path("apis", "serving.kserve.io", "v1beta1", "namespaces", this.namespace,
				"inferenceservices", name);
	}

	private KubeClient.Request pods() {
		return KubeClient.path("api", "v1", "namespaces", this.namespace, "pods");
	}

	private List<JsonObject> podsOf(String name) {
		return KubeJson.objects(this.client.get(pods().query("labelSelector", ISVC_LABEL + "=" + name)), "items");
	}

	private KubeClient.Request log(String pod) {
		return KubeClient.path("api", "v1", "namespaces", this.namespace, "pods", pod, "log");
	}
}
