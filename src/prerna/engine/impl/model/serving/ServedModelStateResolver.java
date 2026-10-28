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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.engine.impl.model.serving.ServedModelFailure.Cause;
import prerna.engine.impl.model.serving.ServingCatalog.VolumeUri;
import prerna.engine.impl.model.serving.VllmStartupLog.Milestone;
import prerna.util.k8s.KubeJson;

/**
 * Derives a served model's state from what the cluster shows: the
 * InferenceService, its predictor pods, their events and vLLM's startup log. A
 * pure function of its inputs (time included), so every state is tested against
 * objects recorded from the cluster.
 *
 * KServe's own {@code modelStatus.states} is reported but never drives a phase:
 * on this install it goes Pending, Loading (while downloading), Pending again
 * (while vLLM loads), then Loaded.
 */
public final class ServedModelStateResolver {

	static final String MAIN_CONTAINER = "kserve-container";
	static final String INIT_CONTAINER = "storage-initializer";
	static final String ISVC_LABEL = "serving.kserve.io/inferenceservice";
	static final String STOP_ANNOTATION = "serving.kserve.io/stop";
	static final String LAST_ACTION_ANNOTATION = "semoss.org/last-action";
	static final String MANAGED_BY_LABEL = "app.kubernetes.io/managed-by";
	static final String MANAGED_BY_SEMOSS = "semoss";
	/** How long each phase took the last time this model started; SEMOSS writes it when it stops the model */
	static final String START_STATS_ANNOTATION = "semoss.org/start-stats";

	private static final Set<String> IMAGE_PULL_REASONS = Set.of("ErrImagePull", "ImagePullBackOff",
			"InvalidImageName", "ErrImageNeverPull");
	private static final Set<String> CONFIG_REASONS = Set.of("CreateContainerConfigError", "CreateContainerError",
			"RunContainerError");
	private static final Set<String> PULL_EVENT_REASONS = Set.of("Pulling", "Pulled", "Failed", "BackOff");
	/** Istio's absence (KServe warns on every reconcile) and optimistic-concurrency retries */
	private static final Set<String> NOISE_EVENT_REASONS = Set.of("VirtualServiceCRDNotFound");
	private static final String CONFLICT_MESSAGE = "the object has been modified";
	private static final int MAX_EVENTS = 10;
	private static final double MAX_PHASE_FRACTION = 0.95;
	/** 100% is READY's; a start whose last budget has run out still reads 99% */
	private static final double MAX_STARTING_PROGRESS = 0.99;
	/** a phase that takes this many times its budget may be stuck */
	private static final int STALL_FACTOR = 3;
	/** but no phase counts as stuck sooner than this */
	private static final long STALL_MIN_SECONDS = 600;
	/** KServe creates the model server pod within seconds of a start */
	private static final long CONTROLLER_STALL_SECONDS = 120;
	/**
	 * KServe v0.21 mounts a pvc:// volume into the pod and runs no storage
	 * initializer; with direct mounting turned off, the initializer would link the
	 * files in a second or two instead of downloading them
	 */
	private static final int LINK_SECONDS = 5;
	private static final Pattern SCHEDULER_SUMMARY = Pattern
			.compile("^(.*?\\.)\\s+(?:no new claims|preemption:)", Pattern.DOTALL);
	private static final Gson GSON = new Gson();

	private ServedModelStateResolver() {
	}

	/**
	 * @param isvc       the InferenceService
	 * @param pods       its predictor pods, in any order, terminating ones included
	 * @param events     events of the namespace; only those of the
	 *                   InferenceService and its newest pod are used
	 * @param startupLog vLLM's log since the model container started, or null
	 * @param now        the time phases are measured against
	 */
	public static ServedModelStatus resolve(JsonObject isvc, List<JsonObject> pods, List<JsonObject> events,
			VllmStartupLog startupLog, Instant now) {
		ServedModelStatus status = new ServedModelStatus();
		status.observedAt = now;
		status.servedModel = KubeJson.str(isvc, "metadata", "name");
		status.namespace = KubeJson.str(isvc, "metadata", "namespace");
		status.managed = MANAGED_BY_SEMOSS.equals(KubeJson.str(isvc, "metadata", "labels", MANAGED_BY_LABEL));
		status.displayName = KubeJson.str(isvc, "metadata", "annotations", ServedModelTemplate.DISPLAY_NAME_ANNOTATION);
		status.catalogEntry = KubeJson.str(isvc, "metadata", "annotations",
				ServedModelTemplate.CATALOG_ENTRY_ANNOTATION);
		JsonObject model = KubeJson.obj(isvc, "spec", "predictor", "model");
		status.storageUri = KubeJson.str(model, "storageUri");
		status.runtime = KubeJson.str(model, "runtime");
		status.gpuMemoryUtilization = lastFlagValue(KubeJson.strings(model, "args"), "--gpu-memory-utilization");
		status.inClusterUrl = KubeJson.str(isvc, "status", "address", "url");
		status.lastAction = parseLastAction(KubeJson.str(isvc, "metadata", "annotations", LAST_ACTION_ANNOTATION));
		status.conditions = toMaps(KubeJson.objects(isvc, "status", "conditions"));
		JsonObject modelStatus = KubeJson.obj(isvc, "status", "modelStatus");
		status.modelStatus = modelStatus == null ? null : toMap(modelStatus);
		boolean stopRequested = "true"
				.equalsIgnoreCase(KubeJson.str(isvc, "metadata", "annotations", STOP_ANNOTATION));
		// a stopped model has no start in progress, even one it was defined or created with
		status.startRequestedAt = stopRequested ? null : startRequestedAt(isvc, status.lastAction);

		List<JsonObject> allPods = pods == null ? List.of() : pods;
		JsonObject pod = newestLivePod(allPods);
		status.pod = pod == null ? null : KubeJson.str(pod, "metadata", "name");
		List<JsonObject> podEvents = eventsOf(events, "Pod", status.pod);
		status.events = displayEvents(events, status.servedModel, status.pod);
		if (startupLog != null) {
			status.architecture = startupLog.getArchitecture();
			status.maxModelLen = startupLog.getMaxModelLen();
			status.weightsGiB = startupLog.getWeightsGiB();
			status.kvCacheGiB = startupLog.getKvCacheGiB();
			status.compileCache = startupLog.getCompileCache();
		}

		if (KubeJson.str(isvc, "metadata", "deletionTimestamp") != null) {
			set(status, ServedModelState.REMOVING, null, "Being removed", null);
		} else if (stopRequested) {
			if (allPods.isEmpty()) {
				set(status, ServedModelState.STOPPED, null, "Stopped", null);
			} else {
				set(status, ServedModelState.STOPPING, null, "Stopping: the model server is shutting down", null);
			}
		} else if (pod == null) {
			set(status, ServedModelState.STARTING, ServedModelPhase.CREATING, ServedModelPhase.CREATING.getMessage(),
					status.startRequestedAt);
		} else {
			resolvePod(status, isvc, pod, podEvents, startupLog);
		}
		status.phases = pod == null ? List.of() : timeline(isvc, pod, podEvents, startupLog);
		Map<ServedModelPhase, Integer> lastStart = parseStartStats(
				KubeJson.str(isvc, "metadata", "annotations", START_STATS_ANNOTATION));
		progress(status, pullSeen(podEvents, MAIN_CONTAINER) != null, now, lastStart);
		stall(status, now, lastStart);
		return status;
	}

	/**
	 * The scheduler's reason without its preemption report: "0/1 nodes are
	 * available: 1 Insufficient cpu. no new claims to deallocate, preemption: ..."
	 * becomes "0/1 nodes are available: 1 Insufficient cpu."
	 */
	static String schedulerReason(String detail) {
		if (detail == null) {
			return null;
		}
		Matcher summary = SCHEDULER_SUMMARY.matcher(detail);
		return summary.find() ? summary.group(1) : detail;
	}

	private static String schedulingHint(String reason) {
		if (reason == null) {
			return null;
		}
		if (reason.contains("Insufficient cpu") || reason.contains("Insufficient memory")) {
			return "The model asks for more CPU or memory than the node has free; lower its cpu or memory, or stop "
					+ "what else uses them";
		}
		if (reason.contains("node affinity") || reason.contains("node selector")) {
			return "No node matches the model's node selector or affinity";
		}
		if (reason.contains("taint")) {
			return "The node has a taint the model does not tolerate";
		}
		if (reason.contains("persistentvolumeclaim") && reason.contains("not found")) {
			return "Create the PersistentVolumeClaim it names; the model caches are in models/model-cache.yml "
					+ "of the dgx repo";
		}
		if (reason.contains("unbound") && reason.contains("PersistentVolumeClaims")) {
			return "A PersistentVolumeClaim of the model is not bound to a volume; kubectl describe pvc shows why";
		}
		return null;
	}

	private static String pullHint(String message) {
		if (message == null) {
			return null;
		}
		String lower = message.toLowerCase(Locale.ROOT);
		if (lower.contains("not found") || lower.contains("manifest unknown")) {
			return "The image or its tag does not exist; check the image name and tag";
		}
		if (lower.contains("unauthorized") || lower.contains("denied") || lower.contains("authentication required")) {
			return "The registry refused the pull; check the image pull secret (for nvcr.io, the NGC API key)";
		}
		if (lower.contains("timeout") || lower.contains("no such host") || lower.contains("connection refused")) {
			return "The node cannot reach the registry; check its network and proxy";
		}
		return null;
	}

	private static String configHint(String message) {
		if (message == null) {
			return null;
		}
		if (message.startsWith("secret ") && message.contains("not found")) {
			return "Create the Secret it names, or remove the reference from the model";
		}
		if (message.startsWith("configmap ") && message.contains("not found")) {
			return "Create the ConfigMap it names, or remove the reference from the model";
		}
		return null;
	}

	/**
	 * Warn about a start that seems stuck; the state stays STARTING, since a slow
	 * start and a stuck one look the same from here. KServe creates the model
	 * server pod within seconds, so no pod after {@value #CONTROLLER_STALL_SECONDS}
	 * s means it has not acted. A phase is suspect after {@value #STALL_FACTOR}
	 * times its budget (the last start's length, else the default), and never
	 * before {@value #STALL_MIN_SECONDS} s. The ETA is then unknown.
	 */
	private static void stall(ServedModelStatus status, Instant now, Map<ServedModelPhase, Integer> lastStart) {
		if (status.state != ServedModelState.STARTING || status.phase == null) {
			return;
		}
		if (status.phase == ServedModelPhase.CREATING && status.pod == null) {
			long waited = secondsSince(status.startRequestedAt, now);
			if (waited >= CONTROLLER_STALL_SECONDS) {
				status.warnings.add("KServe has not created the model server pod " + duration(waited)
						+ " after the start; check that kserve-controller-manager runs (kubectl -n kserve get pods)");
				status.etaSeconds = null;
			}
			return;
		}
		long elapsed = secondsSince(status.phaseSince, now);
		long expected = budget(status.phase, lastStart, status.storageUri);
		if (elapsed >= Math.max(STALL_MIN_SECONDS, STALL_FACTOR * expected)) {
			status.warnings.add(activity(status.phase, status.storageUri) + " has taken " + duration(elapsed)
					+ ", against about " + duration(expected) + " expected, so it may be stuck. "
					+ whereToLook(status.phase, status.storageUri));
			status.etaSeconds = null;
		}
	}

	private static String activity(ServedModelPhase phase, String storageUri) {
		return switch (phase) {
		case CREATING -> "Starting the model server pod's containers";
		case PULLING_INIT_IMAGE -> "Pulling the storage-initializer image";
		case DOWNLOADING_WEIGHTS -> VolumeUri.of(storageUri) == null ? "The weight download" : "Linking the weights";
		case PULLING_IMAGE -> "Pulling the model server image";
		case LOADING_WEIGHTS -> "Loading the weights";
		case COMPILING -> "Compiling the model";
		case WARMING -> "Capturing CUDA graphs";
		case FINISHING -> "Starting the API server";
		};
	}

	private static String whereToLook(ServedModelPhase phase, String storageUri) {
		return switch (phase) {
		case DOWNLOADING_WEIGHTS -> "GetServedModelLogs with container storage-initializer shows the "
				+ (VolumeUri.of(storageUri) == null ? "download." : "files it links.");
		case PULLING_INIT_IMAGE, PULLING_IMAGE -> "The events in GetServedModelStatus show the pull.";
		case CREATING -> "The events in GetServedModelStatus show what holds the pod.";
		default -> "GetServedModelLogs shows what vLLM is doing.";
		};
	}

	private static long secondsSince(Instant since, Instant now) {
		return since == null ? 0 : Math.max(0, Duration.between(since, now).toSeconds());
	}

	/** 45 s, 12 min, 1 h 5 min */
	static String duration(long seconds) {
		if (seconds < 60) {
			return seconds + " s";
		}
		long minutes = Math.round(seconds / 60.0);
		return minutes < 60 ? minutes + " min" : minutes / 60 + " h " + minutes % 60 + " min";
	}

	private static void resolvePod(ServedModelStatus status, JsonObject isvc, JsonObject pod,
			List<JsonObject> podEvents, VllmStartupLog startupLog) {
		JsonObject init = containerStatus(pod, "initContainerStatuses", INIT_CONTAINER);
		JsonObject main = containerStatus(pod, "containerStatuses", MAIN_CONTAINER);
		status.restarts = restartCount(init) + restartCount(main);
		boolean mainReady = main != null && KubeJson.isTrue(main, "ready");

		if (mainReady && "True".equals(conditionStatus(KubeJson.objects(isvc, "status", "conditions"), "Ready"))) {
			set(status, ServedModelState.READY, null, "Ready", conditionTime(isvc, "Ready"));
			if (status.restarts > 0) {
				status.warnings.add("The model server restarted " + status.restarts + " time(s)");
			}
			return;
		}

		ServedModelFailure failure = failure(isvc, pod, init, main, podEvents, status.storageUri);
		if (failure != null) {
			status.failure = failure;
			set(status, ServedModelState.FAILED, null, failure.getMessage(), null);
			return;
		}

		JsonObject scheduled = condition(KubeJson.objects(pod, "status", "conditions"), "PodScheduled");
		if (scheduled != null && "False".equals(KubeJson.str(scheduled, "status"))) {
			String detail = KubeJson.str(scheduled, "message");
			Instant since = KubeJson.time(scheduled, "lastTransitionTime");
			if (detail != null && detail.contains("nvidia.com/gpu")) {
				set(status, ServedModelState.WAITING_FOR_GPU, null, "Waiting for the GPU: " + detail, since);
			} else if ("Unschedulable".equals(KubeJson.str(scheduled, "reason"))) {
				String reason = schedulerReason(detail);
				status.failure = new ServedModelFailure(Cause.UNSCHEDULABLE, null, "Unschedulable", null, 0, false,
						"Kubernetes cannot schedule the model server" + (reason == null ? "" : ": " + reason));
				status.failure.setHint(schedulingHint(reason));
				set(status, ServedModelState.FAILED, null, status.failure.getMessage(), since);
			} else {
				set(status, ServedModelState.STARTING, ServedModelPhase.CREATING,
						"Waiting to be scheduled" + (detail == null ? "" : ": " + detail), since);
			}
			return;
		}

		// a pod just created has no container statuses yet, but its spec already declares the download
		boolean downloads = init != null || declaresContainer(pod, "initContainers", INIT_CONTAINER);
		if (downloads && !(init != null && isTerminated(init)
				&& exitCode(KubeJson.obj(init, "state", "terminated")) == 0)) {
			Instant downloadStarted = KubeJson.time(init, "state", "running", "startedAt");
			if (downloadStarted != null) {
				set(status, ServedModelState.STARTING, ServedModelPhase.DOWNLOADING_WEIGHTS,
						fetching(status.storageUri, "the model weights"), downloadStarted);
				return;
			}
			Instant pullStarted = pullInProgress(podEvents, INIT_CONTAINER);
			if (pullStarted != null) {
				phase(status, ServedModelPhase.PULLING_INIT_IMAGE, pullStarted);
			} else {
				phase(status, ServedModelPhase.CREATING, KubeJson.time(pod, "metadata", "creationTimestamp"));
			}
			return;
		}

		Instant started = main == null ? null : KubeJson.time(main, "state", "running", "startedAt");
		if (started == null) {
			Instant pullStarted = pullInProgress(podEvents, MAIN_CONTAINER);
			if (pullStarted != null) {
				phase(status, ServedModelPhase.PULLING_IMAGE, pullStarted);
			} else {
				Instant initFinished = init == null ? null : KubeJson.time(init, "state", "terminated", "finishedAt");
				set(status, ServedModelState.STARTING, ServedModelPhase.CREATING, "Starting the model container",
						initFinished != null ? initFinished : KubeJson.time(pod, "metadata", "creationTimestamp"));
			}
			return;
		}

		if (mainReady) {
			set(status, ServedModelState.STARTING, ServedModelPhase.FINISHING,
					"The model server answers; waiting for KServe to report it Ready",
					firstNonNull(timeOf(startupLog, Milestone.STARTUP_COMPLETE), started));
		} else if (reached(startupLog, Milestone.SERVER_STARTING) || reached(startupLog, Milestone.STARTUP_COMPLETE)) {
			phase(status, ServedModelPhase.FINISHING, firstNonNull(timeOf(startupLog, Milestone.SERVER_STARTING),
					timeOf(startupLog, Milestone.STARTUP_COMPLETE), started));
		} else if (reached(startupLog, Milestone.COMPILED)) {
			phase(status, ServedModelPhase.WARMING, firstNonNull(timeOf(startupLog, Milestone.COMPILED), started));
		} else if (reached(startupLog, Milestone.WEIGHTS_LOADED)) {
			phase(status, ServedModelPhase.COMPILING,
					firstNonNull(timeOf(startupLog, Milestone.WEIGHTS_LOADED), started));
		} else {
			phase(status, ServedModelPhase.LOADING_WEIGHTS, started);
		}
	}

	/**
	 * A container that failed: an image that cannot be pulled, a container that
	 * cannot be created, or a terminated instance with a non-zero exit. A failure
	 * stays visible while Kubernetes retries (the previous instance's record
	 * persists), so the state does not flicker back to loading between attempts.
	 */
	private static ServedModelFailure failure(JsonObject isvc, JsonObject pod, JsonObject init, JsonObject main,
			List<JsonObject> podEvents, String storageUri) {
		for (JsonObject container : new JsonObject[] { init, main }) {
			if (container == null) {
				continue;
			}
			String name = KubeJson.str(container, "name");
			boolean isInit = INIT_CONTAINER.equals(name);
			int restarts = restartCount(container);
			String waiting = KubeJson.str(container, "state", "waiting", "reason");
			String waitingMessage = KubeJson.str(container, "state", "waiting", "message");
			if (waiting != null && IMAGE_PULL_REASONS.contains(waiting)) {
				ServedModelFailure pull = new ServedModelFailure(Cause.IMAGE_PULL, name, waiting, null, restarts, false,
						"Cannot pull " + KubeJson.str(container, "image") + ": "
								+ firstNonNull(waitingMessage, waiting));
				pull.setHint(pullHint(waitingMessage));
				return pull;
			}
			if (waiting != null && CONFIG_REASONS.contains(waiting)) {
				ServedModelFailure config = new ServedModelFailure(Cause.CONFIG, name, waiting, null, restarts, false,
						"Kubernetes cannot create the " + name + " container: "
								+ firstNonNull(waitingMessage, waiting));
				config.setHint(configHint(waitingMessage));
				return config;
			}
			JsonObject terminated = KubeJson.obj(container, "state", "terminated");
			JsonObject previous = KubeJson.obj(container, "lastState", "terminated");
			JsonObject record = null;
			boolean previousInstance = false;
			if (terminated != null && exitCode(terminated) != 0) {
				record = terminated;
			} else if (previous != null && exitCode(previous) != 0 && !KubeJson.isTrue(container, "ready")) {
				record = previous;
				previousInstance = true;
			}
			if (record == null) {
				continue;
			}
			String reason = KubeJson.str(record, "reason");
			Integer exitCode = KubeJson.integer(record, "exitCode");
			Cause cause;
			String message;
			if ("OOMKilled".equals(reason)) {
				cause = isInit ? Cause.DOWNLOAD_OOM : Cause.MODEL_OOM;
				message = isInit ? "The weight download ran out of memory (the storage initializer's memory limit)"
						: "The model server was killed for lack of memory (GPU and host share the unified memory)";
			} else if (!isInit && startupProbeKilled(podEvents)) {
				cause = Cause.STARTUP_TIMEOUT;
				message = "The model server did not become healthy before its startup probe gave up";
			} else if (isInit) {
				cause = Cause.DOWNLOAD_FAILED;
				message = fetching(storageUri, "the weights") + " failed (exit code " + exitCode + ")";
			} else {
				cause = Cause.MODEL_CRASH;
				message = "The model server exited with code " + exitCode;
			}
			if (restarts > 0) {
				message += "; Kubernetes is retrying (restart " + restarts + ")";
			}
			return new ServedModelFailure(cause, name, reason, exitCode, restarts, previousInstance, message);
		}
		JsonObject states = KubeJson.obj(isvc, "status", "modelStatus", "states");
		if ("FailedToLoad".equals(KubeJson.str(states, "targetModelState"))) {
			JsonObject info = KubeJson.obj(isvc, "status", "modelStatus", "lastFailureInfo");
			return new ServedModelFailure(Cause.MODEL_CRASH, MAIN_CONTAINER, KubeJson.str(info, "reason"),
					KubeJson.integer(info, "exitCode"), restartCount(main), restartCount(main) > 0,
					firstNonNull(KubeJson.str(info, "message"), "KServe reports that the model failed to load"));
		}
		return null;
	}

	/**
	 * Complete a failure with the end of the failing container's log: the most
	 * telling line joins the message and known errors get a hint.
	 */
	public static void explainFailure(ServedModelStatus status, List<String> logTail) {
		ServedModelFailure failure = status.failure;
		if (failure == null || logTail == null || logTail.isEmpty()) {
			return;
		}
		failure.setLogTail(logTail);
		String line = VllmStartupLog.mostTellingLine(logTail);
		if (line != null && (failure.getCause() == Cause.MODEL_CRASH || failure.getCause() == Cause.DOWNLOAD_FAILED
				|| failure.getCause() == Cause.STARTUP_TIMEOUT)) {
			failure.setMessage(failure.getMessage() + ": " + line);
			status.setMessage(failure.getMessage());
		}
		String all = String.join("\n", logTail);
		if (all.contains("less than desired GPU memory utilization")) {
			failure.setHint("Lower --gpu-memory-utilization for this model, or stop what else is using memory: "
					+ "the GPU and the host share the 128 GB");
		} else if (all.contains("CUDA out of memory") || all.contains("OutOfMemoryError")) {
			failure.setHint("Lower --gpu-memory-utilization or --max-model-len");
		} else if (all.contains("No space left on device")) {
			failure.setHint("The node's disk is full");
		} else if (all.contains("GatedRepoError") || all.contains("401 Client Error")
				|| all.contains("RepositoryNotFoundError")) {
			failure.setHint("The Hugging Face repository is gated, private or missing; check the storageUri");
		} else if (VolumeUri.of(status.storageUri) != null && missingCopy(failure.getCause(), all)) {
			failure.setHint(missingCopyHint(VolumeUri.of(status.storageUri)));
		}
	}

	/**
	 * The storage initializer's complaint about a path that is not on the volume,
	 * or vLLM's when the volume was mounted directly and the folder came up empty.
	 */
	private static boolean missingCopy(Cause cause, String log) {
		if (cause == Cause.DOWNLOAD_FAILED) {
			return log.contains("Cannot locate source uri") || log.contains("does not exist")
					|| log.contains("No model found");
		}
		return cause == Cause.MODEL_CRASH && log.contains("Invalid repository ID or local directory");
	}

	/**
	 * The intake fills the model cache at org/repo/commit, and its cache command
	 * copies a model taken in earlier back from MinIO.
	 */
	private static String missingCopyHint(VolumeUri volume) {
		String[] parts = volume.path().split("/");
		String refill = parts.length == 3
				? "bash models/intake/intake.sh cache " + parts[0] + "/" + parts[1] + " " + parts[2]
				: "models/intake/intake.sh";
		return "Volume " + volume.claim() + " has no copy of the weights at " + volume.path() + ". Copy them there with "
				+ refill + " (the dgx repo), or point the catalog entry at a copy that exists";
	}

	/** Name the models whose pods hold the GPU this one waits for. */
	/**
	 * READY by its pod and KServe, but the endpoint does not reach the model yet:
	 * right after the pod turns ready, a Service takes a moment (2 s on this box)
	 * before it routes to it, and until then connections are refused.
	 */
	static void awaitRoute(ServedModelStatus status) {
		status.state = ServedModelState.STARTING;
		status.phase = ServedModelPhase.FINISHING;
		status.message = "Waiting for " + status.endpoint + " to reach the model";
		status.progress = MAX_STARTING_PROGRESS;
		status.etaSeconds = 1L;
	}

	public static void explainGpuWait(ServedModelStatus status, List<String> holders) {
		if (status.state == ServedModelState.WAITING_FOR_GPU && holders != null && !holders.isEmpty()) {
			status.setMessage("Waiting for the GPU, which " + String.join(", ", holders)
					+ (holders.size() == 1 ? " is" : " are") + " using");
		}
	}

	/**
	 * Rough progress and ETA of a start: the phases' budgets are summed, finished
	 * ones count fully, and the current one counts by elapsed time, capped at 95%
	 * so a slow phase never shows done. A budget is how long the phase took the
	 * last time this model started, or the phase's default length the first time.
	 */
	private static void progress(ServedModelStatus status, boolean imagePullSeen, Instant now,
			Map<ServedModelPhase, Integer> lastStart) {
		if (status.state == ServedModelState.READY) {
			status.progress = 1.0;
			status.etaSeconds = 0L;
			return;
		}
		if (status.state != ServedModelState.STARTING || status.phase == null) {
			return;
		}
		List<ServedModelPhase> plan = new ArrayList<>(List.of(ServedModelPhase.CREATING,
				ServedModelPhase.DOWNLOADING_WEIGHTS, ServedModelPhase.LOADING_WEIGHTS, ServedModelPhase.COMPILING,
				ServedModelPhase.WARMING, ServedModelPhase.FINISHING));
		if (status.phase == ServedModelPhase.PULLING_INIT_IMAGE) {
			plan.add(plan.indexOf(ServedModelPhase.DOWNLOADING_WEIGHTS), ServedModelPhase.PULLING_INIT_IMAGE);
		}
		if (status.phase == ServedModelPhase.PULLING_IMAGE || imagePullSeen) {
			plan.add(plan.indexOf(ServedModelPhase.LOADING_WEIGHTS), ServedModelPhase.PULLING_IMAGE);
		}
		int current = plan.indexOf(status.phase);
		double total = 0;
		double done = 0;
		long after = 0;
		boolean learned = false;
		for (int index = 0; index < plan.size(); index++) {
			int seconds = budget(plan.get(index), lastStart, status.storageUri);
			learned |= lastStart.containsKey(plan.get(index));
			total += seconds;
			if (index < current) {
				done += seconds;
			} else if (index > current) {
				after += seconds;
			}
		}
		status.etaBasis = learned ? "last start" : "defaults";
		double budget = budget(status.phase, lastStart, status.storageUri);
		long elapsed = status.phaseSince == null ? 0
				: Math.max(0, Duration.between(status.phaseSince, now).toSeconds());
		double fraction = Math.min(MAX_PHASE_FRACTION, elapsed / budget);
		status.progress = Math.min(MAX_STARTING_PROGRESS, Math.round((done + fraction * budget) / total * 100) / 100.0);
		long remaining = Math.max(Math.round(budget - elapsed), Math.round(budget * (1 - MAX_PHASE_FRACTION)));
		status.etaSeconds = Math.max(1, remaining + after);
	}

	/**
	 * A phase that took no time still gets one second, so fractions stay defined.
	 * Without a last start, the weights of a pvc:// copy take
	 * {@value #LINK_SECONDS} s, since they are at most linked, never downloaded.
	 */
	private static int budget(ServedModelPhase phase, Map<ServedModelPhase, Integer> lastStart, String storageUri) {
		Integer seconds = lastStart.get(phase);
		if (seconds != null) {
			return Math.max(1, seconds);
		}
		return phase == ServedModelPhase.DOWNLOADING_WEIGHTS && VolumeUri.of(storageUri) != null ? LINK_SECONDS
				: phase.getDefaultSeconds();
	}

	/** @return the phase lengths of the last start, empty when unknown or unreadable */
	static Map<ServedModelPhase, Integer> parseStartStats(String json) {
		Map<ServedModelPhase, Integer> seconds = new EnumMap<>(ServedModelPhase.class);
		if (json == null) {
			return seconds;
		}
		try {
			JsonObject measured = KubeJson.obj(JsonParser.parseString(json).getAsJsonObject(), "seconds");
			if (measured != null) {
				for (ServedModelPhase phase : ServedModelPhase.values()) {
					Integer value = KubeJson.integer(measured, phase.name());
					if (value != null && value >= 0) {
						seconds.put(phase, value);
					}
				}
			}
		} catch (RuntimeException e) {
			// a hand-edited or foreign annotation only costs the better ETA
		}
		return seconds;
	}

	/**
	 * The start-stats annotation to record for a model about to be stopped: the
	 * phases its current pod completed, over the lengths recorded before.
	 *
	 * @return the annotation value, or null when this pod completed no phase
	 */
	static String startStats(ServedModelStatus current, String previousJson, Instant now) {
		Map<ServedModelPhase, Integer> seconds = parseStartStats(previousJson);
		boolean measured = false;
		for (Map<String, Object> span : current.phases) {
			if (span.get("seconds") instanceof Long length) {
				seconds.put(ServedModelPhase.valueOf((String) span.get("phase")), length.intValue());
				measured = true;
			}
		}
		if (!measured) {
			return null;
		}
		JsonObject lengths = new JsonObject();
		seconds.forEach((phase, length) -> lengths.addProperty(phase.name(), length));
		JsonObject stats = new JsonObject();
		stats.addProperty("at", now.toString());
		stats.addProperty("pod", current.pod);
		stats.add("seconds", lengths);
		return stats.toString();
	}

	/** What happened when, for the last start: input for better ETAs and a "last start took" view. */
	private static List<Map<String, Object>> timeline(JsonObject isvc, JsonObject pod, List<JsonObject> podEvents,
			VllmStartupLog startupLog) {
		JsonObject init = containerStatus(pod, "initContainerStatuses", INIT_CONTAINER);
		JsonObject main = containerStatus(pod, "containerStatuses", MAIN_CONTAINER);
		Instant created = KubeJson.time(pod, "metadata", "creationTimestamp");
		Instant downloadStart = firstNonNull(KubeJson.time(init, "state", "running", "startedAt"),
				KubeJson.time(init, "state", "terminated", "startedAt"));
		Instant downloadEnd = KubeJson.time(init, "state", "terminated", "finishedAt");
		Instant pullStart = pullSeen(podEvents, MAIN_CONTAINER);
		Instant pullEnd = pullStart == null ? null : pullFinished(podEvents, MAIN_CONTAINER);
		Instant started = KubeJson.time(main, "state", "running", "startedAt");
		Instant weights = timeOf(startupLog, Milestone.WEIGHTS_LOADED);
		Instant compiled = timeOf(startupLog, Milestone.COMPILED);
		Instant serving = firstNonNull(timeOf(startupLog, Milestone.STARTUP_COMPLETE),
				timeOf(startupLog, Milestone.SERVER_STARTING));
		Instant ready = main != null && KubeJson.isTrue(main, "ready") ? conditionTime(isvc, "Ready") : null;

		List<Map<String, Object>> timeline = new ArrayList<>();
		span(timeline, ServedModelPhase.CREATING, created, firstNonNull(downloadStart, started));
		span(timeline, ServedModelPhase.DOWNLOADING_WEIGHTS, downloadStart, downloadEnd);
		span(timeline, ServedModelPhase.PULLING_IMAGE, pullStart, pullEnd);
		span(timeline, ServedModelPhase.LOADING_WEIGHTS, started, weights);
		span(timeline, ServedModelPhase.COMPILING, weights, compiled);
		span(timeline, ServedModelPhase.WARMING, compiled, serving);
		span(timeline, ServedModelPhase.FINISHING, serving, ready);
		return timeline;
	}

	private static void span(List<Map<String, Object>> timeline, ServedModelPhase phase, Instant start, Instant end) {
		if (start == null) {
			return;
		}
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put("phase", phase.name());
		entry.put("startedAt", start.toString());
		entry.put("endedAt", end == null ? null : end.toString());
		entry.put("seconds", end == null ? null : Math.max(0, Duration.between(start, end).toSeconds()));
		timeline.add(entry);
	}

	private static void set(ServedModelStatus status, ServedModelState state, ServedModelPhase phase,
			String message, Instant since) {
		status.state = state;
		status.phase = phase;
		status.message = message;
		status.phaseSince = since;
	}

	private static void phase(ServedModelStatus status, ServedModelPhase phase, Instant since) {
		set(status, ServedModelState.STARTING, phase, phase.getMessage(), since);
	}

	/** What the storage initializer does; for a pvc:// copy, when it runs at all, it links the files. */
	private static String fetching(String storageUri, String what) {
		VolumeUri volume = VolumeUri.of(storageUri);
		return volume == null ? "Downloading " + what + source(storageUri)
				: "Linking " + what + " from volume " + volume.claim();
	}

	private static String source(String storageUri) {
		if (storageUri == null) {
			return "";
		}
		if (storageUri.startsWith("hf://")) {
			return " from Hugging Face";
		}
		if (storageUri.startsWith("s3://")) {
			return " from S3 (MinIO)";
		}
		int scheme = storageUri.indexOf("://");
		return scheme > 0 ? " from " + storageUri.substring(0, scheme) : "";
	}

	/** vLLM takes the last value of a repeated flag, so the InferenceService's own arguments win. */
	static Double lastFlagValue(List<String> args, String flag) {
		Double value = null;
		for (int index = 0; index < args.size(); index++) {
			String arg = args.get(index);
			String text = null;
			if (arg.startsWith(flag + "=")) {
				text = arg.substring(flag.length() + 1);
			} else if (arg.equals(flag) && index + 1 < args.size()) {
				text = args.get(index + 1);
			}
			if (text != null) {
				try {
					value = Double.valueOf(text.strip());
				} catch (NumberFormatException e) {
					// leave the previous value
				}
			}
		}
		return value;
	}

	private static Map<String, Object> parseLastAction(String json) {
		if (json == null) {
			return null;
		}
		try {
			JsonObject parsed = JsonParser.parseString(json).getAsJsonObject();
			return toMap(parsed);
		} catch (RuntimeException e) {
			return null;
		}
	}

	/** When the current run was asked for: SEMOSS's record, else when KServe saw the stop lifted. */
	private static Instant startRequestedAt(JsonObject isvc, Map<String, Object> lastAction) {
		if (lastAction != null && "start".equals(lastAction.get("action")) && lastAction.get("at") instanceof String at) {
			Instant instant = KubeJson.parseTime(at);
			if (instant != null) {
				return instant;
			}
		}
		JsonObject stopped = condition(KubeJson.objects(isvc, "status", "conditions"), "Stopped");
		if (stopped != null && "False".equals(KubeJson.str(stopped, "status"))) {
			Instant since = KubeJson.time(stopped, "lastTransitionTime");
			if (since != null) {
				return since;
			}
		}
		return KubeJson.time(isvc, "metadata", "creationTimestamp");
	}

	/** @return the newest predictor pod that is not being deleted, or null */
	static JsonObject newestLivePod(List<JsonObject> pods) {
		return pods.stream().filter(pod -> KubeJson.str(pod, "metadata", "deletionTimestamp") == null)
				.max(Comparator.comparing(pod -> firstNonNull(KubeJson.time(pod, "metadata", "creationTimestamp"),
						Instant.EPOCH)))
				.orElse(null);
	}

	private static boolean declaresContainer(JsonObject pod, String field, String name) {
		for (JsonObject container : KubeJson.objects(pod, "spec", field)) {
			if (name.equals(KubeJson.str(container, "name"))) {
				return true;
			}
		}
		return false;
	}

	static JsonObject containerStatus(JsonObject pod, String field, String name) {
		for (JsonObject container : KubeJson.objects(pod, "status", field)) {
			if (name.equals(KubeJson.str(container, "name"))) {
				return container;
			}
		}
		return null;
	}

	private static int restartCount(JsonObject container) {
		Integer count = container == null ? null : KubeJson.integer(container, "restartCount");
		return count == null ? 0 : count;
	}

	private static boolean isTerminated(JsonObject container) {
		return KubeJson.obj(container, "state", "terminated") != null;
	}

	private static int exitCode(JsonObject terminated) {
		Integer code = KubeJson.integer(terminated, "exitCode");
		return code == null ? 0 : code;
	}

	private static JsonObject condition(List<JsonObject> conditions, String type) {
		for (JsonObject condition : conditions) {
			if (type.equals(KubeJson.str(condition, "type"))) {
				return condition;
			}
		}
		return null;
	}

	private static String conditionStatus(List<JsonObject> conditions, String type) {
		JsonObject condition = condition(conditions, type);
		return condition == null ? null : KubeJson.str(condition, "status");
	}

	private static Instant conditionTime(JsonObject isvc, String type) {
		JsonObject condition = condition(KubeJson.objects(isvc, "status", "conditions"), type);
		return condition == null ? null : KubeJson.time(condition, "lastTransitionTime");
	}

	/** Scheduler events carry only eventTime; kubelet and controller events lastTimestamp. */
	static Instant eventTime(JsonObject event) {
		return firstNonNull(KubeJson.time(event, "lastTimestamp"), KubeJson.time(event, "eventTime"),
				KubeJson.time(event, "firstTimestamp"), KubeJson.time(event, "metadata", "creationTimestamp"));
	}

	private static List<JsonObject> eventsOf(List<JsonObject> events, String kind, String name) {
		List<JsonObject> result = new ArrayList<>();
		if (events == null || name == null) {
			return result;
		}
		for (JsonObject event : events) {
			if (kind.equals(KubeJson.str(event, "involvedObject", "kind"))
					&& name.equals(KubeJson.str(event, "involvedObject", "name"))) {
				result.add(event);
			}
		}
		result.sort(Comparator.comparing(event -> firstNonNull(eventTime(event), Instant.EPOCH)));
		return result;
	}

	private static List<Map<String, Object>> displayEvents(List<JsonObject> events, String isvcName,
			String podName) {
		List<JsonObject> relevant = new ArrayList<>(eventsOf(events, "InferenceService", isvcName));
		relevant.addAll(eventsOf(events, "Pod", podName));
		relevant.removeIf(event -> NOISE_EVENT_REASONS.contains(KubeJson.str(event, "reason"))
				|| String.valueOf(KubeJson.str(event, "message")).contains(CONFLICT_MESSAGE));
		relevant.sort(Comparator.comparing((JsonObject event) -> firstNonNull(eventTime(event), Instant.EPOCH))
				.reversed());
		List<Map<String, Object>> result = new ArrayList<>();
		for (JsonObject event : relevant.subList(0, Math.min(MAX_EVENTS, relevant.size()))) {
			Map<String, Object> entry = new LinkedHashMap<>();
			Instant time = eventTime(event);
			entry.put("time", time == null ? null : time.toString());
			entry.put("type", KubeJson.str(event, "type"));
			entry.put("reason", KubeJson.str(event, "reason"));
			entry.put("object", KubeJson.str(event, "involvedObject", "kind") + "/"
					+ KubeJson.str(event, "involvedObject", "name"));
			entry.put("container", container(KubeJson.str(event, "involvedObject", "fieldPath")));
			entry.put("message", KubeJson.str(event, "message"));
			entry.put("count", KubeJson.integer(event, "count"));
			result.add(entry);
		}
		return result;
	}

	/** {@code spec.containers{kserve-container}} names the container an event is about. */
	private static String container(String fieldPath) {
		if (fieldPath == null) {
			return null;
		}
		int open = fieldPath.indexOf('{');
		int close = fieldPath.indexOf('}');
		return open >= 0 && close > open ? fieldPath.substring(open + 1, close) : null;
	}

	private static List<JsonObject> pullEvents(List<JsonObject> podEvents, String container) {
		List<JsonObject> result = new ArrayList<>();
		for (JsonObject event : podEvents) {
			if (container.equals(container(KubeJson.str(event, "involvedObject", "fieldPath")))
					&& PULL_EVENT_REASONS.contains(KubeJson.str(event, "reason"))) {
				result.add(event);
			}
		}
		return result;
	}

	/** @return when the container's image pull started, if the latest pull event says it is still pulling */
	private static Instant pullInProgress(List<JsonObject> podEvents, String container) {
		List<JsonObject> pulls = pullEvents(podEvents, container);
		if (pulls.isEmpty()) {
			return null;
		}
		JsonObject latest = pulls.get(pulls.size() - 1);
		return "Pulling".equals(KubeJson.str(latest, "reason")) ? eventTime(latest) : null;
	}

	/** @return when a real pull (not "already present on machine") of the container's image started */
	private static Instant pullSeen(List<JsonObject> podEvents, String container) {
		for (JsonObject event : pullEvents(podEvents, container)) {
			if ("Pulling".equals(KubeJson.str(event, "reason"))) {
				return eventTime(event);
			}
		}
		return null;
	}

	private static Instant pullFinished(List<JsonObject> podEvents, String container) {
		for (JsonObject event : pullEvents(podEvents, container)) {
			if ("Pulled".equals(KubeJson.str(event, "reason"))) {
				return eventTime(event);
			}
		}
		return null;
	}

	private static boolean startupProbeKilled(List<JsonObject> podEvents) {
		for (JsonObject event : podEvents) {
			String message = KubeJson.str(event, "message");
			if ("Killing".equals(KubeJson.str(event, "reason")) && message != null
					&& message.contains("startup probe")) {
				return true;
			}
		}
		return false;
	}

	private static boolean reached(VllmStartupLog log, Milestone milestone) {
		return log != null && log.hasReached(milestone);
	}

	private static Instant timeOf(VllmStartupLog log, Milestone milestone) {
		return log == null ? null : log.timeOf(milestone);
	}

	@SafeVarargs
	private static <T> T firstNonNull(T... values) {
		for (T value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	private static Map<String, Object> toMap(JsonObject object) {
		return GSON.fromJson(object, new TypeToken<LinkedHashMap<String, Object>>() {
		}.getType());
	}

	private static List<Map<String, Object>> toMaps(List<JsonObject> objects) {
		List<Map<String, Object>> result = new ArrayList<>();
		for (JsonObject object : objects) {
			result.add(toMap(object));
		}
		return result;
	}
}
