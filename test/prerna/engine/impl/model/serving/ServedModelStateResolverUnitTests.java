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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.impl.model.serving.ServedModelFailure.Cause;
import prerna.util.k8s.KubeJson;

class ServedModelStateResolverUnitTests {

	private static ServedModelStatus resolve(String fixture, Instant now) {
		return resolve(fixture, Fixtures.items(fixture + "-pods.json"), null, now);
	}

	private static ServedModelStatus resolve(String fixture, List<JsonObject> pods, VllmStartupLog log, Instant now) {
		return ServedModelStateResolver.resolve(Fixtures.json(fixture + "-isvc.json"), pods,
				Fixtures.items(fixture + "-events.json"), log, now);
	}

	private static JsonObject container(JsonObject pod, String field, String name) {
		return ServedModelStateResolver.containerStatus(pod, field, name);
	}

	private static JsonObject main(JsonObject pod) {
		return container(pod, "containerStatuses", ServedModelService.MAIN_CONTAINER);
	}

	private static JsonObject json(String text) {
		return JsonParser.parseString(text).getAsJsonObject();
	}

	@Test
	void ready() {
		ServedModelStatus status = resolve("ready", Instant.parse("2026-10-26T23:56:52Z"));

		assertEquals(ServedModelState.READY, status.getState());
		assertNull(status.getPhase());
		assertEquals(1.0, status.getProgress());
		assertEquals(0L, status.getEtaSeconds());
		assertEquals("olmo-2-1b-instruct", status.getServedModel());
		assertEquals("olmo-2-1b-instruct-predictor-bfc8475f7-7x5nh", status.getPod());
		assertFalse(status.isManaged(), "the sample was applied with kubectl, not by SEMOSS");
		Map<String, Object> map = status.toDetailMap();
		assertEquals(0.2, map.get("gpuMemoryUtilization"), "the InferenceService's arg wins over the runtime's 0.5");
		assertEquals("hf://allenai/OLMo-2-0425-1B-Instruct", map.get("storageUri"));
		assertEquals("nvidia-vllm", map.get("runtime"));
		assertEquals("http://olmo-2-1b-instruct-predictor.semoss.svc.cluster.local", map.get("inClusterUrl"));
		assertEquals(4, status.conditions.size());
	}

	@Test
	void stoppingThenStopped() {
		ServedModelStatus stopping = resolve("stopping", Instant.parse("2026-10-26T23:56:53Z"));
		assertEquals(ServedModelState.STOPPING, stopping.getState());
		assertNull(stopping.getProgress());

		ServedModelStatus stopped = resolve("stopped", Instant.parse("2026-10-26T23:56:56Z"));
		assertEquals(ServedModelState.STOPPED, stopped.getState());
		assertNull(stopped.getPod());
		assertEquals("Stopped", stopped.getMessage());
	}

	@Test
	void createdStoppedHasNoPods() {
		ServedModelStatus status = resolve("created-stopped", Instant.parse("2026-10-27T00:00:26Z"));
		assertEquals(ServedModelState.STOPPED, status.getState());
		assertEquals("semoss-verify-pending", status.getServedModel());
	}

	@Test
	void startPhasesInOrderWithRisingProgress() {
		ServedModelStatus created = resolve("starting-created", Instant.parse("2026-10-26T23:57:07Z"));
		assertEquals(ServedModelState.STARTING, created.getState());
		assertEquals(ServedModelPhase.CREATING, created.getPhase());

		ServedModelStatus downloading = resolve("starting-downloading", Instant.parse("2026-10-26T23:57:13Z"));
		assertEquals(ServedModelPhase.DOWNLOADING_WEIGHTS, downloading.getPhase());
		assertEquals("Downloading the model weights from Hugging Face", downloading.getMessage());

		List<JsonObject> loadingPods = Fixtures.items("starting-loading-pods.json");
		ServedModelStatus loading = resolve("starting-loading", loadingPods, null,
				Instant.parse("2026-10-26T23:57:50Z"));
		assertEquals(ServedModelPhase.LOADING_WEIGHTS, loading.getPhase());
		assertEquals(Instant.parse("2026-10-26T23:57:46Z"), loading.phaseSince, "the container's start");

		ServedModelStatus compiling = resolve("starting-loading", loadingPods,
				VllmStartupLog.parse(Fixtures.vllmLogUntil("Loading weights took")),
				Instant.parse("2026-10-26T23:58:20Z"));
		assertEquals(ServedModelPhase.COMPILING, compiling.getPhase());
		assertEquals(Instant.parse("2026-10-26T23:58:17.641115406Z"), compiling.phaseSince);

		ServedModelStatus warming = resolve("starting-loading", loadingPods,
				VllmStartupLog.parse(Fixtures.vllmLogUntil("torch.compile takes")),
				Instant.parse("2026-10-26T23:58:35Z"));
		assertEquals(ServedModelPhase.WARMING, warming.getPhase());

		ServedModelStatus finishing = resolve("starting-loading", loadingPods,
				VllmStartupLog.parse(Fixtures.vllmLogUntil("Starting vLLM API server")),
				Instant.parse("2026-10-26T23:58:59.500Z"));
		assertEquals(ServedModelPhase.FINISHING, finishing.getPhase());

		double previous = -1;
		for (ServedModelStatus status : List.of(created, downloading, loading, compiling, warming, finishing)) {
			assertNotNull(status.getProgress(), status.getPhase().name());
			assertTrue(status.getProgress() > previous, status.getPhase() + " " + status.getProgress());
			assertTrue(status.getProgress() < 1.0);
			assertTrue(status.getEtaSeconds() > 0);
			previous = status.getProgress();
		}
		assertTrue(created.getEtaSeconds() > finishing.getEtaSeconds());
	}

	@Test
	void readyAfterRestartWithTimelineAndVllmDetails() {
		ServedModelStatus status = resolve("restarted-ready", Fixtures.items("restarted-ready-pods.json"),
				VllmStartupLog.parse(List.of(Fixtures.text("vllm-startup.log").split("\n"))),
				Instant.parse("2026-10-26T23:59:11Z"));

		assertEquals(ServedModelState.READY, status.getState());
		assertTrue(status.getWarnings().isEmpty());
		Map<String, Object> details = status.toDetailMap();
		@SuppressWarnings("unchecked")
		Map<String, Object> vllm = (Map<String, Object>) details.get("vllm");
		assertEquals("Olmo2ForCausalLM", vllm.get("architecture"));
		assertEquals(4096, vllm.get("maxModelLen"));
		assertEquals(2.7669, vllm.get("weightsGiB"));
		assertEquals(19.63, vllm.get("kvCacheGiB"));
		assertEquals("miss", vllm.get("compileCache"), "the recorded start compiled the model afresh");

		Map<String, Long> seconds = new java.util.LinkedHashMap<>();
		for (Map<String, Object> phase : status.phases) {
			seconds.put((String) phase.get("phase"), (Long) phase.get("seconds"));
		}
		assertEquals(List.of("CREATING", "DOWNLOADING_WEIGHTS", "LOADING_WEIGHTS", "COMPILING", "WARMING", "FINISHING"),
				new ArrayList<>(seconds.keySet()));
		assertEquals(38L, seconds.get("DOWNLOADING_WEIGHTS"));
		assertEquals(31L, seconds.get("LOADING_WEIGHTS"));
		assertEquals(13L, seconds.get("COMPILING"));
		assertEquals(28L, seconds.get("WARMING"));
		assertNotNull(seconds.get("FINISHING"));
	}

	@Test
	void eventsLeaveOutKServeNoise() {
		ServedModelStatus status = resolve("restarted-ready", Instant.parse("2026-10-26T23:59:11Z"));
		assertFalse(status.events.isEmpty());
		for (Map<String, Object> event : status.events) {
			assertFalse("VirtualServiceCRDNotFound".equals(event.get("reason")), event.toString());
			assertFalse(String.valueOf(event.get("message")).contains("the object has been modified"),
					event.toString());
		}
	}

	@Test
	void waitingForTheGpu() {
		ServedModelStatus status = resolve("pending-gpu", Instant.parse("2026-10-27T00:00:48Z"));

		assertEquals(ServedModelState.WAITING_FOR_GPU, status.getState());
		assertTrue(status.getMessage().contains("Insufficient nvidia.com/gpu"), status.getMessage());
		assertNull(status.getProgress());
		Map<String, Object> scheduling = status.events.stream()
				.filter(event -> "FailedScheduling".equals(event.get("reason"))).findFirst().orElseThrow();
		assertNotNull(scheduling.get("time"), "scheduler events carry only eventTime");

		ServedModelStateResolver.explainGpuWait(status, List.of("olmo-2-1b-instruct"));
		assertEquals("Waiting for the GPU, which olmo-2-1b-instruct is using", status.getMessage());
	}

	@Test
	void crashLoopKeepsFailedBetweenRetriesAndExplainsVllmError() {
		List<JsonObject> pods = Fixtures.items("starting-loading-pods.json");
		JsonObject main = main(pods.get(0));
		main.add("state", json("{\"waiting\":{\"reason\":\"CrashLoopBackOff\"}}"));
		main.add("lastState", json("{\"terminated\":{\"exitCode\":1,\"reason\":\"Error\"}}"));
		main.addProperty("restartCount", 2);

		ServedModelStatus status = resolve("starting-loading", pods, null, Instant.parse("2026-10-26T23:59:30Z"));

		assertEquals(ServedModelState.FAILED, status.getState());
		ServedModelFailure failure = status.getFailure();
		assertEquals(Cause.MODEL_CRASH, failure.getCause());
		assertEquals("kserve-container", failure.getContainer());
		assertTrue(failure.isPreviousInstance());
		assertEquals(2, failure.getRestarts());
		assertTrue(failure.getMessage().contains("exited with code 1"), failure.getMessage());
		assertTrue(failure.getMessage().contains("restart 2"), failure.getMessage());

		String error = "ValueError: Free memory on device (5.02/119.7 GiB) on startup is less than desired GPU "
				+ "memory utilization (0.95, 113.71 GiB). Decrease GPU memory utilization or reduce GPU memory "
				+ "used by other processes.";
		ServedModelStateResolver.explainFailure(status,
				List.of("2026-10-26T23:59:20.1Z (EngineCore_DP0 pid=129) Traceback (most recent call last):",
						"2026-10-26T23:59:20.2Z (EngineCore_DP0 pid=129)   File \"gpu_worker.py\", line 220",
						"2026-10-26T23:59:20.3Z (EngineCore_DP0 pid=129) " + error));
		assertTrue(status.getMessage().endsWith(error), status.getMessage());
		assertTrue(failure.getHint().contains("--gpu-memory-utilization"), failure.getHint());
		assertEquals(3, failure.getLogTail().size());
	}

	@Test
	void outOfMemoryKill() {
		List<JsonObject> pods = Fixtures.items("starting-loading-pods.json");
		JsonObject main = main(pods.get(0));
		main.add("lastState", json("{\"terminated\":{\"exitCode\":137,\"reason\":\"OOMKilled\"}}"));
		main.addProperty("restartCount", 1);

		ServedModelStatus status = resolve("starting-loading", pods, null, Instant.parse("2026-10-26T23:59:30Z"));

		assertEquals(ServedModelState.FAILED, status.getState());
		assertEquals(Cause.MODEL_OOM, status.getFailure().getCause());
	}

	@Test
	void startupProbeGaveUp() {
		List<JsonObject> pods = Fixtures.items("starting-loading-pods.json");
		JsonObject main = main(pods.get(0));
		main.add("lastState", json("{\"terminated\":{\"exitCode\":137,\"reason\":\"Error\"}}"));
		main.addProperty("restartCount", 1);
		List<JsonObject> events = Fixtures.items("starting-loading-events.json");
		events.add(json("""
				{"involvedObject": {"kind": "Pod", "name": "olmo-2-1b-instruct-predictor-bfc8475f7-x5l5t",
				  "fieldPath": "spec.containers{kserve-container}"},
				 "reason": "Killing", "type": "Normal", "lastTimestamp": "2026-10-27T00:27:46Z",
				 "message": "Container kserve-container failed startup probe, will be restarted"}"""));

		ServedModelStatus status = ServedModelStateResolver.resolve(Fixtures.json("starting-loading-isvc.json"),
				pods, events, null, Instant.parse("2026-10-27T00:28:00Z"));

		assertEquals(Cause.STARTUP_TIMEOUT, status.getFailure().getCause());
	}

	@Test
	void downloadFailureNamesTheSourceAndHintsAtGatedRepos() {
		List<JsonObject> pods = Fixtures.items("starting-downloading-pods.json");
		JsonObject init = container(pods.get(0), "initContainerStatuses", ServedModelService.INIT_CONTAINER);
		init.add("state", json("{\"terminated\":{\"exitCode\":1,\"reason\":\"Error\"}}"));

		ServedModelStatus status = resolve("starting-downloading", pods, null, Instant.parse("2026-10-26T23:57:30Z"));

		assertEquals(ServedModelState.FAILED, status.getState());
		ServedModelFailure failure = status.getFailure();
		assertEquals(Cause.DOWNLOAD_FAILED, failure.getCause());
		assertEquals("storage-initializer", failure.getContainer());
		assertFalse(failure.isPreviousInstance(), "the failed instance is the current one until it restarts");
		assertTrue(failure.getMessage().contains("from Hugging Face"), failure.getMessage());

		ServedModelStateResolver.explainFailure(status, List.of(
				"2026-10-26T23:57:29Z huggingface_hub.errors.RepositoryNotFoundError: 401 Client Error. (Request ID: x)"));
		assertTrue(failure.getHint().contains("gated"), failure.getHint());
	}

	private static final String CACHED_COMMIT = "0123456789abcdef0123456789abcdef01234567";

	/** The recorded InferenceService, as if its catalog entry took the weights from the model cache */
	private static JsonObject cached(String fixture) {
		JsonObject isvc = Fixtures.json(fixture + "-isvc.json");
		KubeJson.obj(isvc, "spec", "predictor", "model").addProperty("storageUri",
				"pvc://semoss-model-cache/allenai/OLMo-2-0425-1B-Instruct/" + CACHED_COMMIT);
		return isvc;
	}

	@Test
	void weightsFromTheModelCacheAreLinkedInSeconds() {
		Instant now = Instant.parse("2026-10-26T23:57:09Z");
		ServedModelStatus status = ServedModelStateResolver.resolve(cached("starting-downloading"),
				Fixtures.items("starting-downloading-pods.json"), Fixtures.items("starting-downloading-events.json"),
				null, now);

		assertEquals(ServedModelPhase.DOWNLOADING_WEIGHTS, status.getPhase());
		assertEquals("Linking the model weights from volume semoss-model-cache", status.getMessage());
		// 3 s left of the 5 s a link takes, then vLLM's defaults: 35 + 15 + 30 + 10
		assertEquals(3L + 90, status.getEtaSeconds());
		assertEquals(118L + 90, resolve("starting-downloading", now).getEtaSeconds(),
				"a download from Hugging Face budgets 120 s");
	}

	@Test
	void aCopyMissingFromTheModelCacheNamesTheCommandThatFillsIt() {
		List<JsonObject> pods = Fixtures.items("starting-downloading-pods.json");
		JsonObject init = container(pods.get(0), "initContainerStatuses", ServedModelService.INIT_CONTAINER);
		init.add("state", json("{\"terminated\":{\"exitCode\":1,\"reason\":\"Error\"}}"));

		ServedModelStatus status = ServedModelStateResolver.resolve(cached("starting-downloading"), pods,
				Fixtures.items("starting-downloading-events.json"), null, Instant.parse("2026-10-26T23:57:30Z"));

		ServedModelFailure failure = status.getFailure();
		assertEquals(Cause.DOWNLOAD_FAILED, failure.getCause());
		assertEquals("Linking the weights from volume semoss-model-cache failed (exit code 1)", failure.getMessage());
		ServedModelStateResolver.explainFailure(status, List.of("2026-10-26T23:57:29Z 2026-10-26 23:57:29.512 1 "
				+ "storage.initializer ERROR [initializer-entrypoint:<module>():28] Exception: Cannot locate source uri "
				+ "/mnt/pvc/allenai/OLMo-2-0425-1B-Instruct/" + CACHED_COMMIT + " for PVC"));
		assertTrue(failure.getHint().contains("bash models/intake/intake.sh cache allenai/OLMo-2-0425-1B-Instruct "
				+ CACHED_COMMIT), failure.getHint());

		// the same words in a model's crash say nothing about the copy
		ServedModelStatus crash = resolve("starting-loading", Instant.parse("2026-10-26T23:58:00Z"));
		crash.storageUri = "pvc://semoss-model-cache/a/b/c";
		crash.failure = new ServedModelFailure(Cause.MODEL_CRASH, ServedModelService.MAIN_CONTAINER, "Error", 1, 0, false,
				"The model server exited with code 1");
		ServedModelStateResolver.explainFailure(crash, List.of("2026-10-26T23:58:00Z FileNotFoundError: x does not exist"));
		assertNull(crash.getFailure().getHint());
	}

	@Test
	void aMissingVolumeIsUnschedulableAndNamesItsManifest() {
		List<JsonObject> pods = Fixtures.items("pending-gpu-pods.json");
		for (JsonObject condition : KubeJson.objects(pods.get(0), "status", "conditions")) {
			condition.addProperty("message", "0/1 nodes are available: persistentvolumeclaim \"semoss-vllm-cache\" not "
					+ "found. preemption: 0/1 nodes are available: 1 Preemption is not helpful for scheduling.");
		}
		ServedModelStatus status = resolve("pending-gpu", pods, null, Instant.parse("2026-10-27T00:00:48Z"));

		assertEquals(Cause.UNSCHEDULABLE, status.getFailure().getCause());
		assertEquals("Kubernetes cannot schedule the model server: 0/1 nodes are available: persistentvolumeclaim "
				+ "\"semoss-vllm-cache\" not found.", status.getMessage());
		assertTrue(status.getFailure().getHint().contains("models/model-cache.yml"), status.getFailure().getHint());
	}

	@Test
	void imagePullAndConfigErrors() {
		List<JsonObject> pods = Fixtures.items("starting-loading-pods.json");
		main(pods.get(0)).add("state", json(
				"{\"waiting\":{\"reason\":\"ImagePullBackOff\",\"message\":\"Back-off pulling image \\\"nvcr.io/nvidia/vllm:99.99-py3\\\"\"}}"));
		ServedModelStatus pull = resolve("starting-loading", pods, null, Instant.parse("2026-10-26T23:58:00Z"));
		assertEquals(Cause.IMAGE_PULL, pull.getFailure().getCause());
		assertTrue(pull.getMessage().contains("vllm:99.99-py3"), pull.getMessage());

		pods = Fixtures.items("starting-loading-pods.json");
		main(pods.get(0)).add("state", json(
				"{\"waiting\":{\"reason\":\"CreateContainerConfigError\",\"message\":\"secret \\\"hf-token\\\" not found\"}}"));
		ServedModelStatus config = resolve("starting-loading", pods, null, Instant.parse("2026-10-26T23:58:00Z"));
		assertEquals(Cause.CONFIG, config.getFailure().getCause());
		assertTrue(config.getMessage().contains("secret \"hf-token\" not found"), config.getMessage());
	}

	@Test
	void unschedulableForAnotherReason() {
		List<JsonObject> pods = Fixtures.items("pending-gpu-pods.json");
		for (JsonObject condition : KubeJson.objects(pods.get(0), "status", "conditions")) {
			condition.addProperty("message", "0/1 nodes are available: 1 Insufficient memory.");
		}
		ServedModelStatus status = resolve("pending-gpu", pods, null, Instant.parse("2026-10-27T00:00:48Z"));
		assertEquals(ServedModelState.FAILED, status.getState());
		assertEquals(Cause.UNSCHEDULABLE, status.getFailure().getCause());
		assertTrue(status.getMessage().contains("Insufficient memory"));
	}

	@Test
	void pullingTheModelServerImageAddsItsBudget() {
		List<JsonObject> pods = Fixtures.items("starting-loading-pods.json");
		JsonObject main = main(pods.get(0));
		main.add("state", json("{\"waiting\":{\"reason\":\"ContainerCreating\"}}"));
		main.addProperty("ready", false);
		List<JsonObject> events = Fixtures.items("starting-loading-events.json");
		events.removeIf(event -> "spec.containers{kserve-container}".equals(
				KubeJson.str(event, "involvedObject", "fieldPath")));
		events.add(json("""
				{"involvedObject": {"kind": "Pod", "name": "olmo-2-1b-instruct-predictor-bfc8475f7-x5l5t",
				  "fieldPath": "spec.containers{kserve-container}"},
				 "reason": "Pulling", "type": "Normal", "lastTimestamp": "2026-10-26T23:57:46Z",
				 "message": "Pulling image \\"nvcr.io/nvidia/vllm:25.11-py3\\""}"""));

		ServedModelStatus status = ServedModelStateResolver.resolve(Fixtures.json("starting-loading-isvc.json"),
				pods, events, null, Instant.parse("2026-10-26T23:58:46Z"));

		assertEquals(ServedModelPhase.PULLING_IMAGE, status.getPhase());
		assertEquals(Instant.parse("2026-10-26T23:57:46Z"), status.phaseSince);
		assertEquals(420L + 35 + 15 + 30 + 10, status.getEtaSeconds());
	}

	@Test
	void kserveFailedToLoadIsAFallback() {
		JsonObject isvc = Fixtures.json("starting-loading-isvc.json");
		isvc.getAsJsonObject("status").add("modelStatus", json("""
				{"states": {"activeModelState": "", "targetModelState": "FailedToLoad"},
				 "lastFailureInfo": {"reason": "ModelLoadFailed", "message": "Failed to load model", "exitCode": 1}}"""));
		ServedModelStatus status = ServedModelStateResolver.resolve(isvc, Fixtures.items("starting-loading-pods.json"),
				List.of(), null, Instant.parse("2026-10-26T23:58:00Z"));
		assertEquals(Cause.MODEL_CRASH, status.getFailure().getCause());
		assertEquals("Failed to load model", status.getMessage());
	}

	@Test
	void recoveredRetryIsReadyWithAWarning() {
		List<JsonObject> pods = Fixtures.items("restarted-ready-pods.json");
		JsonObject main = main(pods.get(0));
		main.add("lastState", json("{\"terminated\":{\"exitCode\":1,\"reason\":\"Error\"}}"));
		main.addProperty("restartCount", 1);

		ServedModelStatus status = resolve("restarted-ready", pods, null, Instant.parse("2026-10-26T23:59:11Z"));

		assertEquals(ServedModelState.READY, status.getState());
		assertEquals(List.of("The model server restarted 1 time(s)"), status.getWarnings());
	}

	@Test
	void beingRemoved() {
		JsonObject isvc = Fixtures.json("ready-isvc.json");
		isvc.getAsJsonObject("metadata").addProperty("deletionTimestamp", "2026-10-27T01:00:00Z");
		ServedModelStatus status = ServedModelStateResolver.resolve(isvc, Fixtures.items("ready-pods.json"), List.of(),
				null, Instant.parse("2026-10-27T01:00:05Z"));
		assertEquals(ServedModelState.REMOVING, status.getState());
	}

	@Test
	void startRequestedAtComesFromSemossLastAction() {
		JsonObject isvc = Fixtures.json("starting-downloading-isvc.json");
		isvc.getAsJsonObject("metadata").getAsJsonObject("annotations").addProperty(
				ServedModelStateResolver.LAST_ACTION_ANNOTATION,
				"{\"action\":\"start\",\"user\":\"admin-id\",\"at\":\"2026-10-26T23:57:06Z\",\"instance\":\"host\"}");
		ServedModelStatus status = ServedModelStateResolver.resolve(isvc,
				Fixtures.items("starting-downloading-pods.json"), List.of(), null, Instant.parse("2026-10-26T23:57:13Z"));
		assertEquals("2026-10-26T23:57:06Z", status.toSummaryMap().get("startRequestedAt"));
		assertEquals("admin-id", status.getLastAction().get("user"));
	}

	@Test
	void aStoppedModelHasNoStartInProgress() {
		ServedModelStatus status = ServedModelStateResolver.resolve(Fixtures.json("created-stopped-isvc.json"),
				List.of(), List.of(), null, Instant.parse("2026-10-27T17:56:00Z"));

		assertEquals(ServedModelState.STOPPED, status.getState());
		assertNull(status.toSummaryMap().get("startRequestedAt"), "defined stopped, never started");
	}

	@Test
	void lastFlagValueWins() {
		assertEquals(0.3, ServedModelStateResolver.lastFlagValue(
				List.of("--gpu-memory-utilization=0.5", "--max-model-len=4096", "--gpu-memory-utilization", "0.3"),
				"--gpu-memory-utilization"));
		assertNull(ServedModelStateResolver.lastFlagValue(List.of("--max-model-len=4096"), "--gpu-memory-utilization"));
	}

	@Test
	void brandNewPodWithoutStatusesIsBeingCreated() {
		List<JsonObject> pods = Fixtures.items("starting-created-pods.json");
		JsonObject podStatus = pods.get(0).getAsJsonObject("status");
		podStatus.remove("initContainerStatuses");
		podStatus.remove("containerStatuses");

		ServedModelStatus status = ServedModelStateResolver.resolve(Fixtures.json("starting-created-isvc.json"), pods,
				List.of(), null, Instant.parse("2026-10-26T23:57:07Z"));

		assertEquals(ServedModelPhase.CREATING, status.getPhase());
		assertEquals("Creating the model server pod", status.getMessage());
		assertEquals("defaults", status.toSummaryMap().get("etaBasis"));
	}

	@Test
	void etaComesFromTheLastStart() {
		JsonObject isvc = Fixtures.json("starting-downloading-isvc.json");
		isvc.getAsJsonObject("metadata").getAsJsonObject("annotations").addProperty(
				ServedModelStateResolver.START_STATS_ANNOTATION, "{\"at\":\"2026-10-27T01:20:30Z\",\"seconds\":"
						+ "{\"CREATING\":0,\"DOWNLOADING_WEIGHTS\":25,\"LOADING_WEIGHTS\":32,\"COMPILING\":13,"
						+ "\"WARMING\":29,\"FINISHING\":1}}");
		List<JsonObject> pods = Fixtures.items("starting-downloading-pods.json");
		Instant downloadStarted = KubeJson.time(
				container(pods.get(0), "initContainerStatuses", ServedModelService.INIT_CONTAINER), "state", "running",
				"startedAt");

		ServedModelStatus status = ServedModelStateResolver.resolve(isvc, pods, List.of(), null,
				downloadStarted.plusSeconds(10));

		assertEquals(ServedModelPhase.DOWNLOADING_WEIGHTS, status.getPhase());
		assertEquals("last start", status.toSummaryMap().get("etaBasis"));
		// budgets 1 (CREATING took 0 s) + 25 + 32 + 13 + 29 + 1 = 101 s, 10 s into the 25 s download
		assertEquals(15L + 32 + 13 + 29 + 1, status.getEtaSeconds());
		assertEquals(Math.round((1 + 10.0) / 101 * 100) / 100.0, status.getProgress());
	}

	@Test
	void startStatsRecordWhatThisPodMeasuredOverWhatWasKnown() {
		ServedModelStatus ready = resolve("restarted-ready", Fixtures.items("restarted-ready-pods.json"),
				VllmStartupLog.parse(List.of(Fixtures.text("vllm-startup.log").split("\n"))),
				Instant.parse("2026-10-26T23:59:11Z"));

		String json = ServedModelStateResolver.startStats(ready,
				"{\"seconds\":{\"PULLING_IMAGE\":480,\"DOWNLOADING_WEIGHTS\":141}}", Instant.parse("2026-10-27T00:10:00Z"));

		assertEquals(Map.of(ServedModelPhase.CREATING, 0, ServedModelPhase.DOWNLOADING_WEIGHTS, 38,
				ServedModelPhase.PULLING_IMAGE, 480, ServedModelPhase.LOADING_WEIGHTS, 31, ServedModelPhase.COMPILING, 13,
				ServedModelPhase.WARMING, 28, ServedModelPhase.FINISHING, 7), ServedModelStateResolver.parseStartStats(json));
		assertTrue(json.contains("\"pod\":\"olmo-2-1b-instruct-predictor-bfc8475f7-x5l5t\""), json);
	}

	@Test
	void nothingMeasuredMeansNoStartStats() {
		ServedModelStatus stopped = resolve("stopped", Instant.parse("2026-10-26T23:56:56Z"));
		assertNull(ServedModelStateResolver.startStats(stopped, null, Instant.parse("2026-10-26T23:57:00Z")));
		assertTrue(ServedModelStateResolver.parseStartStats("not json").isEmpty());
		assertTrue(ServedModelStateResolver.parseStartStats("{\"seconds\":{\"WARP\":3,\"COMPILING\":-1}}").isEmpty());
	}

	/** Seen live: FINISHING's 1 s budget ran out while the API server came up, and the start read 100%. */
	@Test
	void aStartNeverReadsDoneBeforeReady() {
		JsonObject isvc = Fixtures.json("starting-loading-isvc.json");
		isvc.getAsJsonObject("metadata").getAsJsonObject("annotations").addProperty(
				ServedModelStateResolver.START_STATS_ANNOTATION, "{\"seconds\":{\"CREATING\":0,"
						+ "\"DOWNLOADING_WEIGHTS\":25,\"LOADING_WEIGHTS\":31,\"COMPILING\":13,\"WARMING\":28,\"FINISHING\":0}}");
		ServedModelStatus status = ServedModelStateResolver.resolve(isvc, Fixtures.items("starting-loading-pods.json"),
				List.of(), VllmStartupLog.parse(Fixtures.vllmLogUntil("Starting vLLM API server")),
				Instant.parse("2026-10-26T23:59:05Z"));

		assertEquals(ServedModelPhase.FINISHING, status.getPhase());
		assertEquals(0.99, status.getProgress());
		assertEquals(1L, status.getEtaSeconds());
	}

	/** the download in starting-downloading began at 23:57:07 */
	private static final Instant DOWNLOAD_STARTED = Instant.parse("2026-10-26T23:57:07Z");

	@Test
	void aDownloadFarBeyondItsBudgetIsFlagged() {
		ServedModelStatus nine = resolve("starting-downloading", DOWNLOAD_STARTED.plusSeconds(9 * 60));
		assertTrue(nine.getWarnings().isEmpty(), nine.getWarnings().toString());

		ServedModelStatus eleven = resolve("starting-downloading", DOWNLOAD_STARTED.plusSeconds(11 * 60));

		assertEquals(ServedModelState.STARTING, eleven.getState(), "a warning, not a failure");
		assertEquals(List.of("The weight download has taken 11 min, against about 2 min expected, so it may be stuck. "
				+ "GetServedModelLogs with container storage-initializer shows the download."), eleven.getWarnings());
		assertNull(eleven.getEtaSeconds(), "no estimate for a phase that may be stuck");
	}

	@Test
	void aLearnedBudgetMovesTheThreshold() {
		JsonObject isvc = Fixtures.json("starting-downloading-isvc.json");
		isvc.getAsJsonObject("metadata").getAsJsonObject("annotations").addProperty(
				ServedModelStateResolver.START_STATS_ANNOTATION,
				"{\"at\":\"2026-10-26T23:00:00Z\",\"seconds\":{\"DOWNLOADING_WEIGHTS\":400}}");
		List<JsonObject> pods = Fixtures.items("starting-downloading-pods.json");

		assertTrue(ServedModelStateResolver.resolve(isvc, pods, List.of(), null, DOWNLOAD_STARTED.plusSeconds(11 * 60))
				.getWarnings().isEmpty(), "three times 400 s is 20 min");
		List<String> late = ServedModelStateResolver
				.resolve(isvc, pods, List.of(), null, DOWNLOAD_STARTED.plusSeconds(21 * 60)).getWarnings();
		assertTrue(late.get(0).startsWith("The weight download has taken 21 min, against about 7 min expected"),
				late.toString());
	}

	@Test
	void noPodLongAfterTheStartMeansKServeHasNotActed() {
		JsonObject isvc = Fixtures.json("created-stopped-isvc.json");
		JsonObject annotations = isvc.getAsJsonObject("metadata").getAsJsonObject("annotations");
		annotations.addProperty(ServedModelStateResolver.STOP_ANNOTATION, "false");
		annotations.addProperty(ServedModelStateResolver.LAST_ACTION_ANNOTATION,
				"{\"action\":\"start\",\"at\":\"2026-10-27T17:00:00Z\",\"instance\":\"host\"}");

		ServedModelStatus soon = ServedModelStateResolver.resolve(isvc, List.of(), List.of(), null,
				Instant.parse("2026-10-27T17:01:00Z"));
		assertEquals(ServedModelPhase.CREATING, soon.getPhase());
		assertTrue(soon.getWarnings().isEmpty(), soon.getWarnings().toString());

		ServedModelStatus late = ServedModelStateResolver.resolve(isvc, List.of(), List.of(), null,
				Instant.parse("2026-10-27T17:03:00Z"));
		assertEquals(List.of("KServe has not created the model server pod 3 min after the start; check that "
				+ "kserve-controller-manager runs (kubectl -n kserve get pods)"), late.getWarnings());
	}

	@Test
	void durationsReadLikeSpeech() {
		assertEquals("45 s", ServedModelStateResolver.duration(45));
		assertEquals("10 min", ServedModelStateResolver.duration(600));
		assertEquals("1 h 5 min", ServedModelStateResolver.duration(3900));
	}
}
