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

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.util.k8s.KubeJson;

class ServedModelServiceUnitTests {

	private static final String SAMPLE = "olmo-2-1b-instruct";
	private static final String ISVC_PATH = "/apis/serving.kserve.io/v1beta1/namespaces/semoss/inferenceservices";

	@TempDir
	Path dir;

	private FakeKubeApi api;

	@BeforeEach
	void startApi() throws Exception {
		this.api = new FakeKubeApi();
	}

	@AfterEach
	void stopApi() {
		this.api.close();
	}

	private void sample(String fixture) {
		this.api.inferenceServices.add(Fixtures.json(fixture + "-isvc.json"));
		this.api.pods.addAll(Fixtures.items(fixture + "-pods.json"));
		this.api.events.addAll(Fixtures.items(fixture + "-events.json"));
	}

	@Test
	void statusOfAReadyModelReadsTheStartOfVllmsLog() throws Exception {
		sample("ready");
		this.api.log = Fixtures.text("vllm-startup.log");

		ServedModelStatus status = this.api.service(this.dir, Instant.parse("2026-10-26T23:56:00Z")).status(SAMPLE);

		assertEquals(ServedModelState.READY, status.getState());
		assertEquals(4096, status.maxModelLen);
		List<FakeKubeApi.Request> logs = this.api.requests("GET", "/api/v1/namespaces/semoss/pods/");
		assertEquals(1, logs.size());
		Map<String, String> query = logs.get(0).query();
		assertEquals("kserve-container", query.get("container"));
		assertEquals("2026-10-26T22:54:04Z", query.get("sinceTime"), "since the vLLM container started");
		assertEquals("262144", query.get("limitBytes"));
		assertEquals("true", query.get("timestamps"));
		assertEquals("/api/v1/namespaces/semoss/pods/olmo-2-1b-instruct-predictor-bfc8475f7-7x5nh/log",
				logs.get(0).path());
	}

	@Test
	void stopPatchesTheAnnotationOnce() throws Exception {
		sample("ready");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-26T23:56:52Z"));

		ServedModelChange stop = service.stop(SAMPLE, null);

		assertTrue(stop.changed());
		assertEquals(ServedModelState.STOPPING, stop.status().getState(), "the pod is still there");
		List<FakeKubeApi.Request> patches = this.api.requests("PATCH", ISVC_PATH + "/" + SAMPLE);
		assertEquals(1, patches.size());
		assertTrue(patches.get(0).contentType().startsWith("application/merge-patch+json"));
		JsonObject annotations = KubeJson.obj(JsonParser.parseString(patches.get(0).body()).getAsJsonObject(),
				"metadata", "annotations");
		assertEquals("true", KubeJson.str(annotations, ServedModelStateResolver.STOP_ANNOTATION));
		JsonObject lastAction = JsonParser
				.parseString(KubeJson.str(annotations, ServedModelStateResolver.LAST_ACTION_ANNOTATION))
				.getAsJsonObject();
		assertEquals("stop", KubeJson.str(lastAction, "action"));
		assertEquals("2026-10-26T23:56:52Z", KubeJson.str(lastAction, "at"));
		assertTrue(annotations.has(ServedModelStateResolver.START_STATS_ANNOTATION));
		assertEquals(3, annotations.size(), "the patch touches nothing else");

		ServedModelChange again = service.stop(SAMPLE, null);
		assertFalse(again.changed());
		assertEquals(1, this.api.requests("PATCH", ISVC_PATH).size());
	}

	@Test
	void startLiftsTheStop() throws Exception {
		sample("stopped");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-26T23:57:07Z"));

		ServedModelChange start = service.start(SAMPLE, null);

		assertTrue(start.changed());
		assertEquals(ServedModelState.STARTING, start.status().getState());
		assertEquals(ServedModelPhase.CREATING, start.status().getPhase());
		JsonObject annotations = KubeJson.obj(
				JsonParser.parseString(this.api.requests("PATCH", ISVC_PATH).get(0).body()).getAsJsonObject(),
				"metadata", "annotations");
		assertEquals("false", KubeJson.str(annotations, ServedModelStateResolver.STOP_ANNOTATION));
	}

	@Test
	void logsDropProbeLinesAndKeepTheTail() throws Exception {
		sample("ready");
		this.api.log = Fixtures.text("vllm-startup.log");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-26T23:56:00Z"));

		ServedModelLogs quiet = service.logs(SAMPLE, ServedModelService.MAIN_CONTAINER, 5, false, false);
		assertEquals(5, quiet.lines().size());
		assertTrue(quiet.lines().stream().noneMatch(line -> line.contains("GET /health")));
		assertTrue(quiet.lines().get(4).endsWith("Application startup complete."));
		Map<String, String> query = this.api.requests("GET", "/api/v1/namespaces/semoss/pods/").get(0).query();
		assertEquals("25", query.get("tailLines"), "reads extra lines to make up for the dropped ones");
		assertNull(query.get("previous"));

		ServedModelLogs all = service.logs(SAMPLE, ServedModelService.MAIN_CONTAINER, 2, true, true);
		assertTrue(all.lines().get(1).contains("GET /v1/models"));
		assertEquals("true", this.api.requests("GET", "/api/v1/namespaces/semoss/pods/").get(1).query().get("previous"));
	}

	@Test
	void rejectsBadArgumentsBeforeCallingKubernetes() throws Exception {
		sample("ready");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-26T23:56:00Z"));

		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> service.logs(SAMPLE, "sidecar", null, false, false)).getMessage().contains("container must be"));
		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> service.logs(SAMPLE, ServedModelService.MAIN_CONTAINER, 0, false, false)).getMessage()
				.contains("tailLines"));
		assertTrue(assertThrows(IllegalArgumentException.class, () -> service.status("../secrets")).getMessage()
				.contains("Invalid served model name"));
		assertTrue(this.api.requests.isEmpty());

		assertEquals("No served model named missing in namespace semoss",
				assertThrows(IllegalArgumentException.class, () -> service.status("missing")).getMessage());
	}

	@Test
	void stoppedModelHasNoLogs() throws Exception {
		sample("stopped");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-26T23:57:00Z"));
		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> service.logs(SAMPLE, ServedModelService.MAIN_CONTAINER, null, false, false)).getMessage()
				.contains("has no pod"));
	}

	@Test
	void listGroupsPodsByModel() throws Exception {
		sample("ready");
		this.api.inferenceServices.add(Fixtures.json("created-stopped-isvc.json"));

		List<ServedModelStatus> models = this.api.service(this.dir, Instant.parse("2026-10-26T23:56:00Z")).list();

		assertEquals(List.of(SAMPLE, "semoss-verify-pending"),
				models.stream().map(ServedModelStatus::getServedModel).toList());
		assertEquals(ServedModelState.READY, models.get(0).getState());
		assertEquals(ServedModelState.STOPPED, models.get(1).getState());
		assertEquals(3, this.api.requests.size(), "one call each for the models, all their pods and the Services");
	}

	@Test
	void waitingModelNamesTheGpuHolder() throws Exception {
		sample("pending-gpu");
		this.api.pods.addAll(Fixtures.items("ready-pods.json"));

		ServedModelStatus status = this.api.service(this.dir, Instant.parse("2026-10-27T00:00:48Z"))
				.status("semoss-verify-pending");

		assertEquals(ServedModelState.WAITING_FOR_GPU, status.getState());
		assertEquals("Waiting for the GPU, which olmo-2-1b-instruct is using", status.getMessage());
	}

	@Test
	void crashReadsThePreviousInstancesLog() throws Exception {
		sample("starting-loading");
		JsonObject main = ServedModelStateResolver.containerStatus(this.api.pods.get(0), "containerStatuses",
				ServedModelService.MAIN_CONTAINER);
		main.add("state", JsonParser.parseString("{\"waiting\":{\"reason\":\"CrashLoopBackOff\"}}"));
		main.add("lastState", JsonParser.parseString("{\"terminated\":{\"exitCode\":1,\"reason\":\"Error\"}}"));
		main.addProperty("restartCount", 1);
		this.api.log = "2026-10-26T23:58:10Z (EngineCore_DP0 pid=129) RuntimeError: Engine core initialization failed.\n";

		ServedModelStatus status = this.api.service(this.dir, Instant.parse("2026-10-26T23:59:00Z")).status(SAMPLE);

		assertEquals(ServedModelState.FAILED, status.getState());
		assertTrue(status.getMessage().endsWith("RuntimeError: Engine core initialization failed."),
				status.getMessage());
		Map<String, String> query = this.api.requests("GET", "/api/v1/namespaces/semoss/pods/").get(0).query();
		assertEquals("true", query.get("previous"));
		assertEquals("60", query.get("tailLines"));
	}

	@Test
	void stopRecordsTheStartForTheNextEta() throws Exception {
		sample("restarted-ready");
		this.api.log = Fixtures.text("vllm-startup.log");

		this.api.service(this.dir, Instant.parse("2026-10-27T00:10:00Z")).stop(SAMPLE, null);

		JsonObject annotations = KubeJson.obj(
				JsonParser.parseString(this.api.requests("PATCH", ISVC_PATH).get(0).body()).getAsJsonObject(),
				"metadata", "annotations");
		assertEquals(Map.of(ServedModelPhase.CREATING, 0, ServedModelPhase.DOWNLOADING_WEIGHTS, 38,
				ServedModelPhase.LOADING_WEIGHTS, 31, ServedModelPhase.COMPILING, 13, ServedModelPhase.WARMING, 28,
				ServedModelPhase.FINISHING, 7), ServedModelStateResolver.parseStartStats(
						KubeJson.str(annotations, ServedModelStateResolver.START_STATS_ANNOTATION)));
	}

	@Test
	void statusCarriesTheEndpointForThisSemoss() throws Exception {
		sample("ready");
		this.api.services.addAll(Fixtures.items("services-running.json"));
		Instant now = Instant.parse("2026-10-26T23:56:00Z");

		assertEquals("http://localhost:8000/v1", this.api.service(this.dir, now).status(SAMPLE).getEndpoint());
		ServedModelService inCluster = this.api.service(this.dir, now,
				new ServedModelEndpoints(ServedModelEndpoints.Mode.CLUSTER, "localhost"));
		assertEquals("http://olmo-2-1b-instruct-predictor.semoss.svc.cluster.local/v1",
				inCluster.status(SAMPLE).getEndpoint());
		assertEquals("http://localhost:8000/v1", this.api.service(this.dir, now).list().get(0).getEndpoint());
	}

	@Test
	void forbiddenServicesOnlyCostTheEndpoint() throws Exception {
		sample("ready");
		this.api.servicesForbidden = "services is forbidden: User \"system:serviceaccount:semoss:semoss\" cannot list";

		ServedModelStatus status = this.api.service(this.dir, Instant.parse("2026-10-26T23:56:00Z")).status(SAMPLE);

		assertEquals(ServedModelState.READY, status.getState());
		assertNull(status.getEndpoint());
		assertTrue(String.valueOf(status.toSummaryMap().get("endpointNote")).startsWith("Cannot list Services"));
	}

	private static final String OLMO_ENTRY = "allenai/OLMo-2-0425-1B-Instruct";
	private static final String SERVICES_PATH = "/api/v1/namespaces/semoss/services";

	@Test
	void defineCreatesTheModelStoppedWithItsOwnService() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-27T02:00:00Z"));

		ServedModelStatus status = service.define("olmo-2-1b", OLMO_ENTRY, Map.of("maxModelLen", 2048.0), false, null);

		assertEquals(ServedModelState.STOPPED, status.getState());
		assertTrue(status.isManaged());
		assertEquals("OLMo 2 1B Instruct", status.getDisplayName(), "for its engine's name");
		assertEquals(OLMO_ENTRY, status.getCatalogEntry(), "for its engine's limits");
		assertEquals("http://10.43.0.90/v1", status.getEndpoint(), "the owned Service's ClusterIP, from the host");
		JsonObject isvc = JsonParser.parseString(this.api.requests("POST", ISVC_PATH).get(0).body()).getAsJsonObject();
		assertEquals("true", KubeJson.str(isvc, "metadata", "annotations", ServedModelStateResolver.STOP_ANNOTATION));
		assertEquals(List.of("--gpu-memory-utilization=0.2", "--max-model-len=2048"),
				KubeJson.strings(isvc, "spec", "predictor", "model", "args"));
		JsonObject endpoint = JsonParser.parseString(this.api.requests("POST", SERVICES_PATH).get(0).body())
				.getAsJsonObject();
		assertEquals("olmo-2-1b-endpoint", KubeJson.str(endpoint, "metadata", "name"));
		assertEquals("uid-1", KubeJson.str(KubeJson.objects(endpoint, "metadata", "ownerReferences").get(0), "uid"),
				"owned by the InferenceService as created");
	}

	@Test
	void defineRefusesWhatIsNotApprovedOrAlreadyThere() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		sample("ready");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-27T02:00:00Z"));

		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> service.define("unapproved", "example/unapproved-model", null, false, null)).getMessage()
				.contains("is not in the serving catalog"));
		assertEquals("A served model named olmo-2-1b-instruct already exists in namespace semoss",
				assertThrows(IllegalArgumentException.class,
						() -> service.define(SAMPLE, OLMO_ENTRY, null, false, null)).getMessage());
		assertTrue(this.api.requests("POST", SERVICES_PATH).isEmpty());
	}

	@Test
	void defineUndoesTheModelWhenItsServiceCannotBeCreated() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		this.api.serviceCreateForbidden = "services is forbidden: cannot create";
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-27T02:00:00Z"));

		prerna.util.k8s.KubeApiException error = assertThrows(prerna.util.k8s.KubeApiException.class,
				() -> service.define("olmo-2-1b", OLMO_ENTRY, null, false, null));

		assertTrue(error.getMessage().contains("so olmo-2-1b was not defined"), error.getMessage());
		assertEquals(1, this.api.requests("DELETE", ISVC_PATH + "/olmo-2-1b").size());
		assertTrue(this.api.inferenceServices.isEmpty());
	}

	@Test
	void removeTakesOnlyWhatSemossDefined() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		sample("ready");
		ServedModelService service = this.api.service(this.dir, Instant.parse("2026-10-27T02:00:00Z"));
		service.define("olmo-2-1b", OLMO_ENTRY, null, false, null);

		ServedModelRemoval removal = service.remove("olmo-2-1b", null);

		assertTrue(removal.removed());
		assertEquals("REMOVED", removal.toMap().get("state"));
		assertEquals("Background",
				this.api.requests("DELETE", ISVC_PATH + "/olmo-2-1b").get(0).query().get("propagationPolicy"));

		assertTrue(assertThrows(IllegalArgumentException.class, () -> service.remove(SAMPLE, null)).getMessage()
				.contains("was not defined by SEMOSS"));
		assertTrue(this.api.requests("DELETE", ISVC_PATH + "/" + SAMPLE).isEmpty());
	}

	@Test
	void missingCatalogIsReportedNotThrown() throws Exception {
		ServingCatalog catalog = this.api.service(this.dir, Instant.parse("2026-10-27T02:00:00Z")).catalog();
		assertTrue(catalog.getEntries().isEmpty());
		assertEquals(List.of("There is no serving catalog: ConfigMap semoss/semoss-serving-catalog does not exist"),
				catalog.getProblems());
	}

	private static final Instant LATER = Instant.parse("2026-10-27T17:20:00Z");
	private static final String HOLDER_BUSY = "The GPU is in use by olmo-2-1b-instruct (READY since 2026-10-26 22:55 UTC).";

	/** Another model like the sample, stopped */
	private static JsonObject stoppedCopy(String name) {
		JsonObject isvc = Fixtures.json("ready-isvc.json");
		isvc.getAsJsonObject("metadata").addProperty("name", name);
		isvc.getAsJsonObject("metadata").getAsJsonObject("annotations")
				.addProperty(ServedModelStateResolver.STOP_ANNOTATION, "true");
		return isvc;
	}

	/** Which model each annotation patch went to, and the stop value it set */
	private List<String> stopPatches() {
		return this.api.requests("PATCH", ISVC_PATH).stream()
				.map(request -> request.path().substring(ISVC_PATH.length() + 1) + " stop="
						+ KubeJson.str(JsonParser.parseString(request.body()).getAsJsonObject(), "metadata", "annotations",
								ServedModelStateResolver.STOP_ANNOTATION))
				.toList();
	}

	@Test
	void startIsRefusedWhileAnotherModelHoldsTheGpu() throws Exception {
		sample("ready");
		this.api.inferenceServices.add(stoppedCopy("olmo-2-1b"));
		ServedModelService service = this.api.service(this.dir, LATER);

		ServingCapacityException error = assertThrows(ServingCapacityException.class,
				() -> service.start("olmo-2-1b", null));

		assertEquals(HOLDER_BUSY + " Stop it first, or start olmo-2-1b with replace=[true].", error.getMessage());
		assertTrue(stopPatches().isEmpty(), "nothing changed");
	}

	@Test
	void replaceStopsTheHolderWaitsForItsPodsThenStarts() throws Exception {
		sample("ready");
		this.api.inferenceServices.add(stoppedCopy("olmo-2-1b"));
		this.api.stopTakesPodLists = 2;
		ServedModelService service = this.api.service(this.dir, LATER);
		java.util.concurrent.atomic.AtomicInteger sleeps = new java.util.concurrent.atomic.AtomicInteger();
		service.setSleeper(millis -> sleeps.incrementAndGet());

		ServedModelChange change = service.start("olmo-2-1b", null, true);

		assertTrue(change.changed());
		assertEquals(List.of(SAMPLE), change.replaced());
		assertEquals(List.of(SAMPLE + " stop=true", "olmo-2-1b stop=false"), stopPatches());
		assertTrue(sleeps.get() >= 1, "it waited for the sample's pods to go");
		assertTrue(this.api.pods.isEmpty());
	}

	@Test
	void aFailedModelIsStoppedAndStartedAgain() throws Exception {
		sample("ready");
		JsonObject main = KubeJson.objects(this.api.pods.get(0), "status", "containerStatuses").get(0);
		main.addProperty("ready", false);
		main.add("state", JsonParser.parseString("{\"terminated\": {\"exitCode\": 1, \"reason\": \"Error\"}}"));
		this.api.stopTakesPodLists = 0;
		ServedModelService service = this.api.service(this.dir, LATER);
		service.setSleeper(millis -> {
		});

		ServedModelChange change = service.start(SAMPLE, null);

		assertTrue(change.restarted());
		assertTrue(change.changed());
		assertEquals(List.of(SAMPLE + " stop=true", SAMPLE + " stop=false"), stopPatches());
	}

	@Test
	void aModelWaitingForTheGpuCanReplaceWhatHoldsIt() throws Exception {
		sample("ready");
		this.api.inferenceServices.add(Fixtures.json("pending-gpu-isvc.json"));
		this.api.pods.addAll(Fixtures.items("pending-gpu-pods.json"));
		this.api.events.addAll(Fixtures.items("pending-gpu-events.json"));
		ServedModelService service = this.api.service(this.dir, LATER);

		ServedModelChange waiting = service.start("semoss-verify-pending", null);
		assertEquals(ServedModelState.WAITING_FOR_GPU, waiting.status().getState());
		assertFalse(waiting.changed());
		assertTrue(stopPatches().isEmpty());

		ServedModelChange replaced = service.start("semoss-verify-pending", null, true);

		assertFalse(replaced.changed(), "the waiting model itself is unchanged");
		assertEquals(List.of(SAMPLE), replaced.replaced());
		assertEquals(List.of(SAMPLE + " stop=true"), stopPatches());
	}

	@Test
	void defineWithStartStaysStoppedWhileTheGpuIsBusy() throws Exception {
		sample("ready");
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		ServedModelService service = this.api.service(this.dir, LATER);

		ServedModelStatus status = service.define("olmo-2-1b", OLMO_ENTRY, null, true, null);

		assertEquals(ServedModelState.STOPPED, status.getState());
		assertEquals(List.of("olmo-2-1b is defined but was not started: " + HOLDER_BUSY
				+ " Stop it first, or start olmo-2-1b with replace=[true]."), status.getWarnings());
		JsonObject created = JsonParser.parseString(this.api.requests("POST", ISVC_PATH).get(0).body()).getAsJsonObject();
		assertEquals("true", KubeJson.str(created, "metadata", "annotations", ServedModelStateResolver.STOP_ANNOTATION));
		assertTrue(stopPatches().isEmpty());
	}

	@Test
	void defineWithStartStartsItWhenTheGpuIsFree() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		ServedModelService service = this.api.service(this.dir, LATER);

		ServedModelStatus status = service.define("olmo-2-1b", OLMO_ENTRY, null, true, null);

		assertFalse(status.getState() == ServedModelState.STOPPED, status.getState().name());
		assertEquals(List.of("olmo-2-1b stop=false"), stopPatches());
	}

	@Test
	void withoutRightsOnNodesTheGpuCheckIsSkippedWithAWarning() throws Exception {
		sample("stopped");
		this.api.nodesForbidden = "nodes is forbidden: User \"system:serviceaccount:semoss:semoss\" cannot list resource "
				+ "\"nodes\" in API group \"\" at the cluster scope";
		ServedModelService service = this.api.service(this.dir, LATER);

		assertEquals(List.of("SEMOSS may not list the cluster's nodes (HTTP 403), so it does not count GPUs before a "
				+ "start; apply semoss-rbac.yml"), service.capacity().getWarnings());
		assertTrue(service.start(SAMPLE, null).changed());
	}

	@Test
	void aModelWithoutItsOwnFractionCountsItsRuntimes() throws Exception {
		JsonObject isvc = stoppedCopy("olmo-2-1b");
		isvc.getAsJsonObject("spec").getAsJsonObject("predictor").getAsJsonObject("model").add("args",
				new com.google.gson.JsonArray());
		this.api.inferenceServices.add(isvc);
		ServedModelService service = this.api.service(this.dir, LATER);

		Map<?, ?> unknown = models(service.capacity().toMap()).get(0);
		assertEquals(0.9, unknown.get("gpuMemoryUtilization"));
		assertTrue(service.capacity().getWarnings().get(0).startsWith(
				"SEMOSS cannot read the ClusterServingRuntime nvidia-vllm (HTTP 404)"), service.capacity().getWarnings().toString());

		this.api.clusterRuntimes.put("nvidia-vllm", JsonParser.parseString("""
				{"spec": {"containers": [{"name": "kserve-container",
				  "args": ["--port=8080", "--gpu-memory-utilization=0.5"]}]}}""").getAsJsonObject());
		Map<?, ?> fromRuntime = models(service.capacity().toMap()).get(0);
		assertEquals(0.5, fromRuntime.get("gpuMemoryUtilization"));
		assertEquals("runtime nvidia-vllm", fromRuntime.get("fractionSource"));
	}

	@Test
	void eventsOfAnEarlierModelWithTheSameNameStayOut() throws Exception {
		sample("stopped");
		this.api.events.add(JsonParser.parseString("""
				{"involvedObject": {"kind": "InferenceService", "name": "olmo-2-1b-instruct", "uid": "an-earlier-uid"},
				 "type": "Warning", "reason": "InferenceServiceNotReady", "message": "from the earlier model",
				 "lastTimestamp": "2026-10-26T22:00:00Z", "count": 1}""").getAsJsonObject());

		List<?> events = (List<?>) this.api.service(this.dir, LATER).status(SAMPLE).toDetailMap().get("events");

		assertFalse(events.isEmpty(), "its own events are there");
		assertFalse(events.stream().anyMatch(event -> "from the earlier model".equals(((Map<?, ?>) event).get("message"))),
				events.toString());
	}

	@SuppressWarnings("unchecked")
	private static List<Map<?, ?>> models(Map<String, Object> capacity) {
		return (List<Map<?, ?>>) capacity.get("models");
	}

	@Test
	void publishingTakesAFreePortAndWaitsForItsAddress() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		this.api.services.addAll(Fixtures.items("services-running.json"));
		ServedModelService service = this.api.service(this.dir, LATER);

		ServedModelStatus status = service.define("olmo-2-1b", OLMO_ENTRY, null, false, true, null, null);

		JsonObject created = JsonParser.parseString(this.api.requests("POST", SERVICES_PATH).get(0).body())
				.getAsJsonObject();
		assertEquals("LoadBalancer", KubeJson.str(created, "spec", "type"));
		assertEquals(8001, KubeJson.integer(KubeJson.objects(created, "spec", "ports").get(0), "port"));
		assertEquals(8001, status.getHostPort());
		assertEquals("http://localhost:8001/v1", status.getEndpoint());
		assertTrue(status.getWarnings().isEmpty(), status.getWarnings().toString());
	}

	@Test
	void aPortTheNodeCannotBindIsReported() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		this.api.loadBalancerUnbound = true;
		ServedModelService service = this.api.service(this.dir, LATER);
		java.util.concurrent.atomic.AtomicInteger sleeps = new java.util.concurrent.atomic.AtomicInteger();
		service.setSleeper(millis -> sleeps.incrementAndGet());

		ServedModelStatus status = service.define("olmo-2-1b", OLMO_ENTRY, null, false, true, 8007, null);

		assertEquals(30, sleeps.get());
		String unbound = "Port 8007 got no address from the node's load balancer within 30 s; something on the "
				+ "node may already use it. Remove olmo-2-1b and define it again with another publishPort.";
		assertEquals(List.of(unbound), status.getWarnings());

		// started at once, the model keeps the warning
		this.api.inferenceServices.clear();
		this.api.services.clear();
		ServedModelStatus started = service.define("olmo-2-1b", OLMO_ENTRY, null, true, true, 8007, null);
		assertFalse(started.getState() == ServedModelState.STOPPED, started.getState().name());
		assertEquals(List.of(unbound), started.getWarnings());
	}

	@Test
	void aPortThatCannotBeHadLeavesNothingBehind() throws Exception {
		this.api.catalog = Fixtures.text("serving-catalog.yaml");
		this.api.services.addAll(Fixtures.items("services-running.json"));
		ServedModelService service = this.api.service(this.dir, LATER);

		assertEquals("Port 8000 is already taken by a LoadBalancer Service", assertThrows(IllegalArgumentException.class,
				() -> service.define("olmo-2-1b", OLMO_ENTRY, null, false, true, 8000, null)).getMessage());

		this.api.servicesForbidden = "services is forbidden";
		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> service.define("olmo-2-1b", OLMO_ENTRY, null, false, true, null, null)).getMessage()
				.startsWith("SEMOSS cannot list the Services of semoss, so it cannot pick a free port to publish olmo-2-1b"));
		assertTrue(this.api.requests("POST", ISVC_PATH).isEmpty(), "no model was created");
	}

	private static final Instant JUST_READY = Instant.parse("2026-10-26T22:55:30Z");

	@Test
	void aModelIsReadyOnceItsEndpointAnswers() throws Exception {
		sample("ready");
		this.api.services.addAll(Fixtures.items("services-running.json"));
		ServedModelService service = this.api.service(this.dir, JUST_READY);
		service.setProbe(endpoint -> "ConnectException");

		ServedModelStatus routing = service.status(SAMPLE);

		assertEquals(ServedModelState.STARTING, routing.getState(), "9 s after Ready, the Service may not route yet");
		assertEquals(ServedModelPhase.FINISHING, routing.getPhase());
		assertEquals("Waiting for http://localhost:8000/v1 to reach the model", routing.getMessage());
		assertEquals(0.99, routing.getProgress());

		service.setProbe(endpoint -> null);
		assertEquals(ServedModelState.READY, service.status(SAMPLE).getState());
	}

	@Test
	void anEndpointStillUnreachableLongAfterReadyIsAWarning() throws Exception {
		sample("ready");
		this.api.services.addAll(Fixtures.items("services-running.json"));
		ServedModelService service = this.api.service(this.dir, LATER);
		service.setProbe(endpoint -> "ConnectException");

		ServedModelStatus status = service.status(SAMPLE);

		assertEquals(ServedModelState.READY, status.getState());
		assertTrue(status.getWarnings()
				.contains("SEMOSS cannot reach the model at http://localhost:8000/v1: ConnectException"),
				status.getWarnings().toString());
	}

	@Test
	void aListProbesOnlyModelsThatJustTurnedReady() throws Exception {
		sample("ready");
		this.api.services.addAll(Fixtures.items("services-running.json"));
		java.util.concurrent.atomic.AtomicInteger probes = new java.util.concurrent.atomic.AtomicInteger();

		ServedModelService later = this.api.service(this.dir, LATER);
		later.setProbe(endpoint -> {
			probes.incrementAndGet();
			return "ConnectException";
		});
		assertEquals(ServedModelState.READY, later.list().get(0).getState());
		assertEquals(0, probes.get(), "ready for an hour: not probed by a list");

		ServedModelService justReady = this.api.service(this.dir, JUST_READY);
		justReady.setProbe(endpoint -> {
			probes.incrementAndGet();
			return "ConnectException";
		});
		assertEquals(ServedModelState.STARTING, justReady.list().get(0).getState());
		assertEquals(1, probes.get());
	}
}
