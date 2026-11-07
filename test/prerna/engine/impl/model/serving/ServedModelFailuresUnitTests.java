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

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.google.gson.JsonObject;

import prerna.engine.impl.model.serving.ServedModelFailure.Cause;
import prerna.util.k8s.KubeJson;

/**
 * Real failures, recorded on the DGX Spark by kserve-failures.sh with one
 * deliberately broken InferenceService each (no GPU, nothing loaded), read the
 * way the service reads them: the objects at the time of recording, then the
 * failed container's log, if it ever ran.
 */
class ServedModelFailuresUnitTests {

	private static ServedModelStatus observe(String fixture, Instant now, String log) {
		ServedModelStatus status = ServedModelStateResolver.resolve(Fixtures.json(fixture + "-isvc.json"),
				Fixtures.items(fixture + "-pods.json"), Fixtures.items(fixture + "-events.json"), null, now);
		if (log != null) {
			ServedModelStateResolver.explainFailure(status, VllmStartupLog.lines(Fixtures.text(log), false));
		}
		return status;
	}

	@ParameterizedTest(name = "{0}")
	@CsvSource(delimiter = '|', nullValues = "-", textBlock = """
			failed-download | 2026-10-27T19:07:02Z | failed-download-storage-initializer.log | DOWNLOAD_FAILED | storage-initializer \
			| Downloading the weights from Hugging Face failed (exit code 1); Kubernetes is retrying (restart 1): HuggingFace error for hf://allenai/semoss-test-no-such-model: 401 Client Error. \
			| The Hugging Face repository is gated, private or missing; check the storageUri
			failed-image | 2026-10-27T19:07:07Z | - | IMAGE_PULL | kserve-container \
			| Cannot pull nvcr.io/nvidia/vllm:semoss-test-no-such-tag: rpc error: code = NotFound desc = failed to pull and unpack image "nvcr.io/nvidia/vllm:semoss-test-no-such-tag": failed to resolve reference "nvcr.io/nvidia/vllm:semoss-test-no-such-tag": nvcr.io/nvidia/vllm:semoss-test-no-such-tag: not found \
			| The image or its tag does not exist; check the image name and tag
			failed-config | 2026-10-27T19:07:12Z | - | CONFIG | kserve-container \
			| Kubernetes cannot create the kserve-container container: secret "semoss-test-no-such-secret" not found \
			| Create the Secret it names, or remove the reference from the model
			failed-crash | 2026-10-27T19:07:19Z | failed-crash-kserve-container.log | MODEL_CRASH | kserve-container \
			| The model server exited with code 3; Kubernetes is retrying (restart 1): ERROR semoss test: this model server fails on purpose \
			| -
			failed-unschedulable | 2026-10-27T19:07:21Z | - | UNSCHEDULABLE | - \
			| Kubernetes cannot schedule the model server: 0/1 nodes are available: 1 Insufficient cpu. \
			| The model asks for more CPU or memory than the node has free; lower its cpu or memory, or stop what else uses them
			""")
	void aRecordedFailureIsNamedAndExplained(String fixture, String at, String log, Cause cause, String container,
			String message, String hint) {
		ServedModelStatus status = observe(fixture, Instant.parse(at), log);

		assertEquals(ServedModelState.FAILED, status.getState());
		ServedModelFailure failure = status.getFailure();
		assertEquals(cause, failure.getCause());
		assertEquals(container, failure.getContainer());
		assertEquals(message, failure.getMessage());
		assertEquals(message, status.getMessage());
		assertEquals(hint, failure.getHint());
		assertEquals(List.of(), status.getWarnings(), "a failure, not a stall");
	}

	/** A recorded pod's main container, or its PodScheduled condition, to put other words in */
	private static List<JsonObject> pods(String fixture) {
		return Fixtures.items(fixture + "-pods.json");
	}

	private static String hint(String fixture, List<JsonObject> pods) {
		return ServedModelStateResolver.resolve(Fixtures.json(fixture + "-isvc.json"), pods,
				Fixtures.items(fixture + "-events.json"), null, Instant.parse("2026-10-27T19:07:30Z")).getFailure()
				.getHint();
	}

	private static void waitingMessage(List<JsonObject> pods, String message) {
		KubeJson.obj(KubeJson.objects(pods.get(0), "status", "containerStatuses").get(0), "state", "waiting")
				.addProperty("message", message);
	}

	private static void schedulerMessage(List<JsonObject> pods, String message) {
		for (JsonObject condition : KubeJson.objects(pods.get(0), "status", "conditions")) {
			if ("PodScheduled".equals(KubeJson.str(condition, "type"))) {
				condition.addProperty("message", message);
			}
		}
	}

	@Test
	void hintsFollowTheWordsOfTheFailure() {
		List<JsonObject> refused = pods("failed-image");
		waitingMessage(refused, "failed to pull and unpack image \"nvcr.io/nvidia/vllm:x\": 401 Unauthorized");
		assertEquals("The registry refused the pull; check the image pull secret (for nvcr.io, the NGC API key)",
				hint("failed-image", refused));

		List<JsonObject> configMap = pods("failed-config");
		waitingMessage(configMap, "configmap \"semoss-test-settings\" not found");
		assertEquals("Create the ConfigMap it names, or remove the reference from the model",
				hint("failed-config", configMap));

		List<JsonObject> affinity = pods("failed-unschedulable");
		schedulerMessage(affinity, "0/1 nodes are available: 1 node(s) didn't match Pod's node affinity/selector. "
				+ "preemption: 0/1 nodes are available: 1 Preemption is not helpful for scheduling.");
		assertEquals("No node matches the model's node selector or affinity", hint("failed-unschedulable", affinity));

		List<JsonObject> taint = pods("failed-unschedulable");
		schedulerMessage(taint, "0/1 nodes are available: 1 node(s) had untolerated taint "
				+ "{node.kubernetes.io/disk-pressure: }. preemption: 0/1 nodes are available.");
		assertEquals("The node has a taint the model does not tolerate", hint("failed-unschedulable", taint));
	}

	@Test
	void theSchedulersPreemptionReportIsLeftOut() {
		assertEquals("0/1 nodes are available: 1 Insufficient memory.", ServedModelStateResolver.schedulerReason(
				"0/1 nodes are available: 1 Insufficient memory. preemption: 0/1 nodes are available: 1 No preemption victims found."));
		assertEquals("no summary to cut", ServedModelStateResolver.schedulerReason("no summary to cut"));
	}
}
