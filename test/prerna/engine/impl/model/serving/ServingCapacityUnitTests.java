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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.util.k8s.KubeJson;

class ServingCapacityUnitTests {

	private static final String SAMPLE = "olmo-2-1b-instruct";
	private static final String OTHER = "olmo-2-1b";
	private static final Instant NOW = Instant.parse("2026-10-27T17:20:00Z");
	private static final List<ServingCapacity.Node> ONE_GPU = List.of(new ServingCapacity.Node("spark-cbde", 1, true));
	private static final List<ServingCapacity.Node> TWO_GPUS = List.of(new ServingCapacity.Node("spark-cbde", 2, true));

	/** The sample, READY since 2026-10-26 22:55 UTC: 1 GPU, --gpu-memory-utilization=0.2 */
	private static JsonObject sample() {
		return Fixtures.json("ready-isvc.json");
	}

	private static List<JsonObject> samplePods() {
		return Fixtures.items("ready-pods.json");
	}

	/** A stopped copy of the sample under another name; fraction null leaves it to the runtime */
	private static JsonObject stopped(String name, Double fraction) {
		JsonObject isvc = sample();
		isvc.getAsJsonObject("metadata").addProperty("name", name);
		isvc.getAsJsonObject("metadata").getAsJsonObject("annotations")
				.addProperty(ServedModelStateResolver.STOP_ANNOTATION, "true");
		JsonArray args = new JsonArray();
		if (fraction != null) {
			args.add("--gpu-memory-utilization=" + fraction);
		}
		isvc.getAsJsonObject("spec").getAsJsonObject("predictor").getAsJsonObject("model").add("args", args);
		return isvc;
	}

	private static ServingCapacity capacity(List<ServingCapacity.Node> nodes, List<JsonObject> services,
			List<JsonObject> pods, double budget) {
		return new ServingCapacity("semoss", nodes, services, pods, Map.of(), budget, NOW, List.of());
	}

	@Test
	void aFreeGpuAllowsTheStart() {
		ServingCapacity capacity = capacity(ONE_GPU, List.of(stopped(OTHER, 0.2)), List.of(), 0.7);

		ServingCapacity.Decision decision = capacity.check(OTHER);

		assertTrue(decision.allowed(), decision.reason());
		Map<?, ?> gpus = (Map<?, ?>) capacity.toMap().get("gpus");
		assertEquals(1, gpus.get("offered"));
		assertEquals(1, gpus.get("free"));
	}

	@Test
	void aBusyGpuIsRefusedNamingItsHolder() {
		ServingCapacity capacity = capacity(ONE_GPU, List.of(sample(), stopped(OTHER, 0.2)), samplePods(), 0.7);

		ServingCapacity.Decision decision = capacity.check(OTHER);

		assertFalse(decision.allowed());
		assertEquals("The GPU is in use by olmo-2-1b-instruct (READY since 2026-10-26 22:55 UTC). "
				+ "Stop it first, or start olmo-2-1b with replace=[true].", decision.reason());
		assertEquals(List.of(SAMPLE), decision.blocking());
		assertFalse(decision.waitable());
		Map<?, ?> other = models(capacity).get(1);
		assertEquals(false, other.get("canStart"));
		assertEquals(decision.reason(), other.get("reason"));
	}

	@Test
	void holdersShowWhoStartedThem() {
		JsonObject sample = sample();
		sample.getAsJsonObject("metadata").getAsJsonObject("annotations").addProperty(
				ServedModelStateResolver.LAST_ACTION_ANNOTATION,
				"{\"action\":\"start\",\"user\":\"ada\",\"userName\":\"Ada Admin\",\"at\":\"2026-10-27T17:00:00Z\"}");

		String reason = capacity(ONE_GPU, List.of(sample, stopped(OTHER, 0.2)), samplePods(), 0.7).check(OTHER).reason();

		assertTrue(reason.startsWith("The GPU is in use by olmo-2-1b-instruct (READY since 2026-10-26 22:55 UTC, "
				+ "started by Ada Admin)."), reason);
	}

	@Test
	void aStoppingHolderIsWaitedFor() {
		JsonObject sample = sample();
		sample.getAsJsonObject("metadata").getAsJsonObject("annotations")
				.addProperty(ServedModelStateResolver.STOP_ANNOTATION, "true");
		List<JsonObject> pods = samplePods();
		pods.get(0).getAsJsonObject("metadata").addProperty("deletionTimestamp", "2026-10-27T17:19:58Z");

		ServingCapacity.Decision decision = capacity(ONE_GPU, List.of(sample, stopped(OTHER, 0.2)), pods, 0.7)
				.check(OTHER);

		assertFalse(decision.allowed());
		assertTrue(decision.waitable());
		assertEquals("The GPU is in use by olmo-2-1b-instruct (stopping). olmo-2-1b needs 0.2 of the unified memory, "
				+ "and olmo-2-1b-instruct (0.2, stopping) already uses 0.2 of the 0.3 all models may use "
				+ "(KSERVE_MEMORY_BUDGET). It is stopping; start olmo-2-1b again in a moment.",
				capacity(ONE_GPU, List.of(sample, stopped(OTHER, 0.2)), pods, 0.3).check(OTHER).reason());
	}

	@Test
	void aPodSemossDoesNotManageCannotBeReplaced() {
		JsonObject pod = JsonParser.parseString("""
				{"metadata": {"name": "cuda-test"}, "status": {"phase": "Running"},
				 "spec": {"containers": [{"name": "main", "resources": {"limits": {"nvidia.com/gpu": "1"}}}]}}
				""").getAsJsonObject();

		ServingCapacity.Decision decision = capacity(ONE_GPU, List.of(stopped(OTHER, 0.2)), List.of(pod), 0.7)
				.check(OTHER);

		assertFalse(decision.allowed());
		assertEquals(List.of("cuda-test"), decision.unmanaged());
		assertTrue(decision.blocking().isEmpty());
		assertFalse(decision.waitable());
		assertEquals("The GPU is in use by pod cuda-test. SEMOSS does not manage pod cuda-test; free it with kubectl.",
				decision.reason());
	}

	@Test
	void withoutTheNodesTheGpuCheckIsSkipped() {
		ServingCapacity capacity = new ServingCapacity("semoss", null, List.of(sample(), stopped(OTHER, 0.2)),
				samplePods(), Map.of(), 0.7, NOW, List.of("SEMOSS may not list the cluster's nodes"));

		assertTrue(capacity.check(OTHER).allowed());
		assertEquals(List.of("SEMOSS may not list the cluster's nodes"), capacity.toMap().get("warnings"));
		assertFalse(((Map<?, ?>) capacity.toMap().get("gpus")).containsKey("offered"));
	}

	@Test
	void aCordonedNodeOffersNoGpu() {
		ServingCapacity.Decision decision = capacity(List.of(new ServingCapacity.Node("spark-cbde", 1, false)),
				List.of(stopped(OTHER, 0.2)), List.of(), 0.7).check(OTHER);

		assertFalse(decision.allowed());
		assertTrue(decision.reason().startsWith("olmo-2-1b needs 1 GPU, and the schedulable nodes offer none"),
				decision.reason());
	}

	@Test
	void theMemoryBudgetCountsTheModelsThatRun() {
		ServingCapacity.Decision decision = capacity(TWO_GPUS, List.of(sample(), stopped(OTHER, 0.2)), samplePods(),
				0.3).check(OTHER);

		assertFalse(decision.allowed());
		assertEquals("olmo-2-1b needs 0.2 of the unified memory, and olmo-2-1b-instruct (0.2) already uses 0.2 of the "
				+ "0.3 all models may use (KSERVE_MEMORY_BUDGET). Stop it first, or start olmo-2-1b with replace=[true].",
				decision.reason());
		assertTrue(capacity(TWO_GPUS, List.of(sample(), stopped(OTHER, 0.2)), samplePods(), 0.7).check(OTHER).allowed());
		assertTrue(capacity(TWO_GPUS, List.of(stopped(OTHER, 0.5)), List.of(), 0.4).check(OTHER).reason()
				.startsWith("olmo-2-1b asks for 0.5 of the unified memory, more than the 0.4 all models may use"));
	}

	@Test
	void aFractionComesFromTheModelThenItsRuntime() {
		List<JsonObject> services = List.of(stopped(OTHER, null), stopped("own", 0.25));

		Map<?, ?> fromRuntime = models(new ServingCapacity("semoss", ONE_GPU, services, List.of(),
				Map.of("nvidia-vllm", 0.5), 0.7, NOW, List.of())).get(0);
		Map<?, ?> unknownRuntime = models(capacity(ONE_GPU, services, List.of(), 0.7)).get(0);
		Map<?, ?> own = models(capacity(ONE_GPU, services, List.of(), 0.7)).get(1);

		assertEquals(0.5, fromRuntime.get("gpuMemoryUtilization"));
		assertEquals("runtime nvidia-vllm", fromRuntime.get("fractionSource"));
		assertEquals(0.9, unknownRuntime.get("gpuMemoryUtilization"));
		assertEquals("vLLM default", unknownRuntime.get("fractionSource"));
		assertEquals(0.25, own.get("gpuMemoryUtilization"));
		assertEquals("model", own.get("fractionSource"));
	}

	@Test
	void aModelsOwnPodsDoNotCountAgainstIt() {
		List<JsonObject> pods = samplePods();
		JsonObject main = KubeJson.objects(pods.get(0), "status", "containerStatuses").get(0);
		main.addProperty("ready", false);
		main.add("state", JsonParser.parseString("{\"terminated\": {\"exitCode\": 1, \"reason\": \"Error\"}}"));
		ServingCapacity capacity = capacity(ONE_GPU, List.of(sample()), pods, 0.7);

		assertEquals("FAILED", models(capacity).get(0).get("state"));
		assertTrue(capacity.check(SAMPLE).allowed(), capacity.check(SAMPLE).reason());
		assertEquals(true, models(capacity).get(0).get("canStart"));
		assertEquals(1, ((Map<?, ?>) capacity.toMap().get("gpus")).get("held"), "the failed pod still holds the GPU");
	}

	@SuppressWarnings("unchecked")
	private static List<Map<?, ?>> models(ServingCapacity capacity) {
		return new ArrayList<>((List<Map<?, ?>>) capacity.toMap().get("models"));
	}
}
