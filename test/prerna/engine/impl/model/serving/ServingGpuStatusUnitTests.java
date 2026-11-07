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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static prerna.engine.impl.model.serving.GpuFixtures.GB10_UUID;
import static prerna.engine.impl.model.serving.GpuFixtures.IDLE_GPU;
import static prerna.engine.impl.model.serving.GpuFixtures.MEMORY_AVAILABLE;
import static prerna.engine.impl.model.serving.GpuFixtures.MEMORY_TOTAL;
import static prerna.engine.impl.model.serving.GpuFixtures.NODE_EXPORTER;
import static prerna.engine.impl.model.serving.GpuFixtures.OLMO_GPU;
import static prerna.engine.impl.model.serving.GpuFixtures.OLMO_POD;
import static prerna.engine.impl.model.serving.GpuFixtures.matrix;
import static prerna.engine.impl.model.serving.GpuFixtures.metric;
import static prerna.engine.impl.model.serving.GpuFixtures.sample;
import static prerna.engine.impl.model.serving.GpuFixtures.series;
import static prerna.engine.impl.model.serving.GpuFixtures.sparkNode;
import static prerna.engine.impl.model.serving.GpuFixtures.vector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.serving.ServingGpuStatus.GpuMetric;
import prerna.engine.impl.model.serving.ServingGpuStatus.Metrics;
import prerna.engine.impl.model.serving.ServingGpuStatus.Readings;
import prerna.engine.impl.model.serving.ServingGpuStatus.Window;
import prerna.engine.impl.model.serving.ServingGpuStatus.Workload;
import prerna.engine.impl.model.serving.ServingPrometheus.Point;

class ServingGpuStatusUnitTests {

	private static final String SAMPLE = "olmo-2-1b-instruct";
	/** 2026-10-26 23:59 UTC, while the sample was READY */
	private static final long NOW = 1_793_059_181L;
	private static final Map<String, String> SERVED = Map.of(OLMO_POD, SAMPLE);

	private static ServingGpuStatus status(GpuFixtures.FakeTransport transport, String grafanaUrl) {
		Readings readings = ServingGpuStatus.read(transport.prometheus(), Window.HOUR, Instant.ofEpochSecond(NOW));
		return new ServingGpuStatus("semoss", List.of(sparkNode()), SERVED, readings, Metrics.PROMETHEUS,
				"Prometheus for tests", Window.HOUR, Instant.ofEpochSecond(NOW), grafanaUrl, List.of());
	}

	@Test
	void theRecordedSampleShowsTheGb10AndTheModelUsingIt() {
		ServingGpuStatus status = status(GpuFixtures.olmoRunning(NOW), null);

		assertEquals(1, status.getGpus().size());
		ServingGpuStatus.Gpu gpu = status.getGpus().get(0);
		assertEquals("spark-cbde", gpu.node());
		assertEquals("0", gpu.index());
		assertEquals(GB10_UUID, gpu.uuid());
		assertEquals("NVIDIA GB10", gpu.product(), "DCGM's name, not the label's NVIDIA-GB10");
		assertEquals("580.178.04", gpu.driver());
		assertEquals("13.0", gpu.cuda());
		assertEquals("blackwell", gpu.family());
		assertEquals("none", gpu.sharing());
		assertEquals(Map.of(GpuMetric.UTILIZATION, 2.0, GpuMetric.TEMPERATURE, 40.0, GpuMetric.POWER, 9.666,
				GpuMetric.SM_CLOCK, 2418.0), gpu.now());
		assertEquals(List.of(new Workload("semoss", OLMO_POD, "kserve-container", SAMPLE)), gpu.usedBy());

		assertEquals(1, status.getMemory().size());
		assertEquals("spark-cbde", status.getMemory().get(0).node(), "node-exporter's instance named by node_uname_info");
		assertEquals(MEMORY_TOTAL, status.getMemory().get(0).total());
		assertEquals(MEMORY_AVAILABLE, status.getMemory().get(0).available());
		assertEquals(List.of(), status.getWarnings());
	}

	@Test
	void aGpuNoPodUsesIsUsedByNobody() {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		transport.answers.put(ServingGpuStatus.GPUS_NOW,
				vector(sample(metric("DCGM_FI_DEV_GPU_UTIL", IDLE_GPU), NOW, 0),
						sample(metric("DCGM_FI_DEV_POWER_USAGE", IDLE_GPU), NOW, 4.2)));

		ServingGpuStatus.Gpu gpu = status(transport, null).getGpus().get(0);

		assertEquals(List.of(), gpu.usedBy());
		assertEquals(0.0, gpu.now().get(GpuMetric.UTILIZATION));
		assertNull(gpu.now().get(GpuMetric.TEMPERATURE), "a field Prometheus did not answer stays empty");
	}

	@Test
	void workloadsComeFromDcgmsPodLabelsUnderEitherName() {
		Map<String, String> honored = Map.of("pod", OLMO_POD, "namespace", "semoss", "container", "kserve-container");
		assertEquals(new Workload("semoss", OLMO_POD, "kserve-container", SAMPLE),
				ServingGpuStatus.workload(honored, "semoss", SERVED));

		// without honorLabels, Prometheus keeps DCGM's labels as exported_* and sets its own target's
		Map<String, String> exported = Map.of("pod", "nvidia-dcgm-exporter-x7k2p", "namespace", "gpu-operator",
				"container", "nvidia-dcgm-exporter", "exported_pod", "trainer-0", "exported_namespace", "research",
				"exported_container", "main");
		assertEquals(new Workload("research", "trainer-0", "main", null),
				ServingGpuStatus.workload(exported, "semoss", SERVED), "outside semoss there is no served model");

		assertNull(ServingGpuStatus.workload(Map.of("pod", "nvidia-dcgm-exporter-x7k2p", "namespace", "gpu-operator",
				"container", "nvidia-dcgm-exporter"), "semoss", SERVED));
		assertNull(ServingGpuStatus.workload(Map.of("gpu", "0"), "semoss", SERVED));
	}

	@Test
	void theHistoryJoinsAGpusSeriesAcrossPods() {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		// the sample stopped at t=120 and the GPU went idle: Prometheus has two series for one GPU
		transport.answers.put("avg_over_time(DCGM_FI_DEV_GPU_UTIL[60s])",
				matrix(series(metric("DCGM_FI_DEV_GPU_UTIL", IDLE_GPU), 120, 0, 180, 0, 240, 1),
						series(metric("DCGM_FI_DEV_GPU_UTIL", OLMO_GPU), 0, 37, 60, 88, 120, 5)));

		ServingGpuStatus.Gpu gpu = status(transport, null).getGpus().get(0);

		assertEquals(List.of(new Point(0, 37), new Point(60, 88), new Point(120, 5), new Point(180, 0),
				new Point(240, 1)), gpu.history().get(GpuMetric.UTILIZATION),
				"in time order; where both have a point, the higher one");
	}

	@Test
	void memoryInUseIsTheTotalLessWhatIsAvailable() {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		transport.answers.put("avg_over_time(node_memory_MemAvailable_bytes[60s])",
				matrix(series(metric("node_memory_MemAvailable_bytes", NODE_EXPORTER), 0, MEMORY_TOTAL - 20e9, 60,
						MEMORY_AVAILABLE)));

		ServingGpuStatus status = status(transport, null);

		assertEquals(List.of(new Point(0, 20e9), new Point(60, MEMORY_TOTAL - MEMORY_AVAILABLE)),
				status.getMemory().get(0).usedHistory());
		Map<String, Object> node = node(status.toMap());
		assertEquals("spark-cbde", node.get("node"));
		assertEquals(MEMORY_TOTAL - MEMORY_AVAILABLE, node.get("memoryUsed"));
	}

	@Test
	void withoutDcgmSamplesTheNodeLabelsStillNameTheGpu() {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		transport.answers.remove(ServingGpuStatus.GPUS_NOW);

		ServingGpuStatus status = status(transport, null);

		assertEquals(1, status.getGpus().size());
		ServingGpuStatus.Gpu gpu = status.getGpus().get(0);
		assertEquals("NVIDIA-GB10", gpu.product());
		assertEquals("13.0", gpu.cuda());
		assertEquals(Map.of(), gpu.now());
		assertEquals(List.of("Prometheus has no current GPU samples. Is the GPU Operator's DCGM exporter running? "
				+ "(kubectl -n gpu-operator get pods)"), status.getWarnings());
	}

	@Test
	void withoutPrometheusTheCardStillNamesTheGpuAndSaysWhy() {
		ServingGpuStatus status = new ServingGpuStatus("semoss", List.of(sparkNode()), Map.of(), null,
				Metrics.UNAVAILABLE, null, Window.HOUR, Instant.ofEpochSecond(NOW), null,
				List.of("The GPU metrics could not be read. There is no Prometheus at monitoring/..."));

		Map<String, Object> map = status.toMap();
		assertEquals("unavailable", map.get("metrics"));
		assertNull(map.get("source"));
		assertEquals(List.of(), map.get("nodes"));
		Map<String, Object> gpu = gpu(map);
		assertEquals("NVIDIA-GB10", gpu.get("product"));
		assertNull(gpu.get("utilization"));
		assertEquals(Map.of("utilization", List.of(), "power", List.of(), "temperature", List.of(), "smClock",
				List.of()), gpu.get("history"));
		assertEquals(List.of("The GPU metrics could not be read. There is no Prometheus at monitoring/..."),
				map.get("warnings"));

		assertEquals(List.of(), new ServingGpuStatus("semoss", null, Map.of(), null, Metrics.OFF, null, Window.HOUR,
				Instant.ofEpochSecond(NOW), null, List.of()).getGpus(), "nothing names a GPU");
	}

	@Test
	void theMapRoundsValuesAndLinksGrafana() {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		transport.answers.put("avg_over_time(DCGM_FI_DEV_POWER_USAGE[60s])",
				matrix(series(metric("DCGM_FI_DEV_POWER_USAGE", OLMO_GPU), 60, 9.66666, 120, 12.004)));

		Map<String, Object> map = status(transport, null).toMap();

		assertEquals("prometheus", map.get("metrics"));
		assertEquals("Prometheus for tests", map.get("source"));
		assertEquals("2026-10-26T23:59:41Z", map.get("readAt"));
		assertEquals("1h", map.get("window"));
		assertEquals(60L, map.get("stepSeconds"));
		Map<String, Object> gpu = gpu(map);
		assertEquals(9.67, gpu.get("power"));
		assertEquals(List.of(Map.of("namespace", "semoss", "pod", OLMO_POD, "container", "kserve-container",
				"servedModel", SAMPLE)), gpu.get("usedBy"));
		@SuppressWarnings("unchecked")
		Map<String, Object> history = (Map<String, Object>) gpu.get("history");
		assertEquals(List.of(List.of(60L, 9.67), List.of(120L, 12.0)), history.get("power"));
		assertEquals(Map.of("url", "", "port", 3000, "path", "/d/Oxed_c6Wz/nvidia-dcgm-exporter-dashboard").keySet(),
				grafana(map).keySet());
		assertNull(grafana(map).get("url"), "the browser's own address on port 3000");

		assertEquals("http://grafana.example:3000",
				grafana(status(GpuFixtures.olmoRunning(NOW), "http://grafana.example:3000").toMap()).get("url"));
		assertNull(status(GpuFixtures.olmoRunning(NOW), ServingSettings.OFF).toMap().get("grafana"));
	}

	@Test
	void eachWindowHasSixtyStepsEndingOnAStepBoundary() {
		assertEquals(Window.HOUR, Window.parse(null));
		assertEquals(Window.HOUR, Window.parse(" "));
		assertEquals(Window.DAY, Window.parse("24h"));
		assertEquals(Window.MINUTES_15, Window.parse(" 15M "));
		IllegalArgumentException bad = assertThrows(IllegalArgumentException.class, () -> Window.parse("2h"));
		assertEquals("The window must be 15m, 1h, 6h or 24h; it is 2h", bad.getMessage());

		assertEquals(Duration.ofSeconds(60), Window.HOUR.step());
		assertEquals(Duration.ofSeconds(15), Window.MINUTES_15.step());
		assertEquals(Duration.ofSeconds(60), Window.MINUTES_15.lookback(), "a 15 s step still averages a minute");
		assertEquals(Duration.ofSeconds(1440), Window.DAY.lookback());
		assertEquals(Instant.ofEpochSecond(1_793_059_140L), Window.HOUR.end(Instant.ofEpochSecond(NOW)));
	}

	@Test
	void readAsksForNowAndForTheWindowsHistory() {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		ServingGpuStatus.read(transport.prometheus(), Window.HOUR, Instant.ofEpochSecond(NOW));

		List<String> queries = transport.calls.stream().map(call -> call.parameters().get("query")).toList();
		assertEquals(List.of(ServingGpuStatus.GPUS_NOW, "avg_over_time(DCGM_FI_DEV_GPU_UTIL[60s])",
				"avg_over_time(DCGM_FI_DEV_POWER_USAGE[60s])", "avg_over_time(DCGM_FI_DEV_GPU_TEMP[60s])",
				"avg_over_time(DCGM_FI_DEV_SM_CLOCK[60s])", ServingGpuStatus.NODES_NOW,
				"avg_over_time(node_memory_MemAvailable_bytes[60s])"), queries);
		assertEquals(
				"{__name__=~\"DCGM_FI_DEV_GPU_UTIL|DCGM_FI_DEV_POWER_USAGE|DCGM_FI_DEV_GPU_TEMP|DCGM_FI_DEV_SM_CLOCK\"}",
				ServingGpuStatus.GPUS_NOW);
		assertEquals(Map.of("query", ServingGpuStatus.GPUS_NOW, "time", Long.toString(NOW)),
				transport.calls.get(0).parameters());
		assertEquals(Map.of("query", "avg_over_time(DCGM_FI_DEV_GPU_UTIL[60s])", "start", "1793055540", "end",
				"1793059140", "step", "60s"), transport.calls.get(1).parameters());
	}

	@Test
	void onlyThePodsOfTheNamespaceAreLookedUp() {
		Readings readings = ServingGpuStatus.read(GpuFixtures.olmoRunning(NOW).prometheus(), Window.HOUR,
				Instant.ofEpochSecond(NOW));
		assertEquals(Set.of(OLMO_POD), ServingGpuStatus.podsIn(readings, "semoss"));
		assertEquals(Set.of(), ServingGpuStatus.podsIn(readings, "elsewhere"));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> gpu(Map<String, Object> map) {
		return ((List<Map<String, Object>>) map.get("gpus")).get(0);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> node(Map<String, Object> map) {
		return ((List<Map<String, Object>>) map.get("nodes")).get(0);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> grafana(Map<String, Object> map) {
		return (Map<String, Object>) map.get("grafana");
	}
}
