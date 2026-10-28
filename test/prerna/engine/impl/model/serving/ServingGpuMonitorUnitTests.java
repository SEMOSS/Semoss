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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static prerna.engine.impl.model.serving.GpuFixtures.IDLE_GPU;
import static prerna.engine.impl.model.serving.GpuFixtures.OLMO_POD;
import static prerna.engine.impl.model.serving.GpuFixtures.metric;
import static prerna.engine.impl.model.serving.GpuFixtures.sample;
import static prerna.engine.impl.model.serving.GpuFixtures.vector;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.engine.impl.model.serving.ServingGpuStatus.Window;
import prerna.engine.impl.model.serving.ServingGpuStatus.Workload;

class ServingGpuMonitorUnitTests {

	private static final String SAMPLE = "olmo-2-1b-instruct";
	private static final long NOW = 1_793_059_181L;
	private static final String PODS_PATH = "/api/v1/namespaces/semoss/pods";

	@TempDir
	Path dir;

	private FakeKubeApi api;

	@BeforeEach
	void startApi() throws Exception {
		this.api = new FakeKubeApi();
		this.api.nodes.clear();
		this.api.nodes.add(GpuFixtures.sparkNode());
	}

	@AfterEach
	void stopApi() {
		this.api.close();
	}

	private ServingGpuMonitor monitor(ServingPrometheus prometheus, String grafanaProblem) throws IOException {
		return new ServingGpuMonitor(this.api.client(this.dir), "semoss", prometheus, null, grafanaProblem);
	}

	private static ServingGpuStatus read(ServingGpuMonitor monitor) {
		return monitor.read(Window.HOUR, Instant.ofEpochSecond(NOW));
	}

	@Test
	void thePodUsingTheGpuIsNamedByItsServedModel() throws Exception {
		this.api.pods.addAll(Fixtures.items("ready-pods.json"));

		ServingGpuStatus status = read(monitor(GpuFixtures.olmoRunning(NOW).prometheus(), null));

		assertEquals(List.of(new Workload("semoss", OLMO_POD, "kserve-container", SAMPLE)),
				status.getGpus().get(0).usedBy());
		assertEquals("13.0", status.getGpus().get(0).cuda());
		assertEquals(1, this.api.requests("GET", "/api/v1/nodes").size());
		List<FakeKubeApi.Request> pods = this.api.requests("GET", PODS_PATH);
		assertEquals(1, pods.size());
		assertEquals(ServedModelStateResolver.ISVC_LABEL, pods.get(0).query().get("labelSelector"));
		assertEquals(List.of(), status.getWarnings());
	}

	@Test
	void anIdleGpuNeedsNoPodList() throws Exception {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		transport.answers.put(ServingGpuStatus.GPUS_NOW, vector(sample(metric("DCGM_FI_DEV_GPU_UTIL", IDLE_GPU), NOW, 0)));

		ServingGpuStatus status = read(monitor(transport.prometheus(), null));

		assertEquals(List.of(), status.getGpus().get(0).usedBy());
		assertEquals(List.of(), this.api.requests("GET", PODS_PATH));
	}

	@Test
	void forbiddenNodesCostOnlyTheirLabels() throws Exception {
		this.api.nodesForbidden = "nodes is forbidden: User \"system:serviceaccount:semoss:semoss\" cannot list resource \"nodes\"";

		ServingGpuStatus status = read(monitor(GpuFixtures.olmoRunning(NOW).prometheus(), null));

		ServingGpuStatus.Gpu gpu = status.getGpus().get(0);
		assertEquals("NVIDIA GB10", gpu.product(), "DCGM still names it");
		assertEquals(2.0, gpu.now().get(ServingGpuStatus.GpuMetric.UTILIZATION));
		assertNull(gpu.cuda());
		assertEquals(List.of("SEMOSS may not list the cluster's nodes (HTTP 403), so the GPUs' CUDA version and "
				+ "sharing are not shown; apply semoss-rbac.yml"), status.getWarnings());
	}

	@Test
	void withoutPrometheusTheGpuIsStillNamedAndTheCardSaysWhy() throws Exception {
		GpuFixtures.FakeTransport transport = GpuFixtures.olmoRunning(NOW);
		transport.failure = new ServingPrometheus.QueryException(
				"There is no Prometheus at monitoring/monitoring-kube-prometheus-prometheus:9090 (HTTP 404)");

		Map<String, Object> map = read(monitor(transport.prometheus(), null)).toMap();

		assertEquals("unavailable", map.get("metrics"));
		assertEquals(List.of("The GPU metrics could not be read. There is no Prometheus at "
				+ "monitoring/monitoring-kube-prometheus-prometheus:9090 (HTTP 404)"), map.get("warnings"));
		@SuppressWarnings("unchecked")
		Map<String, Object> gpu = ((List<Map<String, Object>>) map.get("gpus")).get(0);
		assertEquals("NVIDIA-GB10", gpu.get("product"), "from the node's labels");
		assertEquals(List.of(), this.api.requests("GET", PODS_PATH));
	}

	@Test
	void metricsTurnedOffAreNotAWarning() throws Exception {
		Map<String, Object> map = read(monitor(ServingPrometheus.off(), null)).toMap();

		assertEquals("off", map.get("metrics"));
		assertEquals(List.of(), map.get("warnings"));
		assertEquals(1, ((List<?>) map.get("gpus")).size());
	}

	@Test
	void aMalformedGrafanaSettingIsReportedAndIgnored() throws Exception {
		ServingGpuStatus status = read(monitor(GpuFixtures.olmoRunning(NOW).prometheus(),
				"KSERVE_GRAFANA_URL must be auto, off or an http(s) URL such as http://spark-cbde.local:3000; it is grafana"));

		assertEquals(List.of("KSERVE_GRAFANA_URL must be auto, off or an http(s) URL such as "
				+ "http://spark-cbde.local:3000; it is grafana"), status.getWarnings());
		@SuppressWarnings("unchecked")
		Map<String, Object> grafana = (Map<String, Object>) status.toMap().get("grafana");
		assertNull(grafana.get("url"));
	}

	@Test
	void theServiceReadsThroughItsMonitorAtItsOwnTime() throws Exception {
		ServedModelService service = this.api.service(this.dir, Instant.ofEpochSecond(NOW));
		service.setGpuMonitor(monitor(GpuFixtures.olmoRunning(NOW).prometheus(), null));

		Map<String, Object> map = service.gpuStatus(Window.DAY).toMap();

		assertEquals("2026-10-26T23:59:41Z", map.get("readAt"));
		assertEquals("24h", map.get("window"));
		assertTrue(((List<?>) map.get("gpus")).size() == 1);
	}
}
