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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Prometheus answers for the GPU card, built around label sets recorded on the
 * DGX Spark with kserve-verify.sh on 2026-10-26 (V10, V11 and V14 in its
 * report) while olmo-2-1b-instruct's predictor used the GB10. Prometheus's
 * reply envelope is its documented API format; the recorded values are kept,
 * and every invented value is marked as such.
 */
final class GpuFixtures {

	private GpuFixtures() {
	}

	static final String OLMO_POD = "olmo-2-1b-instruct-predictor-bfc8475f7-7x5nh";
	static final String GB10_UUID = "GPU-72ec881d-d090-aa20-9c5d-4edbe21369c0";

	/** DCGM's labels for the GB10 while olmo's predictor used it, as Prometheus stored them (V11) */
	static final String OLMO_GPU = """
			"DCGM_FI_DRIVER_VERSION":"580.178.04","UUID":"GPU-72ec881d-d090-aa20-9c5d-4edbe21369c0",\
			"container":"kserve-container","device":"nvidia0","endpoint":"gpu-metrics","gpu":"0",\
			"hostname":"spark-cbde","instance":"10.42.0.21:9400","job":"nvidia-dcgm-exporter",\
			"modelName":"NVIDIA GB10","namespace":"semoss","pci_bus_id":"0000000F:01:00.0",\
			"pod":"olmo-2-1b-instruct-predictor-bfc8475f7-7x5nh","service":"nvidia-dcgm-exporter\"""";

	/**
	 * The same GPU with no pod using it. Not recorded: with honorLabels,
	 * Prometheus gives such a series the exporter's own pod, namespace and
	 * container, its target labels (the pod name is invented).
	 */
	static final String IDLE_GPU = """
			"DCGM_FI_DRIVER_VERSION":"580.178.04","UUID":"GPU-72ec881d-d090-aa20-9c5d-4edbe21369c0",\
			"container":"nvidia-dcgm-exporter","device":"nvidia0","endpoint":"gpu-metrics","gpu":"0",\
			"hostname":"spark-cbde","instance":"10.42.0.21:9400","job":"nvidia-dcgm-exporter",\
			"modelName":"NVIDIA GB10","namespace":"gpu-operator","pci_bus_id":"0000000F:01:00.0",\
			"pod":"nvidia-dcgm-exporter-x7k2p","service":"nvidia-dcgm-exporter\"""";

	/** node-exporter's target labels for the box (V11) */
	static final String NODE_EXPORTER = """
			"container":"node-exporter","endpoint":"http-metrics","instance":"10.0.0.218:9100",\
			"job":"node-exporter","namespace":"monitoring","pod":"monitoring-prometheus-node-exporter-mkzp8",\
			"service":"monitoring-prometheus-node-exporter\"""";

	/** node_memory_MemAvailable_bytes with the sample running (V11) */
	static final long MEMORY_AVAILABLE = 86_777_569_280L;
	/** The node's memory capacity, 127598832Ki (V14), which is MemTotal */
	static final long MEMORY_TOTAL = 127_598_832L * 1024;

	/** {"__name__": name, labels...} */
	static String metric(String name, String labels) {
		return "{\"__name__\":\"" + name + "\"," + labels + "}";
	}

	/** An instant query's sample */
	static String sample(String metric, long time, double value) {
		return "{\"metric\":" + metric + ",\"value\":[" + time + ".781,\"" + format(value) + "\"]}";
	}

	/** A range query's series; times and values alternate */
	static String series(String metric, double... timesAndValues) {
		List<String> values = new ArrayList<>();
		for (int i = 0; i + 1 < timesAndValues.length; i += 2) {
			values.add("[" + (long) timesAndValues[i] + ",\"" + format(timesAndValues[i + 1]) + "\"]");
		}
		return "{\"metric\":" + metric + ",\"values\":[" + String.join(",", values) + "]}";
	}

	/** As Prometheus writes values: 2, 9.666, 130661203968 */
	private static String format(double value) {
		return value == Math.rint(value) && Math.abs(value) < 1e15 ? Long.toString((long) value)
				: Double.toString(value);
	}

	static String vector(String... samples) {
		return "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[" + String.join(",", samples)
				+ "]}}";
	}

	static String matrix(String... series) {
		return "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[" + String.join(",", series)
				+ "]}}";
	}

	/**
	 * The DGX Spark's node with the labels of GPU feature discovery that the card
	 * reads, as recorded (V14)
	 */
	static JsonObject sparkNode() {
		return JsonParser.parseString("""
				{"metadata": {"name": "spark-cbde", "labels": {
				   "nvidia.com/cuda.driver-version.full": "580.178.04",
				   "nvidia.com/cuda.runtime-version.full": "13.0",
				   "nvidia.com/gpu.count": "1",
				   "nvidia.com/gpu.family": "blackwell",
				   "nvidia.com/gpu.machine": "NVIDIA_DGX_Spark",
				   "nvidia.com/gpu.product": "NVIDIA-GB10",
				   "nvidia.com/gpu.sharing-strategy": "none"}},
				 "spec": {},
				 "status": {"allocatable": {"nvidia.com/gpu": "1"}, "conditions": [{"type": "Ready", "status": "True"}]}}
				""").getAsJsonObject();
	}

	/** Prometheus answering canned bodies by query, recording what it was asked */
	static final class FakeTransport implements ServingPrometheus.Transport {

		record Call(String api, Map<String, String> parameters) {
		}

		final List<Call> calls = new CopyOnWriteArrayList<>();
		/** the body for each query; an unknown query gets an empty result */
		final Map<String, String> answers = new LinkedHashMap<>();
		/** when set, every query fails with it */
		volatile RuntimeException failure;

		@Override
		public String get(String api, Map<String, String> parameters) {
			this.calls.add(new Call(api, new LinkedHashMap<>(parameters)));
			if (this.failure != null) {
				throw this.failure;
			}
			String body = this.answers.get(parameters.get("query"));
			if (body != null) {
				return body;
			}
			return api.endsWith("query_range") ? matrix() : vector();
		}

		ServingPrometheus prometheus() {
			return new ServingPrometheus(this, "Prometheus for tests", null);
		}
	}

	/**
	 * Prometheus as it answered with the sample READY and idle: DCGM's GPU_UTIL as
	 * recorded (2 %), GPU_TEMP 40 and POWER_USAGE 9.666 as the exporter showed them
	 * (V10), an SM clock of 2418 MHz (invented: not recorded), and the box's memory
	 */
	static FakeTransport olmoRunning(long now) {
		FakeTransport transport = new FakeTransport();
		transport.answers.put(ServingGpuStatus.GPUS_NOW, vector(
				sample(metric("DCGM_FI_DEV_GPU_UTIL", OLMO_GPU), now, 2),
				sample(metric("DCGM_FI_DEV_GPU_TEMP", OLMO_GPU), now, 40),
				sample(metric("DCGM_FI_DEV_POWER_USAGE", OLMO_GPU), now, 9.666),
				sample(metric("DCGM_FI_DEV_SM_CLOCK", OLMO_GPU), now, 2418)));
		transport.answers.put(ServingGpuStatus.NODES_NOW, vector(
				sample(metric("node_memory_MemTotal_bytes", NODE_EXPORTER), now, MEMORY_TOTAL),
				sample(metric("node_memory_MemAvailable_bytes", NODE_EXPORTER), now, MEMORY_AVAILABLE),
				// node_uname_info is not recorded; its nodename is the node's name
				sample(metric("node_uname_info", NODE_EXPORTER
						+ ",\"machine\":\"aarch64\",\"nodename\":\"spark-cbde\",\"release\":\"7.0.0-1019-nvidia\",\"sysname\":\"Linux\""),
						now, 1)));
		return transport;
	}
}
