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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;

import prerna.util.k8s.KubeApiException;
import prerna.util.k8s.KubeClient;
import prerna.util.k8s.KubeJson;

/**
 * Reads what {@link ServingGpuStatus} shows: the nodes' GPU labels from the
 * Kubernetes API, the GPU and memory metrics from Prometheus, and the served
 * models of the pods that use a GPU. Only the node list is required; without
 * Prometheus the card still names the GPUs, and says why it has no metrics.
 */
final class ServingGpuMonitor {

	private static final Logger classLogger = LogManager.getLogger(ServingGpuMonitor.class);

	private final KubeClient client;
	private final String namespace;
	private final ServingPrometheus prometheus;
	private final String grafanaUrl;
	private final String grafanaProblem;

	/**
	 * @param grafanaUrl     Grafana's address; null for port 3000 of the address
	 *                       the browser used, {@link ServingSettings#OFF} for no link
	 * @param grafanaProblem why {@value ServingSettings#KSERVE_GRAFANA_URL} was
	 *                       ignored; null when it was not
	 */
	ServingGpuMonitor(KubeClient client, String namespace, ServingPrometheus prometheus, String grafanaUrl,
			String grafanaProblem) {
		this.client = client;
		this.namespace = namespace;
		this.prometheus = prometheus;
		this.grafanaUrl = grafanaUrl;
		this.grafanaProblem = grafanaProblem;
	}

	/** As the settings say; a malformed metrics setting costs the metrics, not the card */
	static ServingGpuMonitor fromSettings(KubeClient client, String namespace, ServedModelEndpoints.Mode mode) {
		String grafanaUrl = null;
		String grafanaProblem = null;
		try {
			grafanaUrl = ServingSettings.getGrafanaUrl();
		} catch (IllegalStateException e) {
			grafanaProblem = e.getMessage();
		}
		return new ServingGpuMonitor(client, namespace, ServingPrometheus.fromSettings(client, mode), grafanaUrl,
				grafanaProblem);
	}

	ServingGpuStatus read(ServingGpuStatus.Window window, Instant now) {
		List<String> warnings = new ArrayList<>();
		if (this.grafanaProblem != null) {
			warnings.add(this.grafanaProblem);
		}
		List<JsonObject> nodes = nodes(warnings);
		ServingGpuStatus.Readings readings = null;
		ServingGpuStatus.Metrics metrics;
		if (this.prometheus.isOff()) {
			metrics = ServingGpuStatus.Metrics.OFF;
		} else {
			try {
				readings = ServingGpuStatus.read(this.prometheus, window, now);
				metrics = ServingGpuStatus.Metrics.PROMETHEUS;
			} catch (ServingPrometheus.QueryException e) {
				classLogger.warn("Cannot read the GPU metrics: {}", e.getMessage());
				warnings.add("The GPU metrics could not be read. " + e.getMessage());
				metrics = ServingGpuStatus.Metrics.UNAVAILABLE;
			}
		}
		Map<String, String> servedModels = readings == null ? Map.of()
				: servedModels(ServingGpuStatus.podsIn(readings, this.namespace));
		return new ServingGpuStatus(this.namespace, nodes, servedModels, readings, metrics,
				this.prometheus.getSource(), window, now, this.grafanaUrl, warnings);
	}

	/**
	 * The cluster's nodes, for their GPU labels; null, with a warning, when SEMOSS
	 * may not list them (the ClusterRole in semoss-rbac.yml)
	 */
	private List<JsonObject> nodes(List<String> warnings) {
		try {
			return KubeJson.objects(this.client.get(KubeClient.path("api", "v1", "nodes")), "items");
		} catch (KubeApiException e) {
			if (!e.isUnauthorized()) {
				throw e;
			}
			warnings.add("SEMOSS may not list the cluster's nodes (HTTP " + e.getCode()
					+ "), so the GPUs' CUDA version and sharing are not shown; apply semoss-rbac.yml");
			return null;
		}
	}

	/** The served model of each of these predictor pods; empty when the pods cannot be read */
	private Map<String, String> servedModels(Set<String> pods) {
		Map<String, String> servedModels = new HashMap<>();
		if (pods.isEmpty()) {
			return servedModels;
		}
		try {
			for (JsonObject pod : KubeJson.objects(this.client.get(KubeClient.path("api", "v1", "namespaces",
					this.namespace, "pods").query("labelSelector", ISVC_LABEL)), "items")) {
				String name = KubeJson.str(pod, "metadata", "name");
				String servedModel = KubeJson.str(pod, "metadata", "labels", ISVC_LABEL);
				if (name != null && servedModel != null && pods.contains(name)) {
					servedModels.put(name, servedModel);
				}
			}
		} catch (KubeApiException e) {
			// the card then names the pod instead of its model
			classLogger.warn("Cannot list the predictor pods for the GPU card: {}", e.getMessage());
		}
		return servedModels;
	}
}
