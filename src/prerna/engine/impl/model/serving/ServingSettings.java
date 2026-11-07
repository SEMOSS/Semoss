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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import prerna.util.Utility;
import prerna.util.k8s.KubeConnection;

/**
 * Settings of the KServe connector. Each is read from RDF_Map.prop first (SEMOSS
 * in Tomcat on the host) and then from the environment (the SEMOSS pod's
 * ConfigMap), so the same code runs in both places.
 */
public final class ServingSettings {

	/** {@code true} turns the model-serving reactors on */
	public static final String KSERVE_ENABLED = "KSERVE_ENABLED";
	/** Namespace of the InferenceServices; default {@value #DEFAULT_NAMESPACE} */
	public static final String KSERVE_NAMESPACE = "KSERVE_NAMESPACE";
	/** Kubeconfig to use; default {@code $KUBECONFIG}, then {@code ~/.kube/config} */
	public static final String KSERVE_KUBECONFIG = "KSERVE_KUBECONFIG";
	/** Kubeconfig context to use; default its current-context */
	public static final String KSERVE_KUBE_CONTEXT = "KSERVE_KUBE_CONTEXT";
	/** {@code auto} (default), {@code incluster} or {@code kubeconfig} */
	public static final String KSERVE_KUBE_AUTH = "KSERVE_KUBE_AUTH";
	/** {@code auto} (default), {@code cluster} or {@code host}: how SEMOSS reaches the models */
	public static final String KSERVE_NETWORK_MODE = "KSERVE_NETWORK_MODE";
	/** Host part of LoadBalancer endpoints when SEMOSS runs on the host; default localhost */
	public static final String KSERVE_HOST_ADDRESS = "KSERVE_HOST_ADDRESS";
	/** {@code configmap:<name>} (default {@code configmap:semoss-serving-catalog}) or {@code file:<path>} */
	public static final String KSERVE_CATALOG = "KSERVE_CATALOG";
	/** The (Cluster)ServingRuntimes catalog entries may use, comma-separated; default nvidia-vllm */
	public static final String KSERVE_RUNTIMES = "KSERVE_RUNTIMES";
	/**
	 * The PersistentVolumeClaims that {@code pvc://} catalog entries may take
	 * weights from, comma-separated; default {@value #DEFAULT_WEIGHT_VOLUME}
	 */
	public static final String KSERVE_WEIGHT_VOLUMES = "KSERVE_WEIGHT_VOLUMES";
	public static final String KSERVE_MEMORY_BUDGET = "KSERVE_MEMORY_BUDGET";
	public static final String KSERVE_HOST_PORT_RANGE = "KSERVE_HOST_PORT_RANGE";
	/**
	 * Where the GPU card reads its metrics: {@code auto} (default: Prometheus by
	 * cluster DNS in a pod, through the Kubernetes API on the host), {@code off},
	 * or Prometheus's own URL, such as its ClusterIP
	 */
	public static final String KSERVE_PROMETHEUS_URL = "KSERVE_PROMETHEUS_URL";
	/**
	 * The Grafana the GPU card links to: {@code auto} (default: port 3000 of the
	 * address the browser used), {@code off}, or Grafana's URL
	 */
	public static final String KSERVE_GRAFANA_URL = "KSERVE_GRAFANA_URL";
	/** The value that turns {@link #KSERVE_PROMETHEUS_URL} or {@link #KSERVE_GRAFANA_URL} off */
	public static final String OFF = "off";

	/** Where the Kubernetes credentials come from */
	public enum Auth {
		IN_CLUSTER, KUBECONFIG
	}

	/** The node ports SEMOSS may publish models on, both ends included */
	public record PortRange(int from, int to) {

		public boolean contains(int port) {
			return port >= this.from && port <= this.to;
		}

		@Override
		public String toString() {
			return this.from + "-" + this.to;
		}
	}

	static final PortRange DEFAULT_HOST_PORT_RANGE = new PortRange(8000, 8099);

	static final String DEFAULT_NAMESPACE = "semoss";
	/**
	 * The share of the unified memory that all models together may claim with
	 * --gpu-memory-utilization: about 84 GiB of the GB10's 128 GB, which leaves
	 * about 35 GiB for the host, SEMOSS, Postgres, MinIO and Prometheus
	 */
	static final double DEFAULT_MEMORY_BUDGET = 0.7;
	/** The model cache that the intake Job fills (models/model-cache.yml in the dgx repo) */
	static final String DEFAULT_WEIGHT_VOLUME = "semoss-model-cache";

	private static final Pattern NAMESPACE = Pattern.compile("[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?");
	/** A PersistentVolumeClaim name: a DNS-1123 subdomain */
	private static final Pattern CLAIM = Pattern.compile("[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?");

	private ServingSettings() {
	}

	static String get(String key) {
		String value = Utility.getDIHelperProperty(key);
		if (value == null || value.isBlank()) {
			value = System.getenv(key);
		}
		return value == null || value.isBlank() ? null : value.strip();
	}

	public static boolean isEnabled() {
		return Boolean.parseBoolean(get(KSERVE_ENABLED));
	}

	public static String getNamespace() {
		String namespace = get(KSERVE_NAMESPACE);
		if (namespace == null) {
			return DEFAULT_NAMESPACE;
		}
		if (!NAMESPACE.matcher(namespace).matches()) {
			throw new IllegalStateException(KSERVE_NAMESPACE + " is not a valid namespace name: " + namespace);
		}
		return namespace;
	}

	public static String getKubeContext() {
		return get(KSERVE_KUBE_CONTEXT);
	}

	public static Auth getAuth() {
		return auth(get(KSERVE_KUBE_AUTH), get(KSERVE_KUBECONFIG) != null, KubeConnection.isInCluster());
	}

	/**
	 * In a pod, its ServiceAccount, unless a kubeconfig is named explicitly;
	 * elsewhere a kubeconfig.
	 */
	static Auth auth(String setting, boolean kubeconfigNamed, boolean inCluster) {
		if (setting == null || setting.equalsIgnoreCase("auto")) {
			return inCluster && !kubeconfigNamed ? Auth.IN_CLUSTER : Auth.KUBECONFIG;
		}
		return switch (setting.toLowerCase(Locale.ROOT)) {
		case "incluster", "in-cluster" -> Auth.IN_CLUSTER;
		case "kubeconfig" -> Auth.KUBECONFIG;
		default -> throw new IllegalStateException(KSERVE_KUBE_AUTH + " must be auto, incluster or kubeconfig");
		};
	}

	public static ServedModelEndpoints.Mode getNetworkMode() {
		return networkMode(get(KSERVE_NETWORK_MODE), KubeConnection.isInCluster());
	}

	/** Cluster DNS in a pod, the node's published ports and ClusterIPs elsewhere. */
	static ServedModelEndpoints.Mode networkMode(String setting, boolean inCluster) {
		if (setting == null || setting.equalsIgnoreCase("auto")) {
			return inCluster ? ServedModelEndpoints.Mode.CLUSTER : ServedModelEndpoints.Mode.HOST;
		}
		return switch (setting.toLowerCase(Locale.ROOT)) {
		case "cluster" -> ServedModelEndpoints.Mode.CLUSTER;
		case "host" -> ServedModelEndpoints.Mode.HOST;
		default -> throw new IllegalStateException(KSERVE_NETWORK_MODE + " must be auto, cluster or host");
		};
	}

	public static String getHostAddress() {
		String address = get(KSERVE_HOST_ADDRESS);
		return address == null ? "localhost" : address;
	}

	/** @return Prometheus's URL, null for auto, or {@link #OFF} */
	public static String getPrometheusUrl() {
		return urlOrOff(KSERVE_PROMETHEUS_URL, get(KSERVE_PROMETHEUS_URL), "http://10.43.0.10:9090");
	}

	/** @return Grafana's URL, null for auto, or {@link #OFF} */
	public static String getGrafanaUrl() {
		return urlOrOff(KSERVE_GRAFANA_URL, get(KSERVE_GRAFANA_URL), "http://spark-cbde.local:3000");
	}

	/** An http(s) URL without its trailing slash, null for auto, or {@link #OFF} */
	static String urlOrOff(String key, String setting, String example) {
		if (setting == null || setting.equalsIgnoreCase("auto")) {
			return null;
		}
		if (setting.equalsIgnoreCase(OFF)) {
			return OFF;
		}
		try {
			URI uri = new URI(setting);
			String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
			if ((scheme.equals("http") || scheme.equals("https")) && uri.getHost() != null && uri.getQuery() == null
					&& uri.getFragment() == null) {
				return setting.replaceAll("/+$", "");
			}
		} catch (URISyntaxException e) {
			// reported below
		}
		throw new IllegalStateException(
				key + " must be auto, off or an http(s) URL such as " + example + "; it is " + setting);
	}

	public static String getCatalog() {
		String catalog = get(KSERVE_CATALOG);
		return catalog == null ? "configmap:" + ServingCatalog.DEFAULT_CONFIGMAP : catalog;
	}

	public static double getMemoryBudget() {
		return memoryBudget(get(KSERVE_MEMORY_BUDGET));
	}

	static double memoryBudget(String setting) {
		if (setting == null) {
			return DEFAULT_MEMORY_BUDGET;
		}
		try {
			double budget = Double.parseDouble(setting);
			if (budget > 0 && budget <= 1) {
				return budget;
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalStateException(
				KSERVE_MEMORY_BUDGET + " must be a fraction above 0 and at most 1, such as 0.7; it is " + setting);
	}

	public static PortRange getHostPortRange() {
		return hostPortRange(get(KSERVE_HOST_PORT_RANGE));
	}

	static PortRange hostPortRange(String setting) {
		if (setting == null) {
			return DEFAULT_HOST_PORT_RANGE;
		}
		String[] ends = setting.split("-", -1);
		try {
			if (ends.length == 2) {
				PortRange range = new PortRange(Integer.parseInt(ends[0].strip()), Integer.parseInt(ends[1].strip()));
				if (range.from() >= 1024 && range.from() <= range.to() && range.to() <= 65535) {
					return range;
				}
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalStateException(KSERVE_HOST_PORT_RANGE
				+ " must be two ports from 1024 to 65535, such as 8000-8099; it is " + setting);
	}

	public static Set<String> getRuntimes() {
		return runtimes(get(KSERVE_RUNTIMES));
	}

	static Set<String> runtimes(String setting) {
		Set<String> runtimes = new LinkedHashSet<>();
		if (setting != null) {
			for (String runtime : setting.split(",")) {
				if (!runtime.isBlank()) {
					runtimes.add(runtime.strip());
				}
			}
		}
		return runtimes.isEmpty() ? Set.of("nvidia-vllm") : Set.copyOf(runtimes);
	}

	public static Set<String> getWeightVolumes() {
		return weightVolumes(get(KSERVE_WEIGHT_VOLUMES));
	}

	/**
	 * KServe mounts the whole claim of a {@code pvc://} entry into the model's pod,
	 * so only claims that hold nothing but weights belong here, never a database's.
	 */
	static Set<String> weightVolumes(String setting) {
		Set<String> claims = new LinkedHashSet<>();
		if (setting != null) {
			for (String claim : setting.split(",")) {
				if (claim.isBlank()) {
					continue;
				}
				if (!CLAIM.matcher(claim.strip()).matches()) {
					throw new IllegalStateException(KSERVE_WEIGHT_VOLUMES + " lists " + claim.strip()
							+ ", which is not a PersistentVolumeClaim name");
				}
				claims.add(claim.strip());
			}
		}
		return claims.isEmpty() ? Set.of(DEFAULT_WEIGHT_VOLUME) : Set.copyOf(claims);
	}

	public static Path getKubeconfig() {
		String configured = get(KSERVE_KUBECONFIG);
		if (configured != null) {
			return Path.of(expandHome(configured));
		}
		String fromEnvironment = System.getenv("KUBECONFIG");
		if (fromEnvironment != null && !fromEnvironment.isBlank()) {
			// kubectl merges a list; the connector uses the first file
			return Path.of(expandHome(fromEnvironment.split(Pattern.quote(File.pathSeparator))[0].strip()));
		}
		return Path.of(System.getProperty("user.home"), ".kube", "config");
	}

	private static String expandHome(String path) {
		if (path.equals("~") || path.startsWith("~/")) {
			return System.getProperty("user.home") + path.substring(1);
		}
		return path;
	}
}
