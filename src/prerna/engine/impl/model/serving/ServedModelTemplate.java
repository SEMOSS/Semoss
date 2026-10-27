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
import static prerna.engine.impl.model.serving.ServedModelStateResolver.LAST_ACTION_ANNOTATION;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.MANAGED_BY_LABEL;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.MANAGED_BY_SEMOSS;
import static prerna.engine.impl.model.serving.ServedModelStateResolver.STOP_ANNOTATION;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import prerna.util.k8s.KubeJson;

/**
 * The InferenceService and Service SEMOSS creates for a model it defines, and
 * the checks on what an admin may put in them. The model's image, probes and
 * defaults come from the ClusterServingRuntime; SEMOSS sets only what the
 * serving catalog and the admin's overrides choose: the weights, the vLLM
 * arguments on an allowlist, and CPU and memory requests. There is never a
 * memory limit: on the GB10's unified memory it gets the pod OOM-killed while it
 * loads.
 */
final class ServedModelTemplate {

	/** leaves room for KServe's "-predictor" and SEMOSS's "-endpoint" within 63 characters */
	static final int MAX_NAME_LENGTH = 45;
	/** 2: entries on s3:// run their pods as the catalog's ServiceAccount */
	static final String TEMPLATE_VERSION = "2";
	static final String DISPLAY_NAME_ANNOTATION = "semoss.org/display-name";
	static final String CATALOG_ENTRY_ANNOTATION = "semoss.org/catalog-entry";
	static final String TEMPLATE_VERSION_ANNOTATION = "semoss.org/template-version";
	static final String PART_OF_LABEL = "app.kubernetes.io/part-of";
	static final String SERVED_MODEL_LABEL = "semoss.org/served-model";
	static final String ENDPOINT_SUFFIX = "-endpoint";

	private static final Pattern NAME = Pattern.compile("[a-z]([-a-z0-9]*[a-z0-9])?");
	private static final Pattern QUANTITY = Pattern.compile("[0-9]+(\\.[0-9]+)?(m|k|M|G|T|Ki|Mi|Gi|Ti)?");
	private static final Pattern WORD = Pattern.compile("[A-Za-z0-9_.-]{1,64}");
	private static final Set<String> DTYPES = Set.of("auto", "half", "float16", "bfloat16", "float", "float32");
	private static final Set<String> SWITCHES = Set.of("--enable-prefix-caching", "--no-enable-prefix-caching",
			"--enable-auto-tool-choice", "--enforce-eager");

	private ServedModelTemplate() {
	}

	/** What an admin may change from the catalog entry when defining a model. */
	record Overrides(Double gpuMemoryUtilization, Integer maxModelLen, List<String> args, String cpu, String memory) {

		static final Overrides NONE = new Overrides(null, null, List.of(), null, null);

		/** From the pixel's map; pixel numbers arrive as doubles. */
		static Overrides from(Map<String, Object> map) {
			if (map == null || map.isEmpty()) {
				return NONE;
			}
			for (String key : map.keySet()) {
				if (!Set.of("gpuMemoryUtilization", "maxModelLen", "args", "cpu", "memory").contains(key)) {
					throw new IllegalArgumentException("Unknown override " + key
							+ "; allowed: gpuMemoryUtilization, maxModelLen, args, cpu, memory");
				}
			}
			List<String> args = new ArrayList<>();
			Object rawArgs = map.get("args");
			if (rawArgs instanceof List<?> list) {
				for (Object arg : list) {
					args.add(String.valueOf(arg));
				}
			} else if (rawArgs != null) {
				throw new IllegalArgumentException("args must be a list of vLLM arguments");
			}
			return new Overrides(number(map, "gpuMemoryUtilization"),
					number(map, "maxModelLen") == null ? null : number(map, "maxModelLen").intValue(), args,
					quantity(map, "cpu"), quantity(map, "memory"));
		}

		private static Double number(Map<String, Object> map, String key) {
			Object value = map.get(key);
			if (value == null) {
				return null;
			}
			try {
				return value instanceof Number number ? number.doubleValue() : Double.valueOf(value.toString());
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(key + " must be a number");
			}
		}

		private static String quantity(Map<String, Object> map, String key) {
			Object value = map.get(key);
			if (value == null) {
				return null;
			}
			// pixel numbers arrive as doubles: 4.0 means 4 CPUs
			String text = value instanceof Number number && number.doubleValue() == Math.rint(number.doubleValue())
					? String.valueOf(number.longValue())
					: value.toString();
			return checkQuantity(key, text);
		}
	}

	static void validateName(String name) {
		if (name == null || !NAME.matcher(name).matches() || name.length() > MAX_NAME_LENGTH) {
			throw new IllegalArgumentException("Invalid served model name " + name + ": lowercase letters, digits "
					+ "and '-', starting with a letter, at most " + MAX_NAME_LENGTH
					+ " characters. Clients send it as the model name.");
		}
	}

	static String checkQuantity(String what, String value) {
		if (value == null || !QUANTITY.matcher(value.strip()).matches()) {
			throw new IllegalArgumentException(what + " must be a Kubernetes quantity such as 2, 500m or 4Gi: " + value);
		}
		return value.strip();
	}

	/**
	 * Only the flags below, each with a checked value. {@code --served-model-name}
	 * (the name is the contract), {@code --host}, {@code --port} and
	 * {@code --trust-remote-code} are refused, like anything else not listed.
	 */
	static List<String> validateArgs(List<String> args, String where) {
		List<String> checked = new ArrayList<>();
		for (String raw : args == null ? List.<String>of() : args) {
			String arg = raw == null ? "" : raw.strip();
			int equals = arg.indexOf('=');
			String flag = equals < 0 ? arg : arg.substring(0, equals);
			String value = equals < 0 ? null : arg.substring(equals + 1);
			if (SWITCHES.contains(flag) && value == null) {
				checked.add(arg);
				continue;
			}
			if (value == null) {
				throw new IllegalArgumentException(where + ": " + arg + " is not an allowed vLLM argument; write "
						+ "--flag=value for one of --gpu-memory-utilization, --max-model-len, --max-num-seqs, --dtype, "
						+ "--quantization, --kv-cache-dtype, --tool-call-parser, --reasoning-parser, or one of "
						+ SWITCHES);
			}
			switch (flag) {
			case "--gpu-memory-utilization" -> bounded(where, flag, value, 0.05, 0.9);
			case "--max-model-len" -> integer(where, flag, value, 256, 1_048_576);
			case "--max-num-seqs" -> integer(where, flag, value, 1, 1024);
			case "--dtype" -> {
				if (!DTYPES.contains(value)) {
					throw new IllegalArgumentException(where + ": --dtype must be one of " + DTYPES);
				}
			}
			case "--quantization", "--kv-cache-dtype", "--tool-call-parser", "--reasoning-parser" -> {
				if (!WORD.matcher(value).matches()) {
					throw new IllegalArgumentException(where + ": " + flag + " takes a single word: " + value);
				}
			}
			default -> throw new IllegalArgumentException(where + ": " + flag + " is not an allowed vLLM argument");
			}
			checked.add(flag + "=" + value);
		}
		return checked;
	}

	private static void bounded(String where, String flag, String value, double min, double max) {
		try {
			double number = Double.parseDouble(value);
			if (number >= min && number <= max) {
				return;
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalArgumentException(where + ": " + flag + " must be between " + min + " and " + max);
	}

	private static void integer(String where, String flag, String value, int min, int max) {
		try {
			int number = Integer.parseInt(value);
			if (number >= min && number <= max) {
				return;
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalArgumentException(where + ": " + flag + " must be a whole number from " + min + " to " + max);
	}

	/** The catalog entry's arguments, then the overrides' (vLLM takes the last value of a flag). */
	static List<String> args(ServingCatalog.Entry entry, Overrides overrides) {
		List<String> args = new ArrayList<>(entry.args());
		if (overrides.gpuMemoryUtilization() != null) {
			args.add("--gpu-memory-utilization=" + overrides.gpuMemoryUtilization());
		}
		if (overrides.maxModelLen() != null) {
			args.add("--max-model-len=" + overrides.maxModelLen());
		}
		args.addAll(overrides.args());
		return validateArgs(args, "overrides");
	}

	static JsonObject inferenceService(String name, String namespace, ServingCatalog.Entry entry, Overrides overrides,
			boolean start, JsonObject lastAction) {
		validateName(name);
		JsonObject labels = new JsonObject();
		labels.addProperty(MANAGED_BY_LABEL, MANAGED_BY_SEMOSS);
		labels.addProperty(PART_OF_LABEL, "semoss-model-serving");
		JsonObject annotations = new JsonObject();
		annotations.addProperty("serving.kserve.io/deploymentMode", "Standard");
		annotations.addProperty(STOP_ANNOTATION, Boolean.toString(!start));
		annotations.addProperty(DISPLAY_NAME_ANNOTATION, entry.displayName());
		annotations.addProperty(CATALOG_ENTRY_ANNOTATION, entry.id());
		annotations.addProperty(TEMPLATE_VERSION_ANNOTATION, TEMPLATE_VERSION);
		annotations.addProperty(LAST_ACTION_ANNOTATION, lastAction.toString());
		JsonObject metadata = new JsonObject();
		metadata.addProperty("name", name);
		metadata.addProperty("namespace", namespace);
		metadata.add("labels", labels);
		metadata.add("annotations", annotations);

		JsonObject requests = new JsonObject();
		requests.addProperty("cpu", overrides.cpu() != null ? overrides.cpu() : entry.cpu());
		requests.addProperty("memory", overrides.memory() != null ? overrides.memory() : entry.memory());
		JsonObject limits = new JsonObject();
		limits.addProperty("nvidia.com/gpu", "1");
		JsonObject resources = new JsonObject();
		resources.add("requests", requests);
		resources.add("limits", limits);
		JsonObject format = new JsonObject();
		format.addProperty("name", entry.modelFormat());
		JsonArray args = new JsonArray();
		args(entry, overrides).forEach(args::add);
		JsonObject model = new JsonObject();
		model.add("modelFormat", format);
		model.addProperty("runtime", entry.runtime());
		model.addProperty("storageUri", entry.storageUri());
		model.add("args", args);
		model.add("resources", resources);

		JsonObject podLabels = new JsonObject();
		podLabels.addProperty(MANAGED_BY_LABEL, MANAGED_BY_SEMOSS);
		JsonObject strategy = new JsonObject();
		// with one GPU a rolling update's surge pod can never be scheduled
		strategy.addProperty("type", "Recreate");
		JsonObject predictor = new JsonObject();
		predictor.addProperty("minReplicas", 1);
		predictor.addProperty("maxReplicas", 1);
		predictor.add("deploymentStrategy", strategy);
		predictor.add("labels", podLabels);
		if (entry.serviceAccount() != null) {
			// its Secret tells KServe's storage initializer where the weights are, and how to read them
			predictor.addProperty("serviceAccountName", entry.serviceAccount());
		}
		predictor.add("model", model);
		JsonObject spec = new JsonObject();
		spec.add("predictor", predictor);

		JsonObject isvc = new JsonObject();
		isvc.addProperty("apiVersion", "serving.kserve.io/v1beta1");
		isvc.addProperty("kind", "InferenceService");
		isvc.add("metadata", metadata);
		isvc.add("spec", spec);
		return isvc;
	}

	/**
	 * Node ports that a published model must never take, though no LoadBalancer in
	 * the namespace shows them: in-cluster SEMOSS (8080), Grafana (3000), the DCGM
	 * exporter (9400), Tomcat on the host (9090), the Kubernetes API (6443) and the
	 * kubelet (10250)
	 */
	static final Set<Integer> RESERVED_PORTS = Set.of(8080, 3000, 9400, 9090, 6443, 10250);

	/**
	 * The node port to publish a model on: the one asked for, or the lowest one
	 * in the range that no LoadBalancer of the namespace uses and that is not
	 * reserved. LoadBalancers of other namespaces are not seen; the reserved
	 * ports cover those on this box.
	 *
	 * @param services the namespace's Services
	 */
	static int hostPort(List<JsonObject> services, ServingSettings.PortRange range, Integer requested) {
		Set<Integer> used = new HashSet<>(RESERVED_PORTS);
		for (JsonObject service : services) {
			if ("LoadBalancer".equals(KubeJson.str(service, "spec", "type"))) {
				for (JsonObject port : KubeJson.objects(service, "spec", "ports")) {
					Integer number = KubeJson.integer(port, "port");
					if (number != null) {
						used.add(number);
					}
				}
			}
		}
		if (requested != null) {
			if (!range.contains(requested)) {
				throw new IllegalArgumentException("publishPort " + requested + " is outside "
						+ ServingSettings.KSERVE_HOST_PORT_RANGE + " " + range);
			}
			if (used.contains(requested)) {
				throw new IllegalArgumentException("Port " + requested + " is already taken"
						+ (RESERVED_PORTS.contains(requested) ? " (reserved)" : " by a LoadBalancer Service"));
			}
			return requested;
		}
		for (int port = range.from(); port <= range.to(); port++) {
			if (!used.contains(port)) {
				return port;
			}
		}
		throw new IllegalArgumentException("Every port of " + ServingSettings.KSERVE_HOST_PORT_RANGE + " " + range
				+ " is taken; remove a published model or widen the range");
	}

	static JsonObject endpointService(String name, String namespace, String isvcUid) {
		return endpointService(name, namespace, isvcUid, null);
	}

	/**
	 * The model's own Service, owned by its InferenceService so that Kubernetes
	 * deletes it with it. Its address survives stops and starts (KServe's own
	 * predictor Service does not). It is a ClusterIP on port 80, or, published, a
	 * LoadBalancer on the host port, which k3s's ServiceLB binds on the node.
	 *
	 * @param hostPort the node port to publish on; null for a ClusterIP
	 */
	static JsonObject endpointService(String name, String namespace, String isvcUid, Integer hostPort) {
		JsonObject owner = new JsonObject();
		owner.addProperty("apiVersion", "serving.kserve.io/v1beta1");
		owner.addProperty("kind", "InferenceService");
		owner.addProperty("name", name);
		owner.addProperty("uid", isvcUid);
		owner.addProperty("blockOwnerDeletion", false);
		JsonArray owners = new JsonArray();
		owners.add(owner);
		JsonObject labels = new JsonObject();
		labels.addProperty(MANAGED_BY_LABEL, MANAGED_BY_SEMOSS);
		labels.addProperty(SERVED_MODEL_LABEL, name);
		JsonObject metadata = new JsonObject();
		metadata.addProperty("name", name + ENDPOINT_SUFFIX);
		metadata.addProperty("namespace", namespace);
		metadata.add("labels", labels);
		metadata.add("ownerReferences", owners);

		JsonObject selector = new JsonObject();
		selector.addProperty(ISVC_LABEL, name);
		selector.addProperty("component", "predictor");
		JsonObject port = new JsonObject();
		port.addProperty("name", "http");
		port.addProperty("port", hostPort == null ? 80 : hostPort);
		port.addProperty("targetPort", 8080);
		port.addProperty("protocol", "TCP");
		JsonArray ports = new JsonArray();
		ports.add(port);
		JsonObject spec = new JsonObject();
		spec.addProperty("type", hostPort == null ? "ClusterIP" : "LoadBalancer");
		spec.add("selector", selector);
		spec.add("ports", ports);

		JsonObject service = new JsonObject();
		service.addProperty("apiVersion", "v1");
		service.addProperty("kind", "Service");
		service.add("metadata", metadata);
		service.add("spec", spec);
		return service;
	}
}
