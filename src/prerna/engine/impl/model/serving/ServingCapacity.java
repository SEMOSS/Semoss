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
import static prerna.engine.impl.model.serving.ServedModelStateResolver.STOP_ANNOTATION;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;

import prerna.util.k8s.KubeJson;

/**
 * What the GPUs and the unified memory allow. The GPUs are those the schedulable
 * nodes offer, minus those the namespace's pods hold or wait for. The memory is
 * the budget {@value ServingSettings#KSERVE_MEMORY_BUDGET}, minus the
 * {@code --gpu-memory-utilization} fractions of the models that run.
 * {@link #check(String)} decides whether a model may start and names what is in
 * its way, so a start that the scheduler could not place is refused rather than
 * left as a Pending pod that would take the GPU at some later moment.
 *
 * A pure computation over what {@link ServedModelService#capacity()} read. Pods
 * outside the namespace are not seen.
 */
public final class ServingCapacity {

	public static final String GPU = "nvidia.com/gpu";
	static final String FRACTION_FLAG = "--gpu-memory-utilization";
	/** vLLM's default when neither the model nor its runtime sets a fraction */
	static final double VLLM_DEFAULT_FRACTION = 0.9;
	private static final double EPSILON = 1e-9;
	private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm 'UTC'").withZone(ZoneOffset.UTC);
	private static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm 'UTC'")
			.withZone(ZoneOffset.UTC);

	/** A node's GPUs; those of a node that is not Ready, or is cordoned, do not count */
	public record Node(String name, int gpus, boolean schedulable) {
	}

	/**
	 * A pod that holds GPUs or waits for them, and its served model when it has one.
	 *
	 * @param leaving the pod is going away: it is being deleted, or its model is
	 *                being stopped or removed
	 */
	public record Holder(String servedModel, String pod, int gpus, ServedModelState state, Instant since,
			String startedBy, boolean leaving) {
	}

	/** A model that runs or is still going away, and its share of the unified memory */
	public record MemoryUse(String servedModel, double fraction, String source, boolean leaving) {
	}

	/**
	 * Whether a model may start now.
	 *
	 * @param blocking  the served models in its way, which a replace would stop
	 * @param unmanaged pods without a served model that hold GPUs it needs
	 * @param waitable  everything in its way is already going away, so waiting
	 *                  is enough
	 */
	public record Decision(String servedModel, boolean allowed, String reason, List<String> blocking,
			List<String> unmanaged, boolean waitable) {
	}

	private record Fraction(double value, String source) {
	}

	private final String namespace;
	private final List<Node> nodes;
	private final Map<String, JsonObject> services = new LinkedHashMap<>();
	private final Map<String, ServedModelStatus> statuses = new HashMap<>();
	private final Map<String, Double> runtimeFractions;
	private final List<Holder> holders = new ArrayList<>();
	private final List<MemoryUse> memory = new ArrayList<>();
	private final double budget;
	private final Instant now;
	private final List<String> warnings;

	/**
	 * @param nodes            the nodes' GPUs; null when the nodes cannot be read,
	 *                         which skips the GPU check
	 * @param runtimeFractions the {@value #FRACTION_FLAG} of each runtime read, by
	 *                         name (null for none)
	 * @param budget           the share of the unified memory all models may use
	 */
	ServingCapacity(String namespace, List<Node> nodes, List<JsonObject> inferenceServices, List<JsonObject> pods,
			Map<String, Double> runtimeFractions, double budget, Instant now, List<String> warnings) {
		this.namespace = namespace;
		this.nodes = nodes == null ? null : List.copyOf(nodes);
		this.runtimeFractions = runtimeFractions;
		this.budget = budget;
		this.now = now;
		this.warnings = List.copyOf(warnings);
		Map<String, List<JsonObject>> podsByModel = new HashMap<>();
		for (JsonObject pod : pods) {
			String owner = KubeJson.str(pod, "metadata", "labels", ISVC_LABEL);
			if (owner != null) {
				podsByModel.computeIfAbsent(owner, key -> new ArrayList<>()).add(pod);
			}
		}
		for (JsonObject service : inferenceServices) {
			String name = KubeJson.str(service, "metadata", "name");
			this.services.put(name, service);
			this.statuses.put(name, ServedModelStateResolver.resolve(service, podsByModel.getOrDefault(name, List.of()),
					List.of(), null, now));
		}
		for (JsonObject pod : pods) {
			int gpus = gpus(pod);
			if (gpus == 0 || !live(pod)) {
				continue;
			}
			String owner = KubeJson.str(pod, "metadata", "labels", ISVC_LABEL);
			ServedModelStatus status = owner == null ? null : this.statuses.get(owner);
			boolean leaving = KubeJson.str(pod, "metadata", "deletionTimestamp") != null
					|| (owner != null && going(this.services.get(owner)));
			this.holders.add(new Holder(owner, KubeJson.str(pod, "metadata", "name"), gpus,
					status == null ? null : status.getState(), status == null ? null : status.phaseSince,
					startedBy(status), leaving));
		}
		for (Map.Entry<String, JsonObject> entry : this.services.entrySet()) {
			String name = entry.getKey();
			boolean hasPods = podsByModel.getOrDefault(name, List.of()).stream().anyMatch(ServingCapacity::live);
			boolean going = going(entry.getValue());
			if (going && !hasPods) {
				continue;
			}
			Fraction fraction = fraction(entry.getValue());
			this.memory.add(new MemoryUse(name, fraction.value(), fraction.source(), going));
		}
	}

	/** The GPUs of each node: the device plugin's allocatable count */
	static List<Node> nodes(List<JsonObject> items) {
		List<Node> nodes = new ArrayList<>();
		for (JsonObject node : items) {
			boolean ready = KubeJson.objects(node, "status", "conditions").stream()
					.anyMatch(condition -> "Ready".equals(KubeJson.str(condition, "type"))
							&& "True".equals(KubeJson.str(condition, "status")));
			nodes.add(new Node(KubeJson.str(node, "metadata", "name"), count(KubeJson.str(node, "status", "allocatable", GPU)),
					ready && !KubeJson.isTrue(node, "spec", "unschedulable")));
		}
		return nodes;
	}

	/** The model's own {@value #FRACTION_FLAG}, which overrides its runtime's; null when it sets none */
	static Double ownFraction(JsonObject service) {
		return ServedModelStateResolver.lastFlagValue(KubeJson.strings(service, "spec", "predictor", "model", "args"),
				FRACTION_FLAG);
	}

	/** A (Cluster)ServingRuntime's {@value #FRACTION_FLAG}; null when it sets none */
	static Double runtimeFraction(JsonObject runtime) {
		for (JsonObject container : KubeJson.objects(runtime, "spec", "containers")) {
			if (ServedModelStateResolver.MAIN_CONTAINER.equals(KubeJson.str(container, "name"))) {
				return ServedModelStateResolver.lastFlagValue(KubeJson.strings(container, "args"), FRACTION_FLAG);
			}
		}
		return null;
	}

	static String runtimeOf(JsonObject service) {
		return KubeJson.str(service, "spec", "predictor", "model", "runtime");
	}

	/** The GPUs the model's server asks for */
	static int gpusNeeded(JsonObject service) {
		JsonObject resources = KubeJson.obj(service, "spec", "predictor", "model", "resources");
		String limit = KubeJson.str(resources, "limits", GPU);
		return count(limit != null ? limit : KubeJson.str(resources, "requests", GPU));
	}

	/**
	 * Whether the model may start now. Its own pods do not count against it, so a
	 * model that is being stopped, or has failed, can start again in its own place.
	 */
	public Decision check(String name) {
		JsonObject service = this.services.get(name);
		if (service == null) {
			throw new IllegalArgumentException("No served model named " + name + " in namespace " + this.namespace);
		}
		List<String> reasons = new ArrayList<>();
		List<String> blocking = new ArrayList<>();
		List<String> unmanaged = new ArrayList<>();
		boolean waitable = true;

		int needed = gpusNeeded(service);
		if (needed > 0 && this.nodes != null) {
			int offered = offeredGpus();
			List<Holder> others = this.holders.stream().filter(holder -> !name.equals(holder.servedModel())).toList();
			int held = others.stream().mapToInt(Holder::gpus).sum();
			if (offered < needed) {
				reasons.add(name + " needs " + gpus(needed) + ", and the schedulable nodes offer "
						+ (offered == 0 ? "none" : String.valueOf(offered))
						+ "; check that the NVIDIA device plugin runs and that no node is cordoned.");
				waitable = false;
			} else if (held + needed > offered) {
				List<String> described = others.stream().map(this::describe).toList();
				reasons.add(offered == 1 ? "The GPU is in use by " + join(described) + "."
						: name + " needs " + gpus(needed) + " and " + Math.max(0, offered - held) + " of the " + offered
								+ " are free; the others are in use by " + join(described) + ".");
				for (Holder holder : others) {
					if (holder.servedModel() == null) {
						unmanaged.add(holder.pod());
					} else if (!blocking.contains(holder.servedModel())) {
						blocking.add(holder.servedModel());
					}
					waitable &= holder.leaving();
				}
			}
		}

		double fraction = fraction(service).value();
		List<MemoryUse> running = this.memory.stream().filter(use -> !name.equals(use.servedModel())).toList();
		double used = running.stream().mapToDouble(MemoryUse::fraction).sum();
		if (fraction > this.budget + EPSILON) {
			reasons.add(name + " asks for " + number(fraction) + " of the unified memory, more than the "
					+ number(this.budget) + " all models may use (" + ServingSettings.KSERVE_MEMORY_BUDGET + ").");
			waitable = false;
		} else if (used + fraction > this.budget + EPSILON) {
			reasons.add(name + " needs " + number(fraction) + " of the unified memory, and "
					+ join(running.stream().map(use -> use.servedModel() + " (" + number(use.fraction())
							+ (use.leaving() ? ", stopping" : "") + ")").toList())
					+ (running.size() == 1 ? " already uses " : " already use ") + number(used) + " of the "
					+ number(this.budget) + " all models may use ("
					+ ServingSettings.KSERVE_MEMORY_BUDGET + ").");
			for (MemoryUse use : running) {
				if (!blocking.contains(use.servedModel())) {
					blocking.add(use.servedModel());
				}
				waitable &= use.leaving();
			}
		}

		if (reasons.isEmpty()) {
			return new Decision(name, true, null, List.of(), List.of(), false);
		}
		String advice;
		if (!unmanaged.isEmpty()) {
			advice = " SEMOSS does not manage " + join(unmanaged.stream().map(pod -> "pod " + pod).toList())
					+ "; free it with kubectl.";
		} else if (blocking.isEmpty()) {
			advice = "";
		} else if (waitable) {
			advice = " " + (blocking.size() == 1 ? "It is" : "They are") + " stopping; start " + name
					+ " again in a moment.";
		} else {
			advice = " Stop " + (blocking.size() == 1 ? "it" : "them") + " first, or start " + name
					+ " with replace=[true].";
		}
		return new Decision(name, false, String.join(" ", reasons) + advice, List.copyOf(blocking),
				List.copyOf(unmanaged), waitable && !blocking.isEmpty() && unmanaged.isEmpty());
	}

	private int offeredGpus() {
		return this.nodes.stream().filter(Node::schedulable).mapToInt(Node::gpus).sum();
	}

	private Fraction fraction(JsonObject service) {
		Double own = ownFraction(service);
		if (own != null) {
			return new Fraction(own, "model");
		}
		String runtime = runtimeOf(service);
		Double fromRuntime = runtime == null ? null : this.runtimeFractions.get(runtime);
		if (fromRuntime != null) {
			return new Fraction(fromRuntime, "runtime " + runtime);
		}
		return new Fraction(VLLM_DEFAULT_FRACTION, "vLLM default");
	}

	private String describe(Holder holder) {
		if (holder.servedModel() == null) {
			return "pod " + holder.pod();
		}
		if (holder.leaving()) {
			return holder.servedModel() + " (stopping)";
		}
		StringBuilder text = new StringBuilder(holder.servedModel()).append(" (")
				.append(holder.state() == null ? "running" : holder.state().name());
		if (holder.since() != null) {
			text.append(" since ").append(when(holder.since()));
		}
		if (holder.startedBy() != null) {
			text.append(", started by ").append(holder.startedBy());
		}
		return text.append(")").toString();
	}

	private String when(Instant time) {
		boolean today = time.atZone(ZoneOffset.UTC).toLocalDate().equals(this.now.atZone(ZoneOffset.UTC).toLocalDate());
		return (today ? TIME : DATE_TIME).format(time);
	}

	/** Who started the model, when SEMOSS did and knows the user */
	private static String startedBy(ServedModelStatus status) {
		Map<String, Object> action = status == null ? null : status.lastAction;
		if (action == null || !("start".equals(action.get("action")) || "define".equals(action.get("action")))) {
			return null;
		}
		Object name = action.get("userName") != null ? action.get("userName") : action.get("user");
		return name == null ? null : name.toString();
	}

	/** Being stopped or removed */
	private static boolean going(JsonObject service) {
		return service == null || "true".equalsIgnoreCase(KubeJson.str(service, "metadata", "annotations", STOP_ANNOTATION))
				|| KubeJson.str(service, "metadata", "deletionTimestamp") != null;
	}

	private static boolean live(JsonObject pod) {
		String phase = KubeJson.str(pod, "status", "phase");
		return !"Succeeded".equals(phase) && !"Failed".equals(phase);
	}

	private static int gpus(JsonObject pod) {
		int gpus = 0;
		for (JsonObject container : KubeJson.objects(pod, "spec", "containers")) {
			String limit = KubeJson.str(container, "resources", "limits", GPU);
			gpus += count(limit != null ? limit : KubeJson.str(container, "resources", "requests", GPU));
		}
		return gpus;
	}

	private static int count(String quantity) {
		try {
			return quantity == null ? 0 : Integer.parseInt(quantity.strip());
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	private static String gpus(int count) {
		return count == 1 ? "1 GPU" : count + " GPUs";
	}

	private static String number(double value) {
		return String.valueOf(Math.round(value * 100) / 100.0);
	}

	private static String join(List<String> items) {
		if (items.size() <= 1) {
			return items.isEmpty() ? "" : items.get(0);
		}
		return String.join(", ", items.subList(0, items.size() - 1)) + " and " + items.get(items.size() - 1);
	}

	public List<Holder> getHolders() {
		return this.holders;
	}

	public List<String> getWarnings() {
		return this.warnings;
	}

	/** For GetServingCapacity: the GPUs, the memory, and whether each stopped model could start now */
	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("namespace", this.namespace);
		Map<String, Object> gpus = new LinkedHashMap<>();
		int held = this.holders.stream().mapToInt(Holder::gpus).sum();
		if (this.nodes != null) {
			int offered = offeredGpus();
			gpus.put("offered", offered);
			gpus.put("held", held);
			gpus.put("free", Math.max(0, offered - held));
			gpus.put("nodes", this.nodes.stream().map(node -> {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("name", node.name());
				entry.put("gpus", node.gpus());
				entry.put("schedulable", node.schedulable());
				return entry;
			}).toList());
		} else {
			gpus.put("held", held);
		}
		gpus.put("holders", this.holders.stream().map(holder -> {
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("servedModel", holder.servedModel());
			entry.put("pod", holder.pod());
			entry.put("gpus", holder.gpus());
			entry.put("state", holder.state() == null ? null : holder.state().name());
			entry.put("since", holder.since() == null ? null : holder.since().toString());
			entry.put("startedBy", holder.startedBy());
			entry.put("leaving", holder.leaving());
			return entry;
		}).toList());
		map.put("gpus", gpus);
		Map<String, Object> memoryMap = new LinkedHashMap<>();
		double used = this.memory.stream().mapToDouble(MemoryUse::fraction).sum();
		memoryMap.put("budget", this.budget);
		memoryMap.put("used", Math.round(used * 1000) / 1000.0);
		memoryMap.put("free", Math.round(Math.max(0, this.budget - used) * 1000) / 1000.0);
		memoryMap.put("models", this.memory.stream().map(use -> {
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("servedModel", use.servedModel());
			entry.put("fraction", use.fraction());
			entry.put("source", use.source());
			entry.put("leaving", use.leaving());
			return entry;
		}).toList());
		map.put("memory", memoryMap);
		List<Map<String, Object>> models = new ArrayList<>();
		for (Map.Entry<String, JsonObject> entry : this.services.entrySet()) {
			ServedModelStatus status = this.statuses.get(entry.getKey());
			Map<String, Object> model = new LinkedHashMap<>();
			model.put("servedModel", entry.getKey());
			model.put("state", status.getState().name());
			model.put("gpus", gpusNeeded(entry.getValue()));
			Fraction fraction = fraction(entry.getValue());
			model.put("gpuMemoryUtilization", fraction.value());
			model.put("fractionSource", fraction.source());
			if (status.getState() == ServedModelState.STOPPED || status.getState() == ServedModelState.FAILED) {
				Decision decision = check(entry.getKey());
				model.put("canStart", decision.allowed());
				if (!decision.allowed()) {
					model.put("reason", decision.reason());
				}
			}
			models.add(model);
		}
		map.put("models", models);
		map.put("warnings", this.warnings);
		return map;
	}
}
