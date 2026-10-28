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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * The models admins have approved for serving: the only ones SEMOSS defines.
 * Each entry records the weights, the runtime and the tested vLLM arguments and
 * resources, and who approved it under the model policy (licence, origin). The
 * catalog is a YAML document, normally the ConfigMap
 * {@value #DEFAULT_CONFIGMAP} next to the models, so it is reviewed like any
 * other manifest and shared by every SEMOSS that manages them.
 *
 * <pre>
 * models:
 *   - id: allenai/OLMo-2-0425-1B-Instruct
 *     displayName: OLMo 2 1B Instruct
 *     storageUri: hf://allenai/OLMo-2-0425-1B-Instruct
 *     args: ["--gpu-memory-utilization=0.2"]
 *     cpu: "2"
 *     memory: 4Gi
 *     contextWindow: 4096
 *     maxTokens: 1024
 *     licence: apache-2.0
 *     origin: Ai2 (US)
 *     approvedBy: …
 * </pre>
 *
 * An entry on {@code s3://} (the copy the intake Job put in MinIO) runs its
 * model pods as {@value #DEFAULT_S3_SERVICE_ACCOUNT}, which reads the models
 * bucket; {@code serviceAccount} names another.
 *
 * An entry on {@code pvc://claim/path} takes its weights from a volume that
 * outlives the model's pods, such as the model cache the intake Job fills, so a
 * start copies nothing. KServe mounts the whole claim into the pod, so the claim
 * must be one of {@link ServingSettings#KSERVE_WEIGHT_VOLUMES}.
 *
 * An invalid entry is left out and reported, so one mistake does not hide the
 * rest of the catalog.
 */
public final class ServingCatalog {

	public static final String DEFAULT_CONFIGMAP = "semoss-serving-catalog";
	/** The ServiceAccount whose Secret tells KServe's storage initializer where MinIO is */
	static final String DEFAULT_S3_SERVICE_ACCOUNT = "kserve-minio";
	static final String CATALOG_KEY = "catalog.yaml";

	static final String PVC_SCHEME = "pvc://";

	private static final YAMLMapper YAML = new YAMLMapper();
	private static final Set<String> STORAGE_SCHEMES = Set.of("hf://", "s3://", PVC_SCHEME);
	/** A ServiceAccount name: a DNS-1123 subdomain */
	private static final Pattern SERVICE_ACCOUNT = Pattern.compile("[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?");
	/** One folder of a pvc:// path; no "." or "..", and no hidden folder such as the intake's .incoming */
	private static final Pattern PATH_SEGMENT = Pattern.compile("[A-Za-z0-9_][-A-Za-z0-9_.]*");

	/** The parts of a {@code pvc://claim/path} storage URI */
	record VolumeUri(String claim, String path) {

		/** @return the parts, or null when the URI is not on pvc:// */
		static VolumeUri of(String storageUri) {
			if (storageUri == null || !storageUri.startsWith(PVC_SCHEME)) {
				return null;
			}
			String rest = storageUri.substring(PVC_SCHEME.length());
			int slash = rest.indexOf('/');
			return slash < 0 ? new VolumeUri(rest, "")
					: new VolumeUri(rest.substring(0, slash), rest.substring(slash + 1));
		}
	}

	/**
	 * @param serviceAccount the ServiceAccount the model's pods run as, which gives
	 *                       KServe access to the storage; null for the default
	 * @param contextWindow  the model's context length in tokens, for its SEMOSS
	 *                       engine
	 * @param maxTokens      the default completion length, for its SEMOSS engine
	 */
	public record Entry(String id, String displayName, String storageUri, String serviceAccount, String runtime,
			String modelFormat, List<String> args, String cpu, String memory, Long sizeBytes, Integer contextWindow,
			Integer maxTokens, String licence, String origin, String approvedBy, String approvedAt) {

		public Map<String, Object> toMap() {
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("id", this.id);
			map.put("displayName", this.displayName);
			map.put("storageUri", this.storageUri);
			map.put("serviceAccount", this.serviceAccount);
			map.put("runtime", this.runtime);
			map.put("args", this.args);
			map.put("cpu", this.cpu);
			map.put("memory", this.memory);
			map.put("sizeBytes", this.sizeBytes);
			map.put("contextWindow", this.contextWindow);
			map.put("maxTokens", this.maxTokens);
			map.put("licence", this.licence);
			map.put("origin", this.origin);
			map.put("approvedBy", this.approvedBy);
			map.put("approvedAt", this.approvedAt);
			return map;
		}
	}

	private final String source;
	private final List<Entry> entries;
	private final List<String> problems;

	private ServingCatalog(String source, List<Entry> entries, List<String> problems) {
		this.source = source;
		this.entries = List.copyOf(entries);
		this.problems = List.copyOf(problems);
	}

	static ServingCatalog empty(String source, String problem) {
		return new ServingCatalog(source, List.of(), problem == null ? List.of() : List.of(problem));
	}

	/** As {@link #parse(String, String, Set, Set)}, with the default model cache as the only volume */
	static ServingCatalog parse(String yaml, String source, Set<String> allowedRuntimes) {
		return parse(yaml, source, allowedRuntimes, Set.of(ServingSettings.DEFAULT_WEIGHT_VOLUME));
	}

	/**
	 * @param allowedRuntimes the (Cluster)ServingRuntimes entries may name
	 * @param allowedVolumes  the PersistentVolumeClaims pvc:// entries may name
	 */
	static ServingCatalog parse(String yaml, String source, Set<String> allowedRuntimes, Set<String> allowedVolumes) {
		Map<String, Object> root;
		try {
			root = yaml == null || yaml.isBlank() ? Map.of()
					: YAML.readValue(yaml, new TypeReference<Map<String, Object>>() {
					});
		} catch (JacksonException e) {
			return empty(source, source + " is not valid YAML: " + e.getOriginalMessage());
		}
		List<Entry> entries = new ArrayList<>();
		List<String> problems = new ArrayList<>();
		Object models = root == null ? null : root.get("models");
		if (!(models instanceof List<?> list)) {
			return empty(source, source + " has no models list");
		}
		Set<String> ids = new HashSet<>();
		for (Object item : list) {
			if (!(item instanceof Map<?, ?> map)) {
				problems.add(source + ": a models entry is not a map");
				continue;
			}
			String id = text(map, "id");
			try {
				Entry entry = entry(map, allowedRuntimes, allowedVolumes);
				if (!ids.add(entry.id())) {
					throw new IllegalArgumentException("is listed twice");
				}
				entries.add(entry);
			} catch (IllegalArgumentException e) {
				problems.add(source + ": entry " + (id == null ? "without an id" : id) + " " + e.getMessage());
			}
		}
		return new ServingCatalog(source, entries, problems);
	}

	private static Entry entry(Map<?, ?> map, Set<String> allowedRuntimes, Set<String> allowedVolumes) {
		String id = required(map, "id");
		String storageUri = required(map, "storageUri");
		if (STORAGE_SCHEMES.stream().noneMatch(storageUri::startsWith)) {
			throw new IllegalArgumentException("has storageUri " + storageUri + "; use hf://, s3:// or pvc://");
		}
		VolumeUri volume = VolumeUri.of(storageUri);
		if (volume != null) {
			checkVolume(storageUri, volume, allowedVolumes);
		}
		String runtime = text(map, "runtime") == null ? "nvidia-vllm" : text(map, "runtime");
		if (!allowedRuntimes.contains(runtime)) {
			throw new IllegalArgumentException("names runtime " + runtime + ", which is not one of " + allowedRuntimes);
		}
		List<String> args = new ArrayList<>();
		if (map.get("args") instanceof List<?> list) {
			list.forEach(arg -> args.add(String.valueOf(arg)));
		} else if (map.get("args") != null) {
			throw new IllegalArgumentException("has args that are not a list");
		}
		String serviceAccount = text(map, "serviceAccount");
		if (serviceAccount == null && storageUri.startsWith("s3://")) {
			serviceAccount = DEFAULT_S3_SERVICE_ACCOUNT;
		} else if (serviceAccount != null && !SERVICE_ACCOUNT.matcher(serviceAccount).matches()) {
			throw new IllegalArgumentException("has serviceAccount " + serviceAccount + ", which is not a valid name");
		}
		Long size = number(map, "sizeBytes") == null ? null : number(map, "sizeBytes").longValue();
		Integer contextWindow = number(map, "contextWindow") == null ? null : number(map, "contextWindow").intValue();
		Integer maxTokens = number(map, "maxTokens") == null ? null : number(map, "maxTokens").intValue();
		return new Entry(id, text(map, "displayName") == null ? id : text(map, "displayName"), storageUri,
				serviceAccount, runtime,
				text(map, "modelFormat") == null ? "vllm" : text(map, "modelFormat"),
				List.copyOf(ServedModelTemplate.validateArgs(args, "args")),
				ServedModelTemplate.checkQuantity("cpu", text(map, "cpu") == null ? "2" : text(map, "cpu")),
				ServedModelTemplate.checkQuantity("memory", text(map, "memory") == null ? "4Gi" : text(map, "memory")),
				size, contextWindow, maxTokens, required(map, "licence"), required(map, "origin"),
				required(map, "approvedBy"), text(map, "approvedAt"));
	}

	/**
	 * KServe mounts the whole claim into the model's pod, so the claim must be one
	 * that holds only weights, and the path a folder of it.
	 */
	private static void checkVolume(String storageUri, VolumeUri volume, Set<String> allowedVolumes) {
		if (!allowedVolumes.contains(volume.claim())) {
			throw new IllegalArgumentException("has storageUri " + storageUri + " on volume " + volume.claim()
					+ ", which is not one of " + allowedVolumes + " (" + ServingSettings.KSERVE_WEIGHT_VOLUMES + ")");
		}
		if (volume.path().isEmpty() || !Arrays.stream(volume.path().split("/", -1))
				.allMatch(segment -> PATH_SEGMENT.matcher(segment).matches())) {
			throw new IllegalArgumentException("has storageUri " + storageUri
					+ ", which must name the folder of the weights on the volume, such as pvc://" + volume.claim()
					+ "/<org>/<repo>/<commit>");
		}
	}

	private static Number number(Map<?, ?> map, String key) {
		Object value = map.get(key);
		if (value == null || value instanceof Number) {
			return (Number) value;
		}
		throw new IllegalArgumentException("has " + key + " that is not a number");
	}

	private static String text(Map<?, ?> map, String key) {
		Object value = map.get(key);
		if (value == null) {
			return null;
		}
		String text = value.toString().strip();
		return text.isEmpty() ? null : text;
	}

	/** licence, origin and approvedBy are the model policy's record: an entry without them is not approved */
	private static String required(Map<?, ?> map, String key) {
		String value = text(map, key);
		if (value == null) {
			throw new IllegalArgumentException("has no " + key);
		}
		return value;
	}

	public String getSource() {
		return this.source;
	}

	public List<Entry> getEntries() {
		return this.entries;
	}

	public List<String> getProblems() {
		return this.problems;
	}

	public Entry entry(String id) {
		for (Entry entry : this.entries) {
			if (entry.id().equals(id)) {
				return entry;
			}
		}
		throw new IllegalArgumentException(id + " is not in the serving catalog (" + this.source
				+ "); only models an admin approved there can be defined");
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("source", this.source);
		map.put("models", this.entries.stream().map(Entry::toMap).toList());
		map.put("problems", this.problems);
		return map;
	}
}
