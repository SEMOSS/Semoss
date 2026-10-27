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

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A served model as SEMOSS reports it: identity, lifecycle state and phase, a
 * human-readable message, a rough progress and ETA while starting, and the
 * failure when there is one. {@link #toSummaryMap()} is a list entry and
 * {@link #toDetailMap()} adds the raw KServe status, recent events and the
 * timeline of the last start.
 */
public final class ServedModelStatus {

	String servedModel;
	String namespace;
	boolean managed;
	String displayName;
	String catalogEntry;
	ServedModelState state;
	ServedModelPhase phase;
	String message;
	Double progress;
	Long etaSeconds;
	String etaBasis;
	Instant phaseSince;
	Instant startRequestedAt;
	Map<String, Object> lastAction;
	String storageUri;
	String runtime;
	Double gpuMemoryUtilization;
	String inClusterUrl;
	String endpoint;
	String endpointNote;
	Integer hostPort;
	String pod;
	int restarts;
	ServedModelFailure failure;
	final List<String> warnings = new ArrayList<>();
	String architecture;
	Integer maxModelLen;
	Double weightsGiB;
	Double kvCacheGiB;
	List<Map<String, Object>> conditions = List.of();
	Map<String, Object> modelStatus;
	List<Map<String, Object>> events = List.of();
	List<Map<String, Object>> phases = List.of();
	Instant observedAt;

	ServedModelStatus() {
	}

	public String getServedModel() {
		return this.servedModel;
	}

	public String getNamespace() {
		return this.namespace;
	}

	/** @return whether SEMOSS created this InferenceService (label managed-by=semoss) */
	public boolean isManaged() {
		return this.managed;
	}

	public ServedModelState getState() {
		return this.state;
	}

	/** @return the step of STARTING, or null in any other state */
	public ServedModelPhase getPhase() {
		return this.phase;
	}

	public String getMessage() {
		return this.message;
	}

	void setMessage(String message) {
		this.message = message;
	}

	/** @return 0..1 while STARTING, 1 when READY, otherwise null */
	public Double getProgress() {
		return this.progress;
	}

	public Long getEtaSeconds() {
		return this.etaSeconds;
	}

	/** @return the display name SEMOSS gave the model when it defined it, or null */
	public String getDisplayName() {
		return this.displayName;
	}

	/** @return the serving-catalog entry the model was defined from, or null */
	public String getCatalogEntry() {
		return this.catalogEntry;
	}

	/** @return vLLM's context length, read from its startup log, or null */
	public Integer getMaxModelLen() {
		return this.maxModelLen;
	}

	/** @return the OpenAI base URL at which this SEMOSS reaches the model, or null */
	public String getEndpoint() {
		return this.endpoint;
	}

	/** The node port a LoadBalancer publishes the model on, reachable from the LAN; null when not published */
	public Integer getHostPort() {
		return this.hostPort;
	}

	/** Why there is no endpoint, or why it may not last; null when there is nothing to say */
	public String getEndpointNote() {
		return this.endpointNote;
	}

	public String getPod() {
		return this.pod;
	}

	public ServedModelFailure getFailure() {
		return this.failure;
	}

	public List<String> getWarnings() {
		return this.warnings;
	}

	public Map<String, Object> getLastAction() {
		return this.lastAction;
	}

	public Map<String, Object> toSummaryMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("servedModel", this.servedModel);
		map.put("namespace", this.namespace);
		map.put("managed", this.managed);
		map.put("displayName", this.displayName);
		map.put("catalogEntry", this.catalogEntry);
		map.put("state", this.state == null ? null : this.state.name());
		map.put("phase", this.phase == null ? null : this.phase.name());
		map.put("message", this.message);
		map.put("progress", this.progress);
		map.put("etaSeconds", this.etaSeconds);
		map.put("etaBasis", this.etaBasis);
		map.put("phaseSince", format(this.phaseSince));
		map.put("startRequestedAt", format(this.startRequestedAt));
		map.put("lastAction", this.lastAction);
		map.put("storageUri", this.storageUri);
		map.put("runtime", this.runtime);
		map.put("gpuMemoryUtilization", this.gpuMemoryUtilization);
		map.put("inClusterUrl", this.inClusterUrl);
		map.put("endpoint", this.endpoint);
		map.put("endpointNote", this.endpointNote);
		map.put("hostPort", this.hostPort);
		map.put("pod", this.pod);
		map.put("restarts", this.restarts);
		map.put("failure", this.failure == null ? null : this.failure.toMap());
		map.put("warnings", List.copyOf(this.warnings));
		map.put("observedAt", format(this.observedAt));
		return map;
	}

	public Map<String, Object> toDetailMap() {
		Map<String, Object> map = toSummaryMap();
		Map<String, Object> vllm = new LinkedHashMap<>();
		vllm.put("architecture", this.architecture);
		vllm.put("maxModelLen", this.maxModelLen);
		vllm.put("weightsGiB", this.weightsGiB);
		vllm.put("kvCacheGiB", this.kvCacheGiB);
		map.put("vllm", vllm);
		map.put("phases", this.phases);
		map.put("conditions", this.conditions);
		map.put("modelStatus", this.modelStatus);
		map.put("events", this.events);
		return map;
	}

	static String format(Instant instant) {
		return instant == null ? null : instant.toString();
	}
}
