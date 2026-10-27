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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Why a served model is {@link ServedModelState#FAILED}: the cause, the
 * container it happened in, what Kubernetes reported, and the end of that
 * container's log.
 */
public final class ServedModelFailure {

	public enum Cause {
		/** The storage initializer could not download the weights */
		DOWNLOAD_FAILED,
		/** The storage initializer ran out of memory while downloading */
		DOWNLOAD_OOM,
		/** An image cannot be pulled */
		IMAGE_PULL,
		/** Kubernetes cannot create the container (missing Secret, bad config) */
		CONFIG,
		/** The model server exited with an error */
		MODEL_CRASH,
		/** The model server was killed for lack of memory */
		MODEL_OOM,
		/** The startup probe gave up before the model server answered */
		STARTUP_TIMEOUT,
		/** The pod cannot be scheduled for a reason other than the GPU */
		UNSCHEDULABLE
	}

	private final Cause cause;
	private final String container;
	private final String reason;
	private final Integer exitCode;
	private final int restarts;
	private final boolean previousInstance;
	private String message;
	private String hint;
	private List<String> logTail = List.of();

	ServedModelFailure(Cause cause, String container, String reason, Integer exitCode, int restarts,
			boolean previousInstance, String message) {
		this.cause = cause;
		this.container = container;
		this.reason = reason;
		this.exitCode = exitCode;
		this.restarts = restarts;
		this.previousInstance = previousInstance;
		this.message = message;
	}

	public Cause getCause() {
		return this.cause;
	}

	/** @return the failing container, or null for scheduling failures */
	public String getContainer() {
		return this.container;
	}

	public String getReason() {
		return this.reason;
	}

	public Integer getExitCode() {
		return this.exitCode;
	}

	public int getRestarts() {
		return this.restarts;
	}

	/**
	 * @return whether the failure belongs to the container's previous instance
	 *         (its log is read with {@code previous=true})
	 */
	public boolean isPreviousInstance() {
		return this.previousInstance;
	}

	public String getMessage() {
		return this.message;
	}

	void setMessage(String message) {
		this.message = message;
	}

	public String getHint() {
		return this.hint;
	}

	void setHint(String hint) {
		this.hint = hint;
	}

	public List<String> getLogTail() {
		return this.logTail;
	}

	void setLogTail(List<String> logTail) {
		this.logTail = logTail == null ? List.of() : List.copyOf(logTail);
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("cause", this.cause.name());
		map.put("container", this.container);
		map.put("reason", this.reason);
		map.put("exitCode", this.exitCode);
		map.put("restarts", this.restarts);
		map.put("message", this.message);
		map.put("hint", this.hint);
		map.put("logTail", this.logTail);
		return map;
	}
}
