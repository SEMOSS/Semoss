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

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;

/**
 * Explains a failed ask to the engine of a served model, one whose properties
 * name it in {@value #SERVED_MODEL_PROPERTY} as CreateServedModelEngine writes
 * them. When the model is not running, the error says so, and how far a start
 * has come, instead of the client's "Connection error". Only a failed ask pays
 * for the look-up, and every engine without that property is left alone.
 */
public final class ServedModelGuard {

	private static final Logger classLogger = LogManager.getLogger(ServedModelGuard.class);

	/** The engine properties CreateServedModelEngine writes: the served model the engine talks to */
	public static final String SERVED_MODEL_PROPERTY = "KSERVE_SERVED_MODEL";
	public static final String NAMESPACE_PROPERTY = "KSERVE_NAMESPACE";
	/** The reason an explained error carries, so a UI can offer to start the model */
	public static final String NOT_RUNNING = "SERVED_MODEL_NOT_RUNNING";
	static final String ERROR_TYPE = "ServedModelNotRunning";
	private static final String ENDPOINT_PROPERTY = "ENDPOINT";
	private static final String NO_SUCH_MODEL = "No served model named ";

	/** Reads a served model's status; tests replace it */
	interface Lookup {
		/**
		 * @return the status, or null when this SEMOSS cannot tell
		 * @throws IllegalArgumentException when no such model is served
		 */
		ServedModelStatus status(String namespace, String name);
	}

	private ServedModelGuard() {
	}

	/**
	 * @param engine the engine's properties
	 * @return the error, explained when the engine's served model is not running
	 *         or not where the engine calls it; otherwise the error as it was
	 */
	public static AskErrorModelEngineResponse explain(Properties engine, AskErrorModelEngineResponse error) {
		if (engine == null || engine.getProperty(SERVED_MODEL_PROPERTY) == null) {
			return error;
		}
		return explain(engine, error, ServedModelGuard::lookUp);
	}

	static AskErrorModelEngineResponse explain(Properties engine, AskErrorModelEngineResponse error, Lookup lookup) {
		String name = engine.getProperty(SERVED_MODEL_PROPERTY);
		// an error with a reason (a context overflow, a usage limit) is about the call, not the model
		if (name == null || (error.getReason() != null && !error.getReason().isBlank())) {
			return error;
		}
		String namespace = engine.getProperty(NAMESPACE_PROPERTY);
		String why;
		String state;
		try {
			ServedModelStatus status = lookup.status(namespace, name);
			if (status == null) {
				return error;
			}
			why = whyNot(status, engine.getProperty(ENDPOINT_PROPERTY));
			state = status.getState().name();
		} catch (IllegalArgumentException e) {
			if (e.getMessage() == null || !e.getMessage().startsWith(NO_SUCH_MODEL)) {
				return error;
			}
			why = name + " is no longer served" + (namespace == null ? "" : " in namespace " + namespace)
					+ "; an admin can define it again, or delete this engine.";
			state = "GONE";
		}
		if (why == null) {
			return error;
		}
		AskErrorModelEngineResponse explained = new AskErrorModelEngineResponse(
				why + " The client's error was: " + error.getStringResponse(), ERROR_TYPE, error.getCode(),
				error.getClient(), error.getModel(), error.getTraceback());
		explained.setReason(NOT_RUNNING);
		explained.setReasonDetail(state);
		return explained;
	}

	/**
	 * Why a model's engine cannot reach it, or null when it runs where the engine
	 * calls it, so that the error is the model's own.
	 */
	static String whyNot(ServedModelStatus status, String engineEndpoint) {
		String name = status.getServedModel();
		return switch (status.getState()) {
		case READY -> {
			String unreachable = status.getWarnings().stream()
					.filter(warning -> warning.startsWith("SEMOSS cannot reach the model")).findFirst().orElse(null);
			if (unreachable != null) {
				yield name + " is running, but " + unreachable + ".";
			}
			if (engineEndpoint != null && status.getEndpoint() != null
					&& !trimmed(engineEndpoint).equals(trimmed(status.getEndpoint()))) {
				yield name + " runs at " + status.getEndpoint() + ", but this engine calls " + engineEndpoint
						+ "; an admin can change the engine's ENDPOINT.";
			}
			yield null;
		}
		case STOPPED -> name + " is stopped. An admin can start it with StartServedModel(servedModel=[\"" + name
				+ "\"]).";
		case STOPPING -> name + " is being stopped.";
		case STARTING -> name + " is still starting: " + status.getMessage() + progress(status) + ".";
		case WAITING_FOR_GPU -> name + " cannot start yet: " + status.getMessage() + ".";
		case FAILED -> name + " has failed: " + status.getMessage()
				+ ". GetServedModelStatus shows why, and StartServedModel starts it again.";
		case REMOVING -> name + " is being removed.";
		};
	}

	private static String progress(ServedModelStatus status) {
		if (status.getProgress() == null) {
			return "";
		}
		long percent = Math.round(status.getProgress() * 100);
		return " (" + percent + "%" + (status.getEtaSeconds() == null ? ""
				: ", about " + ServedModelStateResolver.duration(status.getEtaSeconds()) + " left") + ")";
	}

	private static String trimmed(String endpoint) {
		return endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
	}

	/** Null when serving is off on this SEMOSS, the model is in another namespace, or the cluster cannot be read */
	private static ServedModelStatus lookUp(String namespace, String name) {
		if (!ServingSettings.isEnabled()) {
			return null;
		}
		try {
			ServedModelService service = ServedModelService.getInstance();
			if (namespace != null && !namespace.equals(service.getNamespace())) {
				return null;
			}
			return service.status(name);
		} catch (IllegalArgumentException e) {
			if (e.getMessage() != null && e.getMessage().startsWith(NO_SUCH_MODEL)) {
				throw e;
			}
			classLogger.warn("Cannot read served model {} to explain a failed ask: {}", name, e.getMessage());
			return null;
		} catch (RuntimeException e) {
			classLogger.warn("Cannot read served model {} to explain a failed ask: {}", name, e.getMessage());
			return null;
		}
	}
}
