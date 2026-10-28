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
package prerna.reactor.model.serving;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserAuditTrailUtils.AuditEvent;
import prerna.util.k8s.KubeApiException;

/**
 * Common ground of the reactors that manage models served by KServe: they are
 * for admins only (a served model takes the box's one GPU), they share the
 * {@link #SERVED_MODEL} key, and they turn Kubernetes errors into messages a
 * user can act on.
 */
public abstract class AbstractServedModelReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractServedModelReactor.class);

	/** The InferenceService name, which is also the model name clients send */
	static final String SERVED_MODEL = "servedModel";

	@Override
	public NounMetadata execute() {
		User user = requireAdmin();
		organizeKeys();
		try {
			return run(user, service());
		} catch (KubeApiException e) {
			classLogger.warn("Kubernetes API call failed for {}: {}", getClass().getSimpleName(), e.getMessage());
			throw new IllegalArgumentException(describe(e), e);
		}
	}

	/** The reactor's work, once the keys are organized and the user is an admin. */
	protected abstract NounMetadata run(User user, ServedModelService service);

	private User requireAdmin() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed in to manage served models",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException error = new SemossPixelException(noun);
			error.setContinueThreadOfExecution(false);
			throw error;
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException(
					"User is not an admin and does not have access to served models. Please login as an admin");
		}
		return user;
	}

	private static ServedModelService service() {
		try {
			return ServedModelService.getInstance();
		} catch (IllegalStateException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	protected String getServedModel() {
		String value = this.keyValue.get(SERVED_MODEL);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must define the " + SERVED_MODEL + " (the InferenceService name)");
		}
		return value.strip();
	}

	/** The id of the SEMOSS engine given by the engine key; refused when absent. */
	protected String getEngineId() {
		String value = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must define the engine (its id)");
		}
		return value.strip();
	}

	/** A map argument given by key; null when absent. */
	@SuppressWarnings("unchecked")
	protected Map<String, Object> getMapArgument(String key) {
		GenRowStruct values = this.store.getGenRowStruct(key);
		if (values != null && !values.isEmpty()) {
			List<NounMetadata> maps = values.getNounsOfType(PixelDataType.MAP);
			if (maps != null && !maps.isEmpty()) {
				return (Map<String, Object>) maps.get(0).getValue();
			}
			throw new IllegalArgumentException(key + " must be a map");
		}
		return null;
	}

	protected static NounMetadata result(Map<String, Object> map) {
		return new NounMetadata(map, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/** Record a successful start or stop; failures are recorded by the pixel runner. */
	protected static void audit(User user, AuditEventType eventType, String namespace, String name) {
		UserAuditTrailUtils.recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS)
				.actorUser(user).target(AuditTargetType.MODEL, namespace + "/" + name, name)
				.details(Map.of("namespace", namespace, "servedModel", name)));
	}

	/** Record that a model engine was linked to a served model, or unlinked from it. */
	protected static void auditEngineLink(User user, String engineId, String engineName, String namespace,
			String servedModel, boolean linked) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("namespace", namespace);
		details.put("servedModel", servedModel);
		details.put("linked", linked);
		UserAuditTrailUtils.recordEvent(new AuditEvent().eventType(AuditEventType.MODEL_SERVING_ENGINE_LINK)
				.status(AuditStatus.SUCCESS).actorUser(user).target(AuditTargetType.ENGINE, engineId, engineName)
				.details(details));
	}

	static String describe(KubeApiException e) {
		if (e.isUnreachable()) {
			return e.getMessage();
		}
		if (e.isUnauthorized()) {
			return "SEMOSS is not allowed to do this in the cluster (HTTP " + e.getCode()
					+ "); check its kubeconfig or ServiceAccount permissions. Kubernetes said: " + e.getMessage();
		}
		return "Kubernetes refused the request (HTTP " + e.getCode()
				+ (e.getReason() == null ? "" : " " + e.getReason()) + "): " + e.getMessage();
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SERVED_MODEL.equals(key)) {
			return "Name of the KServe InferenceService, which is also the model name clients send";
		}
		return super.getDescriptionForKey(key);
	}
}
