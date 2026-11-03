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

import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.k8s.KubeApiException;

/**
 * Defines a served model from a serving-catalog entry: an InferenceService
 * labelled app.kubernetes.io/managed-by=semoss, stopped unless start is true,
 * and its own Service, whose address survives stops and starts.
 *
 * <pre>
 * DefineServedModel(servedModel=["olmo-2-1b"], catalogEntry=["allenai/OLMo-2-0425-1B-Instruct"]);
 * DefineServedModel(servedModel=["olmo-2-1b"], catalogEntry=["allenai/OLMo-2-0425-1B-Instruct"],
 *     overrides=[{"gpuMemoryUtilization": 0.25, "maxModelLen": 2048}], start=[true], createEngine=[true]);
 * DefineServedModel(servedModel=["olmo-2-1b"], catalogEntry=["allenai/OLMo-2-0425-1B-Instruct"], publish=[true]);
 * </pre>
 */
public class DefineServedModelReactor extends AbstractServedModelReactor {

	static final String CATALOG_ENTRY = "catalogEntry";
	static final String OVERRIDES = "overrides";
	static final String START = "start";
	static final String CREATE_ENGINE = "createEngine";
	static final String ENGINE_GLOBAL = "engineGlobal";
	static final String PUBLISH = "publish";
	static final String PUBLISH_PORT = "publishPort";

	public DefineServedModelReactor() {
		this.keysToGet = new String[] { SERVED_MODEL, CATALOG_ENTRY, OVERRIDES, START, CREATE_ENGINE,
				CreateServedModelEngineReactor.ENGINE_NAME, ENGINE_GLOBAL, PUBLISH, PUBLISH_PORT };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		String name = getServedModel();
		String entry = this.keyValue.get(CATALOG_ENTRY);
		Map<String, Object> overrides = getMapArgument(OVERRIDES);
		ServedModelStatus status = service.define(name, entry == null ? null : entry.strip(), overrides,
				Boolean.parseBoolean(this.keyValue.get(START)), Boolean.parseBoolean(this.keyValue.get(PUBLISH)),
				publishPort(), user);
		audit(user, AuditEventType.MODEL_SERVING_DEFINE, service.getNamespace(), name);
		Map<String, Object> result = status.toDetailMap();
		NounMetadata noun = result(result);
		if (Boolean.parseBoolean(this.keyValue.get(CREATE_ENGINE))) {
			try {
				result.put("engine", ServedModelEngines.create(this.insight, service, status,
						this.keyValue.get(CreateServedModelEngineReactor.ENGINE_NAME),
						Boolean.parseBoolean(this.keyValue.get(ENGINE_GLOBAL))).get("engine"));
			} catch (RuntimeException e) {
				// the model stands on its own and its engine can be created later; the
				// pixel succeeds, so audit the engine's failure here
				UserAuditTrailUtils.recordPixelFailure(this.insight, null, "CreateServedModelEngine", e, null);
				String reason = e instanceof KubeApiException kube ? describe(kube) : e.getMessage();
				noun.addAdditionalReturn(NounMetadata.getWarningNounMessage(name + " is defined, but its engine was not "
						+ "created: " + reason + ". Retry with CreateServedModelEngine"));
			}
		}
		return noun;
	}

	/** Pixel numbers arrive as doubles */
	private Integer publishPort() {
		String value = this.keyValue.get(PUBLISH_PORT);
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			double port = Double.parseDouble(value);
			if (port == Math.rint(port)) {
				return (int) port;
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalArgumentException(PUBLISH_PORT + " must be a port number, such as 8001");
	}

	@Override
	public String getReactorDescription() {
		return "Defines a model served by KServe from an approved serving-catalog entry, optionally published on a "
				+ "node port for the LAN. Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
		case CATALOG_ENTRY -> "Id of the serving-catalog entry (ListServingCatalog)";
		case OVERRIDES -> "Optional map: gpuMemoryUtilization, maxModelLen, args (allowed vLLM flags), cpu, memory";
		case START -> "true to start the model at once; default false (defined, stopped)";
		case CREATE_ENGINE -> "true to also create its SEMOSS model engine (see CreateServedModelEngine)";
		case CreateServedModelEngineReactor.ENGINE_NAME -> "The engine's name; default the catalog's display name";
		case ENGINE_GLOBAL -> "true to make the engine public";
		case PUBLISH -> "true to publish its OpenAI API on a node port for the LAN (no authentication yet)";
		case PUBLISH_PORT -> "The node port to publish on; default the lowest free one of KSERVE_HOST_PORT_RANGE";
		default -> super.getDescriptionForKey(key);
		};
	}
}
