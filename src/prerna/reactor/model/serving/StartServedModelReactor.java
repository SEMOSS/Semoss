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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.serving.ServedModelChange;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelState;
import prerna.engine.impl.model.serving.ServingCapacityException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;

/**
 * Starts a defined (stopped) served model by lifting its InferenceService's
 * {@code serving.kserve.io/stop} annotation, once the GPUs and the memory budget
 * allow it (GetServingCapacity shows the same check). Otherwise it refuses and
 * names the model in the way; {@code replace} stops that model first. A model
 * that failed is stopped and started again. Returns once the model is STARTING;
 * follow it with GetServedModelStatus (about two to four minutes on this box with
 * the image cached, longer the first time).
 *
 * <pre>
 * StartServedModel(servedModel=["olmo-2-1b-instruct"]);
 * StartServedModel(servedModel=["olmo-2-1b"], replace=[true]);
 * </pre>
 */
public class StartServedModelReactor extends AbstractServedModelReactor {

	static final String REPLACE = "replace";

	public StartServedModelReactor() {
		this.keysToGet = new String[] { SERVED_MODEL, REPLACE };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		String name = getServedModel();
		ServedModelChange change;
		try {
			change = service.start(name, user, Boolean.parseBoolean(this.keyValue.get(REPLACE)));
		} catch (ServingCapacityException e) {
			// the models stopped to make room stay stopped
			auditStops(user, service, e.getReplaced());
			throw e;
		}
		auditStops(user, service, change.replaced());
		Map<String, Object> result = change.status().toDetailMap();
		result.put("changed", change.changed());
		if (!change.replaced().isEmpty()) {
			result.put("replaced", change.replaced());
		}
		if (change.restarted()) {
			result.put("restarted", true);
		}
		NounMetadata noun = result(result);
		if (change.changed()) {
			audit(user, AuditEventType.MODEL_SERVING_START, service.getNamespace(), name);
		} else if (change.status().getState() == ServedModelState.WAITING_FOR_GPU && change.replaced().isEmpty()) {
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage(name + " is waiting for the GPU; "
					+ "StartServedModel with replace=[true] stops what holds it"));
		}
		return noun;
	}

	private static void auditStops(User user, ServedModelService service, List<String> stopped) {
		for (String model : stopped) {
			audit(user, AuditEventType.MODEL_SERVING_STOP, service.getNamespace(), model);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Starts a stopped model served by KServe when the GPU and memory budget allow it; returns at once "
				+ "while it starts. Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (REPLACE.equals(key)) {
			return "true to stop the models in its way first; default false, which refuses and names them";
		}
		return super.getDescriptionForKey(key);
	}
}
