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
import prerna.engine.impl.model.serving.ServedModelChange;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;

/**
 * Stops a served model and frees the GPU while keeping its definition: the
 * InferenceService's {@code serving.kserve.io/stop} annotation is set, and
 * KServe deletes the predictor's Deployment, pods and Service within seconds.
 * Start it again with StartServedModel.
 *
 * <pre>
 * StopServedModel(servedModel=["olmo-2-1b-instruct"]);
 * </pre>
 */
public class StopServedModelReactor extends AbstractServedModelReactor {

	public StopServedModelReactor() {
		this.keysToGet = new String[] { SERVED_MODEL };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		String name = getServedModel();
		ServedModelChange change = service.stop(name, user);
		Map<String, Object> result = change.status().toDetailMap();
		result.put("changed", change.changed());
		if (change.changed()) {
			audit(user, AuditEventType.MODEL_SERVING_STOP, service.getNamespace(), name);
		}
		return result(result);
	}

	@Override
	public String getReactorDescription() {
		return "Stops a model served by KServe and frees the GPU, keeping its definition. Admins only";
	}
}
