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

import prerna.auth.User;
import prerna.engine.impl.model.serving.ServedModelRemoval;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;

/**
 * Removes a served model SEMOSS defined: its InferenceService and, through its
 * owner reference, its Service. Models applied with kubectl (such as the
 * sample) are refused.
 *
 * <pre>
 * RemoveServedModel(servedModel=["olmo-2-1b"]);
 * </pre>
 */
public class RemoveServedModelReactor extends AbstractServedModelReactor {

	public RemoveServedModelReactor() {
		this.keysToGet = new String[] { SERVED_MODEL };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		String name = getServedModel();
		ServedModelRemoval removal = service.remove(name, user);
		audit(user, AuditEventType.MODEL_SERVING_REMOVE, service.getNamespace(), name);
		return result(removal.toMap());
	}

	@Override
	public String getReactorDescription() {
		return "Removes a model SEMOSS defined for KServe, with its Service. Admins only";
	}
}
