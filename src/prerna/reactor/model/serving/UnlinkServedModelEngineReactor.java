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
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.reactor.model.serving.ServedModelEngineLinks.ModelEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Removes a model engine's link to a served model (LinkServedModelEngine,
 * CreateServedModelEngine). The engine keeps calling the same endpoint; it just
 * no longer says when the model is stopped.
 *
 * <pre>
 * UnlinkServedModelEngine(engine=["2d5c..."]);
 * </pre>
 */
public class UnlinkServedModelEngineReactor extends AbstractServedModelReactor {

	public UnlinkServedModelEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		ModelEngine engine = ServedModelEngineStore.modelEngine(getEngineId());
		boolean changed = engine.isLinked() && ServedModelEngineStore.link(engine, null, null);
		if (changed) {
			auditEngineLink(user, engine.engineId(), engine.engineName(), engine.namespace(), engine.servedModel(),
					false);
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("engineId", engine.engineId());
		result.put("engineName", engine.engineName());
		result.put("unlinkedFrom", engine.servedModel());
		result.put("changed", changed);
		return result(result);
	}

	@Override
	public String getReactorDescription() {
		return "Removes a model engine's link to a model served by KServe. Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ENGINE.getKey().equals(key)) {
			return "Id of the model engine to unlink";
		}
		return super.getDescriptionForKey(key);
	}
}
