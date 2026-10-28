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
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.reactor.model.serving.ServedModelEngineLinks.ModelEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Links a model engine, such as one made by hand, to a model KServe serves, the
 * way CreateServedModelEngine links the engines it creates: when the model is
 * stopped, the engine then says so instead of failing to connect. The engine
 * must call the model, by its MODEL (the served model's name) or its ENDPOINT.
 * The engine needs no restart.
 *
 * <pre>
 * LinkServedModelEngine(servedModel=["olmo-2-1b-instruct"], engine=["2d5c..."]);
 * </pre>
 */
public class LinkServedModelEngineReactor extends AbstractServedModelReactor {

	public LinkServedModelEngineReactor() {
		this.keysToGet = new String[] { SERVED_MODEL, ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		String name = getServedModel();
		String namespace = service.getNamespace();
		ServedModelStatus model = service.status(name);
		ModelEngine engine = ServedModelEngineStore.modelEngine(getEngineId());
		List<String> calls = ServedModelEngineLinks.calls(engine, List.of(model));
		if (calls.isEmpty()) {
			throw new IllegalArgumentException(ServedModelEngineLinks.mismatch(engine, model));
		}
		boolean changed = false;
		if (!name.equals(engine.servedModel()) || !namespace.equals(engine.namespace())) {
			changed = ServedModelEngineStore.link(engine, namespace, name);
			if (changed) {
				auditEngineLink(user, engine.engineId(), engine.engineName(), namespace, name, true);
			}
		}
		Map<String, Object> result = engine.withLink(namespace, name).toMap(calls);
		result.put("changed", changed);
		return result(result);
	}

	@Override
	public String getReactorDescription() {
		return "Links a model engine that calls a model served by KServe to it, so the engine says when the model is "
				+ "stopped. Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ENGINE.getKey().equals(key)) {
			return "Id of the model engine to link; it must call the served model";
		}
		return super.getDescriptionForKey(key);
	}
}
