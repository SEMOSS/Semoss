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
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates the SEMOSS model engine of a served model (one SEMOSS defined, or one
 * applied with kubectl such as the sample): a Self Hosted, OpenAI-compatible
 * engine pointed at the endpoint this SEMOSS reaches the model by. The model
 * does not have to be running.
 *
 * <pre>
 * CreateServedModelEngine(servedModel=["olmo-2-1b-instruct"]);
 * CreateServedModelEngine(servedModel=["olmo-2-1b"], engineName=["OLMo on the DGX"], global=[true]);
 * </pre>
 */
public class CreateServedModelEngineReactor extends AbstractServedModelReactor {

	static final String ENGINE_NAME = "engineName";

	public CreateServedModelEngineReactor() {
		this.keysToGet = new String[] { SERVED_MODEL, ENGINE_NAME, ReactorKeysEnum.GLOBAL.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		return result(ServedModelEngines.create(this.insight, service, service.status(getServedModel()),
				this.keyValue.get(ENGINE_NAME), Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey()))));
	}

	@Override
	public String getReactorDescription() {
		return "Creates the SEMOSS model engine of a model served by KServe, at this SEMOSS's endpoint for it. "
				+ "Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ENGINE_NAME.equals(key)) {
			return "Optional engine name; default the model's display name, else its name";
		}
		return super.getDescriptionForKey(key);
	}
}
