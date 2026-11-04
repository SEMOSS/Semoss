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
package prerna.reactor.engine;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.util.Utility;

/**
 * A reactor that reads or writes an engine's guardrail configuration, which
 * only an editor of the engine may do.
 */
public abstract class AbstractEngineGuardrailConfigReactor extends AbstractReactor {

	/**
	 * The engine whose guardrails the user edits: one they can edit, of any type
	 * but a guardrail, since a guardrail engine guarding its own calls would run
	 * itself.
	 *
	 * @param engineId the engine
	 * @return the engine, as {@link Utility#getEngine} returns it
	 * @throws IllegalArgumentException when the user cannot edit the engine, it is
	 *                                  a guardrail engine, or it cannot be loaded
	 */
	protected IEngine loadEditableEngine(String engineId) {
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have edit access to it");
		}
		IEngine.CATALOG_TYPE type = (IEngine.CATALOG_TYPE) SecurityEngineUtils.getEngineTypeAndSubtype(engineId)[0];
		if (type == IEngine.CATALOG_TYPE.GUARDRAIL) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " is a guardrail engine, which cannot have guardrails of its own");
		}
		IEngine engine = Utility.getEngine(engineId, type, true);
		if (engine == null) {
			throw new IllegalArgumentException("Could not load engine " + engineId);
		}
		return engine;
	}
}
