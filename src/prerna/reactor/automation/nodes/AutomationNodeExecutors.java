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
package prerna.reactor.automation.nodes;

import java.util.Map;

import prerna.reactor.automation.AutomationConstants;

/**
 * Shared registry of stateless node executor instances.
 * Used by both TriggerAutomationReactor and RunAutomationNodeReactor.
 */
public final class AutomationNodeExecutors {

	private AutomationNodeExecutors() {}

	public static final Map<String, IAutomationNodeExecutor> EXECUTORS = Map.of(
			AutomationConstants.NODE_WAIT, new WaitNodeExecutor(),
			AutomationConstants.NODE_DATABASE_ENGINE, new DatabaseEngineNodeExecutor(),
			AutomationConstants.NODE_MODEL_ENGINE, new ModelEngineNodeExecutor(),
			AutomationConstants.NODE_VECTOR_ENGINE, new VectorEngineNodeExecutor(),
			AutomationConstants.NODE_STORAGE_ENGINE, new StorageEngineNodeExecutor(),
			AutomationConstants.NODE_FUNCTION_ENGINE, new FunctionEngineNodeExecutor()
	);
}
