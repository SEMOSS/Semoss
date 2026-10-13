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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;

public class AutomationNodeDispatcherUnitTests {

	@Test
	void selectsJavaControlHandlersByCanonicalType() {
		assertEquals(AutomationNodeDispatcher.Handler.START,
				handlerFor(AutomationConstants.NODE_START));
		assertEquals(AutomationNodeDispatcher.Handler.CONDITION,
				handlerFor(AutomationConstants.NODE_CONTROL_IF));
		assertEquals(AutomationNodeDispatcher.Handler.JEV,
				handlerFor(AutomationConstants.NODE_CONTROL_JEV));
		assertEquals(AutomationNodeDispatcher.Handler.LOOP,
				handlerFor(AutomationConstants.NODE_CONTROL_LOOP));
	}

	@Test
	void selectsTypedAndExplicitPythonHandlers() {
		assertEquals(AutomationNodeDispatcher.Handler.FUNCTION,
				handlerFor(AutomationConstants.NODE_FUNCTION_EXECUTE));
		assertEquals(AutomationNodeDispatcher.Handler.PYTHON,
				handlerFor(AutomationConstants.NODE_DEVELOPER_PYTHON));
	}

	private static AutomationNodeDispatcher.Handler handlerFor(String type) {
		String nodeId = "node";
		Map<String, Object> node = Map.of(
				AutomationConstants.NODE_FIELD_ID, nodeId,
				AutomationConstants.NODE_FIELD_TYPE, type,
				AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		var definition = new AutomationDefinitionValidator.ValidatedDefinition(
				Map.of(), List.of(node), List.of(), "{}", "hash");
		Map<String, String> sources = AutomationConstants.NODE_DEVELOPER_PYTHON.equals(type)
				? Map.of(nodeId, "def run(scope):\n    return {}\n")
				: Map.of();
		AutomationExecutionContract contract = AutomationExecutionContract.forSubmission(definition, sources);
		return AutomationNodeDispatcher.selectHandler(contract, nodeId,
				contract.executionDescriptor(nodeId, node));
	}
}
