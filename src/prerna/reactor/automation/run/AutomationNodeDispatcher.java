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

import java.util.Map;

import prerna.reactor.automation.AutomationConstants;

/**
 * Selects the established execution owner for one validated Automation node.
 */
final class AutomationNodeDispatcher {

	enum Handler {
		START,
		CONDITION,
		JEV,
		LOOP,
		WAIT,
		APP_PIXEL,
		FUNCTION,
		STORAGE_LIST,
		DATABASE_WRITE,
		VECTOR_WRITE,
		MODEL,
		STORAGE,
		AGENT,
		DATABASE_QUERY,
		VECTOR_SEARCH,
		STORAGE_DOWNLOAD,
		DATA_EXTRACT,
		PYTHON
	}

	private AutomationNodeDispatcher() {
	}

	static Handler selectHandler(AutomationExecutionContract contract, String nodeId,
			Map<String, Object> executionDescriptor) {
		AutomationExecutionContract.Selection selection = contract.selection(nodeId);
		String implementation = selection.implementation();
		if (AutomationExecutionContract.IMPLEMENTATION_START_V1.equals(implementation)) {
			return Handler.START;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_CONDITION_V1.equals(implementation)) {
			return Handler.CONDITION;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_JEV_V1.equals(implementation)) {
			return Handler.JEV;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_LOOP_V1.equals(implementation)) {
			return Handler.LOOP;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_WAIT_V1.equals(implementation)) {
			return Handler.WAIT;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_APP_PIXEL_V1.equals(implementation)) {
			return Handler.APP_PIXEL;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_FUNCTION_V1.equals(implementation)) {
			return Handler.FUNCTION;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_STORAGE_LIST_V1.equals(implementation)) {
			return Handler.STORAGE_LIST;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_DATABASE_WRITE_V1.equals(implementation)) {
			return Handler.DATABASE_WRITE;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_VECTOR_WRITE_V1.equals(implementation)) {
			return Handler.VECTOR_WRITE;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_MODEL_V1.equals(implementation)) {
			return Handler.MODEL;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_STORAGE_V1.equals(implementation)) {
			return Handler.STORAGE;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_AGENT_V1.equals(implementation)) {
			return Handler.AGENT;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_DATABASE_QUERY_V2.equals(implementation)) {
			return Handler.DATABASE_QUERY;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_VECTOR_SEARCH_V1.equals(implementation)) {
			return Handler.VECTOR_SEARCH;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_STORAGE_DOWNLOAD_V1.equals(implementation)) {
			return Handler.STORAGE_DOWNLOAD;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_DATA_EXTRACT_V1.equals(implementation)) {
			return Handler.DATA_EXTRACT;
		}
		if (AutomationExecutionContract.IMPLEMENTATION_PYTHON_TABLE_V2.equals(implementation)) {
			if (!AutomationConstants.NODE_DEVELOPER_PYTHON.equals(
					executionDescriptor.get(AutomationConstants.NODE_FIELD_TYPE))) {
				throw new IllegalStateException(
						"Python execution requires an explicit developer.python node: " + nodeId);
			}
			return Handler.PYTHON;
		}
		throw new IllegalStateException(
				"Automation run node '" + nodeId + "' uses unsupported implementation '" + implementation + "'.");
	}
}
