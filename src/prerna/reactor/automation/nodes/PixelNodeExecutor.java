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
import prerna.reactor.automation.AutomationExecutionUtils;
import prerna.reactor.automation.PixelExecutionUtils;

/**
 * Executor for {@code app}-type nodes - runs a node's frontend-precompiled {@code builtPixel}
 * verbatim (after {@code ${var}} substitution). This is the correct behavior for {@code app}
 * nodes because they represent an arbitrary, multi-engine recipe scoped to a project (e.g.
 * {@code LoadApp(project=[...]); IndexPubmedDocuments(database=[..], storage=[..], vector=[..],
 * ...)}) with no single backing engine to dispatch to - unlike {@code database-engine}/
 * {@code model-engine}/{@code vector-engine}/{@code storage-engine}/{@code function-engine}
 * nodes, which read structured {@code config} and call the matching engine/reactor directly.
 */
public final class PixelNodeExecutor implements IAutomationNodeExecutor {

	@Override
	public Object execute(AutomationNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String builtPixel = (String) node.get("builtPixel");
		if (builtPixel == null || builtPixel.isBlank() || builtPixel.startsWith("//")) {
			throw new IllegalStateException("Node \"" + node.get("label") +
					"\" has no compiled pixel - please Save the automation before running");
		}

		int timeoutSeconds = AutomationExecutionUtils.getNodeTimeout(node);
		String resolvedPixel = AutomationExecutionUtils.resolve(builtPixel, scope, configMap);

		return PixelExecutionUtils.runAndCollect(ctx.insight(), resolvedPixel, timeoutSeconds);
	}
}
