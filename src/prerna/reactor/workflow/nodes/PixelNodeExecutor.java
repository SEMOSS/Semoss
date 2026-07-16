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
package prerna.reactor.workflow.nodes;

import java.util.Map;

import prerna.reactor.workflow.PixelExecutionUtils;
import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Default/fallback executor - runs a node's frontend-precompiled {@code builtPixel} verbatim
 * (after {@code ${var}} substitution). This is the correct behavior for node types that are
 * genuinely arbitrary or composed Pixel with no single backing engine:
 * <ul>
 *   <li>{@code trigger} - returns the run's trigger timestamp, no Pixel execution</li>
 *   <li>{@code app} - runs an arbitrary multi-engine recipe scoped to a project
 *       (e.g. {@code IndexPubmedDocuments(database=[..], storage=[..], vector=[..], ...)})</li>
 *   <li>{@code custom-pixel} - arbitrary user-authored Pixel, optionally scoped to an app via
 *       a leading {@code LoadApp(...)} setup call</li>
 * </ul>
 *
 * <p>Unlike the previous {@code executeNodePixel}, this is <em>not</em> used for
 * {@code database-engine}/{@code model-engine}/{@code vector-engine}/{@code storage-engine}/
 * {@code function-engine} nodes - those have their own dedicated executors that read structured
 * {@code config} and call the matching engine/reactor directly, rather than trusting a
 * frontend-precompiled Pixel string (see ticket #2743).
 */
public final class PixelNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String type = ctx.nodeType();

		if (WorkflowConstants.NODE_TRIGGER.equals(type)) {
			return scope.get("triggered_at");
		}

		String builtPixel = (String) node.get("builtPixel");
		if (builtPixel == null || builtPixel.isBlank() || builtPixel.startsWith("//")) {
			throw new IllegalStateException("Node \"" + node.get("label") +
					"\" has no compiled pixel - please Save the workflow before running");
		}

		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(node);
		String resolvedPixel = WorkflowExecutionUtils.resolve(builtPixel, scope, configMap);

		// For custom-pixel nodes with an appId, the builtPixel is "LoadApp(...); actualPixel".
		// Run LoadApp as a fire-and-forget setup step so only the actual pixel's output
		// is captured and stored as the node's result.
		if (WorkflowConstants.NODE_CUSTOM_PIXEL.equals(type)) {
			Map<String, Object> config = (Map<String, Object>) node.get("config");
			Object appIdObj = config != null ? config.get("appId") : null;
			if (appIdObj != null && !appIdObj.toString().isBlank()) {
				int semicolon = resolvedPixel.indexOf(';');
				if (semicolon > 0) {
					String setupPixel = resolvedPixel.substring(0, semicolon).trim();
					String actualPixel = resolvedPixel.substring(semicolon + 1).trim();
					if (!setupPixel.isBlank() && !actualPixel.isBlank()) {
						// SEMOSS pixel parser requires a trailing semicolon on every statement
						if (!setupPixel.endsWith(";")) setupPixel += ";";
						if (!actualPixel.endsWith(";")) actualPixel += ";";
						ctx.insight().runPixel(setupPixel); // set context, discard output
						return PixelExecutionUtils.runAndCollect(ctx.insight(), actualPixel, timeoutSeconds);
					}
				}
			}
		}

		return PixelExecutionUtils.runAndCollect(ctx.insight(), resolvedPixel, timeoutSeconds);
	}
}
