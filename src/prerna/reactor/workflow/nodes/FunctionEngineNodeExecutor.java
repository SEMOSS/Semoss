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
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "function-engine" node: builds and runs an {@code ExecuteFunctionEngine(...)} /
 * {@code ExecuteStreamingFunctionEngine(...)} Pixel call from structured {@code config} on the
 * backend, instead of trusting a frontend-precompiled {@code builtPixel} string (ticket #2743).
 * Reuses the existing {@code ExecuteFunctionEngineReactor}/{@code ExecuteStreamingFunctionEngineReactor}
 * unmodified via the normal Pixel path.
 *
 * <p>Config: {@code {engineId, operation: "default"|"streaming", params (a JSON object string)}}.
 *
 * <p>{@code params} is passed as a quoted, quote-escaped string (matching the frontend's existing
 * {@code buildPixelPreview()} shape exactly, rather than {@code <encode>}-wrapped like other
 * fields here) because the target reactor's {@code map} key expects a Pixel Map noun, and
 * {@code <encode>} would change how the parser types the literal. {@code ${var}} substitution
 * happens on the raw field first (via
 * {@link EngineNodeSupport#resolveAndEscapeForQuotedPixelString}), then the resolved text is
 * quote-escaped before it is embedded - so a substituted value's own quotes can't break out of
 * the surrounding {@code "..."} boundary.
 */
public final class FunctionEngineNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String engineId = EngineNodeSupport.required(config, "engineId", "Function-engine", nodeLabel);
		String operation = EngineNodeSupport.optional(config, "operation");
		String params = EngineNodeSupport.optional(config, "params", "{}");
		String escapedParams = EngineNodeSupport.resolveAndEscapeForQuotedPixelString(params, scope, configMap);

		String command = "streaming".equals(operation) ? "ExecuteStreamingFunctionEngine" : "ExecuteFunctionEngine";
		String pixel = command + "(engine=[" + EngineNodeSupport.resolveEncoded(engineId, scope, configMap) +
				"], map=[\"" + escapedParams + "\"]);";

		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(ctx.node());
		return PixelExecutionUtils.runAndCollect(ctx.insight(), pixel, timeoutSeconds);
	}
}
