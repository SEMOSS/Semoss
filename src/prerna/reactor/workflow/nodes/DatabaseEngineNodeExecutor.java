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

import prerna.reactor.workflow.PixelExecutionUtils;
import prerna.reactor.workflow.WorkflowExecutionUtils;

import java.util.Map;

/**
 * Executes a "database-engine" node: builds and runs a {@code SqlQuery(...)} Pixel call from
 * structured {@code config} on the backend, instead of trusting a frontend-precompiled
 * {@code builtPixel} string (ticket #2743). Reuses {@code SqlQueryReactor}/
 * {@code AbstractSqlQueryReactor} unmodified via the normal Pixel path - including its existing
 * SELECT-vs-mutation permission split ({@code userCanViewEngine} for reads,
 * {@code userCanEditEngine} for writes) - so no security logic is duplicated here.
 *
 * <p>Config: {@code {engineId, operation: "read"|"write", expression (the SQL), limit, commit}}.
 */
public final class DatabaseEngineNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();

		String engineId = EngineNodeSupport.required(config, "engineId", "Database-engine", nodeLabel);
		String sql = EngineNodeSupport.required(config, "expression", "Database-engine", nodeLabel);
		String operation = EngineNodeSupport.optional(config, "operation", "read");

		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String encodedEngineId = EngineNodeSupport.resolveEncoded(engineId, scope, configMap);
		String encodedSql = EngineNodeSupport.resolveEncoded(sql, scope, configMap);

		String pixel;
		if ("write".equals(operation)) {
			pixel = "SqlQuery(database=[" + encodedEngineId + "], query=[" + encodedSql + "], commit=[true]);";
		} else {
			int limit = EngineNodeSupport.optionalInt(config, "limit", 50);
			pixel = "SqlQuery(database=[" + encodedEngineId + "], query=[" + encodedSql + "], limit=[" + limit + "]);";
		}

		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(ctx.node());
		return PixelExecutionUtils.runAndCollect(ctx.insight(), pixel, timeoutSeconds);
	}
}
