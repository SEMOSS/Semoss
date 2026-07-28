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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationExecutionUtils;
import prerna.reactor.automation.PixelExecutionUtils;
import prerna.util.Utility;

/**
 * Executor for {@code app}-type nodes. Runs an arbitrary pixel expression, optionally
 * inside a specific app's project context.
 *
 * <p>Config fields (from {@code node.config}):
 * <ul>
 *   <li>{@code pixel} (required) — the pixel expression to run; supports {@code ${var}} substitution</li>
 *   <li>{@code appId} (optional) — if set, the pixel runs inside this project's insight context</li>
 * </ul>
 *
 * <p>When {@code appId} is provided the project context overrides are set on {@link ThreadStore}
 * before execution. {@link PixelExecutionUtils#runAndCollect} snapshots the caller's thread
 * context (including these overrides) before submitting to the timeout thread, so the context
 * propagates correctly even under timeout enforcement.
 */
public final class AppEngineNodeExecutor implements IAutomationNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(AppEngineNodeExecutor.class);

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String pixel = required(config, "pixel", nodeLabel);
		String appId = optional(config, "appId");
		String resolvedPixel = AutomationExecutionUtils.resolve(pixel, scope, configMap);
		String resolvedAppId = appId != null ? AutomationExecutionUtils.resolve(appId, scope, configMap) : null;

		classLogger.debug("App-engine node \"{}\" executing pixel in appId={}", nodeLabel, resolvedAppId != null ? resolvedAppId : "caller context");

		if (resolvedAppId != null && !resolvedAppId.isBlank()) {
			IProject project = Utility.getProject(resolvedAppId);
			if (project == null) {
				throw new IllegalArgumentException(
						"App node \"" + nodeLabel + "\": project not found: " + resolvedAppId);
			}
			ThreadStore.setContextProjectIdOverride(resolvedAppId);
			ThreadStore.setContextProjectNameOverride(project.getProjectName());
			try {
				return PixelExecutionUtils.runAndCollect(ctx.insight(), resolvedPixel);
			} finally {
				ThreadStore.clearContextProjectOverride();
			}
		}

		return PixelExecutionUtils.runAndCollect(ctx.insight(), resolvedPixel);
	}

	private static String required(Map<String, Object> config, String key, String nodeLabel) {
		Object v = config.get(key);
		if (v == null || v.toString().isBlank()) {
			throw new IllegalArgumentException(
					"App node \"" + nodeLabel + "\": '" + key + "' is required");
		}
		return v.toString();
	}

	private static String optional(Map<String, Object> config, String key) {
		Object v = config.get(key);
		return (v == null || v.toString().isBlank()) ? null : v.toString();
	}
}
