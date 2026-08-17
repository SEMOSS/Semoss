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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.ds.py.PyTranslator;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Runs a project-owned Python implementation for an automation graph node.
 *
 * <p>Python source is deliberately kept outside {@code automation.json}. The graph retains
 * orchestration data while each action node references one file below
 * {@code automation/steps/}. A step must expose {@code run(context, inputs)} and return a
 * JSON-serializable value.
 */
public final class PythonStepNodeExecutor {

	public Object execute(AutomationNodeContext ctx) {
		String stepRef = AutomationExecutionUtils.required(ctx.config(),
				AutomationConstants.CONFIG_STEP_REF, ctx.nodeLabel());
		Path stepPath = resolveStepPath(ctx.projectId(), stepRef);
		if (!Files.isRegularFile(stepPath)) {
			throw new IllegalArgumentException("Python step file does not exist: " + stepRef);
		}

		IProject project = Utility.getProject(ctx.projectId());
		if (project == null) {
			throw new IllegalArgumentException("Automation project was not found: " + ctx.projectId());
		}

		Map<String, Object> context = new LinkedHashMap<>();
		context.put("projectId", ctx.projectId());
		context.put("runId", ctx.runId());
		context.put("nodeId", ctx.nodeId());
		context.put("nodeLabel", ctx.nodeLabel());
		context.put("configKeys", ctx.configMap().keySet());
		Map<String, Object> inputs = resolveInputs(ctx);

		String assetsFolder = AssetUtility.getProjectAssetsFolder(ctx.projectId()).replace('\\', '/');
		String stepFolder = stepPath.getParent().toString().replace('\\', '/');
		String script = buildInvocation(stepPath, context, inputs);
		PyTranslator translator = project.getProjectPyTranslator();
		return translator.runScriptWithExplicitAssetPaths(ctx.insight(), script, assetsFolder,
				new String[] { stepFolder });
	}

	private static Map<String, Object> resolveInputs(AutomationNodeContext ctx) {
		Object rawInputs = ctx.config().get(AutomationConstants.CONFIG_INPUTS);
		if (rawInputs == null) {
			return Map.of();
		}
		if (!(rawInputs instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Python step node \"" + ctx.nodeLabel()
					+ "\": inputs must be an object.");
		}

		Map<String, Object> resolved = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : ((Map<?, ?>) rawInputs).entrySet()) {
			if (!(entry.getKey() instanceof String) || ((String) entry.getKey()).isBlank()) {
				throw new IllegalArgumentException("Python step node \"" + ctx.nodeLabel()
						+ "\": inputs must use nonblank string keys.");
			}
			Object value = entry.getValue();
			resolved.put((String) entry.getKey(), value instanceof String
					? AutomationExecutionUtils.resolve((String) value, ctx.scope(), ctx.configMap())
					: value);
		}
		return resolved;
	}

	private static Path resolveStepPath(String projectId, String stepRef) {
		if (!stepRef.matches("^automation/steps/[A-Za-z0-9][A-Za-z0-9_.-]*\\.py$")) {
			throw new IllegalArgumentException("Python step references must be a .py file directly under "
					+ AutomationConstants.AUTOMATION_STEPS_FOLDER + ".");
		}
		Path assetsFolder = Path.of(AssetUtility.getProjectAssetsFolder(projectId)).toAbsolutePath().normalize();
		Path stepPath = assetsFolder.resolve(stepRef).normalize();
		if (!stepPath.startsWith(assetsFolder)) {
			throw new IllegalArgumentException("Python step file must be inside the automation steps folder.");
		}
		return stepPath;
	}

	private static String buildInvocation(Path stepPath, Map<String, Object> context, Map<String, Object> inputs) {
		String modulePath = AutomationExecutionUtils.GSON.toJson(stepPath.toString().replace('\\', '/'));
		String contextJson = AutomationExecutionUtils.GSON.toJson(AutomationExecutionUtils.GSON.toJson(context));
		String inputsJson = AutomationExecutionUtils.GSON.toJson(AutomationExecutionUtils.GSON.toJson(inputs));
		return """
				import importlib.util
				import json

				_step_path = %s
				_step_spec = importlib.util.spec_from_file_location("semoss_automation_step", _step_path)
				_step_module = importlib.util.module_from_spec(_step_spec)
				_step_spec.loader.exec_module(_step_module)
				_step_result = _step_module.run(json.loads(%s), json.loads(%s))
				_step_result
				""".formatted(modulePath, contextJson, inputsJson);
	}
}
