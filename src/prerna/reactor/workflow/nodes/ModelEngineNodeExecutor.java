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
 * Executes a "model-engine" node: builds and runs the matching model-operation Pixel call
 * ({@code LLM}/{@code Embeddings}/{@code Vision}/{@code NER}) from structured {@code config} on
 * the backend, instead of trusting a frontend-precompiled {@code builtPixel} string (ticket
 * #2743). Reuses the existing {@code LLMReactor}/{@code EmbeddingsReactor}/{@code VisionReactor}/
 * {@code NERReactor} unmodified via the normal Pixel path.
 *
 * <p>Config: {@code {engineId, operation: "llm"|"embeddings"|"vision"|"ner", command, context,
 * paramValues, values, image, prompt, entities}}.
 */
public final class ModelEngineNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String engineId = EngineNodeSupport.required(config, "engineId", "Model-engine", nodeLabel);
		String operation = EngineNodeSupport.optional(config, "operation", "llm");
		String encodedEngineId = EngineNodeSupport.resolveEncoded(engineId, scope, configMap);

		String pixel;
		switch (operation) {
			case "embeddings": {
				String values = EngineNodeSupport.required(config, "values", "Model-engine", nodeLabel);
				pixel = "Embeddings(engine=[" + encodedEngineId +
						"], values=[" + EngineNodeSupport.resolveEncoded(values, scope, configMap) + "]);";
				break;
			}
			case "vision": {
				String command = EngineNodeSupport.required(config, "command", "Model-engine", nodeLabel);
				String image = EngineNodeSupport.required(config, "image", "Model-engine", nodeLabel);
				pixel = "Vision(engine=[" + encodedEngineId +
						"], command=[" + EngineNodeSupport.resolveEncoded(command, scope, configMap) +
						"], image=[" + EngineNodeSupport.resolveEncoded(image, scope, configMap) + "]);";
				break;
			}
			case "ner": {
				String prompt = EngineNodeSupport.required(config, "prompt", "Model-engine", nodeLabel);
				String entities = EngineNodeSupport.required(config, "entities", "Model-engine", nodeLabel);
				pixel = "NER(engine=[" + encodedEngineId +
						"], prompt=[" + EngineNodeSupport.resolveEncoded(prompt, scope, configMap) +
						"], entities=[" + EngineNodeSupport.resolveEncoded(entities, scope, configMap) + "]);";
				break;
			}
			default: {
				// llm
				String command = EngineNodeSupport.required(config, "command", "Model-engine", nodeLabel);
				StringBuilder pixelBuilder = new StringBuilder("LLM(engine=[")
						.append(encodedEngineId)
						.append("], command=[").append(EngineNodeSupport.resolveEncoded(command, scope, configMap)).append("]");
				String context = EngineNodeSupport.optional(config, "context");
				if (context != null) {
					pixelBuilder.append(", context=[").append(EngineNodeSupport.resolveEncoded(context, scope, configMap)).append("]");
				}
				String paramValues = EngineNodeSupport.optional(config, "paramValues");
				if (paramValues != null) {
					// paramValues is a Pixel map literal (e.g. {"key":"value"}), not a string -
					// ReactorKeysEnum.PARAM_VALUES_MAP expects an actual map, so this must stay
					// unquoted/un-encoded to match the FE's existing wire contract (buildPixelPreview
					// emits it the same way). resolveAndValidateJsonLiteral substitutes ${var}
					// refs on this field alone and rejects anything that doesn't resolve to
					// complete, balanced JSON, so a value can't break out of the map literal and
					// inject arbitrary Pixel syntax.
					String resolvedParamValues = EngineNodeSupport.resolveAndValidateJsonLiteral(
							paramValues, scope, configMap, "paramValues", "Model-engine", nodeLabel);
					pixelBuilder.append(", paramValues=[").append(resolvedParamValues).append("]");
				}
				pixelBuilder.append(");");
				pixel = pixelBuilder.toString();
			}
		}

		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(ctx.node());
		return PixelExecutionUtils.runAndCollect(ctx.insight(), pixel, timeoutSeconds);
	}
}
