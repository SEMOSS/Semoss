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
 * Executes a "vector-engine" node: builds and runs the matching vector-operation Pixel call from
 * structured {@code config} on the backend, instead of trusting a frontend-precompiled
 * {@code builtPixel} string (ticket #2743). Reuses the existing
 * {@code VectorDatabaseQueryReactor}/{@code VectorAttachFileToSourceReactor}/
 * {@code CreateEmbeddingsFromVectorCSVFileReactor}/{@code ListDocumentsInVectorDatabaseReactor}/
 * {@code RemoveDocumentFromVectorDatabaseReactor}/{@code VectorFileDownloadReactor} unmodified via
 * the normal Pixel path - including {@code CreateEmbeddingsFromVectorCSVFileReactor}'s
 * substantial (~385 line) CSV-parsing/chunking logic, which this deliberately does not
 * re-implement.
 *
 * <p>Config: {@code {engineId, operation: "search"|"add-file"|"add-csv"|"list"|"delete"|
 * "download", command, limit, filePath, source, space, filePaths, paramValues, fileNames}}.
 */
public final class VectorEngineNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String engineId = EngineNodeSupport.required(config, "engineId", "Vector-engine", nodeLabel);
		String operation = EngineNodeSupport.optional(config, "operation", "search");
		String encodedEngineId = EngineNodeSupport.resolveEncoded(engineId, scope, configMap);

		String pixel;
		switch (operation) {
			case "add-file": {
				String filePath = EngineNodeSupport.required(config, "filePath", "Vector-engine", nodeLabel);
				StringBuilder pixelBuilder = new StringBuilder("VectorAttachFileToSource(engine=[")
						.append(encodedEngineId)
						.append("], filePath=[").append(EngineNodeSupport.resolveEncoded(filePath, scope, configMap)).append("]");
				String source = EngineNodeSupport.optional(config, "source");
				if (source != null) pixelBuilder.append(", source=[").append(EngineNodeSupport.resolveEncoded(source, scope, configMap)).append("]");
				String space = EngineNodeSupport.optional(config, "space");
				if (space != null) pixelBuilder.append(", space=[").append(EngineNodeSupport.resolveEncoded(space, scope, configMap)).append("]");
				pixelBuilder.append(");");
				pixel = pixelBuilder.toString();
				break;
			}
			case "add-csv": {
				String filePaths = EngineNodeSupport.required(config, "filePaths", "Vector-engine", nodeLabel);
				StringBuilder pixelBuilder = new StringBuilder("CreateEmbeddingsFromVectorCSVFile(engine=[")
						.append(encodedEngineId)
						.append("], filePaths=[").append(EngineNodeSupport.resolveEncoded(filePaths, scope, configMap)).append("]");
				String paramValues = EngineNodeSupport.optional(config, "paramValues");
				if (paramValues != null) {
					// Pixel map literal, not a string - see ModelEngineNodeExecutor's paramValues
					// handling for the same resolve-then-validate treatment.
					String resolvedParamValues = EngineNodeSupport.resolveAndValidateJsonLiteral(
							paramValues, scope, configMap, "paramValues", "Vector-engine", nodeLabel);
					pixelBuilder.append(", paramValues=[").append(resolvedParamValues).append("]");
				}
				pixelBuilder.append(");");
				pixel = pixelBuilder.toString();
				break;
			}
			case "list":
				pixel = "ListDocumentsInVectorDatabase(engine=[" + encodedEngineId + "]);";
				break;
			case "delete": {
				String fileNames = EngineNodeSupport.required(config, "fileNames", "Vector-engine", nodeLabel);
				pixel = "RemoveDocumentFromVectorDatabase(engine=[" + encodedEngineId +
						"], fileNames=[" + EngineNodeSupport.resolveEncoded(fileNames, scope, configMap) + "]);";
				break;
			}
			case "download": {
				String fileNames = EngineNodeSupport.required(config, "fileNames", "Vector-engine", nodeLabel);
				pixel = "VectorFileDownload(engine=[" + encodedEngineId +
						"], fileNames=[" + EngineNodeSupport.resolveEncoded(fileNames, scope, configMap) + "]);";
				break;
			}
			default: {
				// search
				String command = EngineNodeSupport.required(config, "command", "Vector-engine", nodeLabel);
				int limit = EngineNodeSupport.optionalInt(config, "limit", 5);
				pixel = "VectorDatabaseQuery(engine=[" + encodedEngineId +
						"], command=[" + EngineNodeSupport.resolveEncoded(command, scope, configMap) + "], limit=[" + limit + "]);";
			}
		}

		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(ctx.node());
		return PixelExecutionUtils.runAndCollect(ctx.insight(), pixel, timeoutSeconds);
	}
}
