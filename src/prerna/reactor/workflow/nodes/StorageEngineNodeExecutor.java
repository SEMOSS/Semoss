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
 * Executes a "storage-engine" node: builds and runs the matching storage-operation Pixel call
 * from structured {@code config} on the backend, instead of trusting a frontend-precompiled
 * {@code builtPixel} string (ticket #2743). Reuses the existing
 * {@code ListStoragePathReactor}/{@code PullFromStorageReactor}/{@code PushToStorageReactor}/
 * {@code DeleteFromStorageReactor}/{@code GetStorageFileAsBase64Reactor} unmodified via the
 * normal Pixel path.
 *
 * <p>Config: {@code {engineId, operation: "list"|"download"|"upload"|"delete"|"read-base64",
 * storagePath, filePath, metadata}}.
 */
public final class StorageEngineNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String engineId = EngineNodeSupport.required(config, "engineId", "Storage-engine", nodeLabel);
		String operation = EngineNodeSupport.optional(config, "operation", "list");
		String encodedEngineId = EngineNodeSupport.resolveEncoded(engineId, scope, configMap);

		String pixel;
		switch (operation) {
			case "download": {
				String storagePath = EngineNodeSupport.required(config, "storagePath", "Storage-engine", nodeLabel);
				String filePath = EngineNodeSupport.required(config, "filePath", "Storage-engine", nodeLabel);
				pixel = "PullFromStorage(storage=[" + encodedEngineId +
						"], storagePath=[" + EngineNodeSupport.resolveEncoded(storagePath, scope, configMap) +
						"], filePath=[" + EngineNodeSupport.resolveEncoded(filePath, scope, configMap) + "]);";
				break;
			}
			case "upload": {
				String storagePath = EngineNodeSupport.required(config, "storagePath", "Storage-engine", nodeLabel);
				String filePath = EngineNodeSupport.required(config, "filePath", "Storage-engine", nodeLabel);
				StringBuilder pixelBuilder = new StringBuilder("PushToStorage(storage=[")
						.append(encodedEngineId)
						.append("], storagePath=[").append(EngineNodeSupport.resolveEncoded(storagePath, scope, configMap))
						.append("], filePath=[").append(EngineNodeSupport.resolveEncoded(filePath, scope, configMap)).append("]");
				String metadata = EngineNodeSupport.optional(config, "metadata");
				if (metadata != null) {
					// Pixel map literal, not a string - see ModelEngineNodeExecutor's paramValues
					// handling for the same resolve-then-validate treatment.
					String resolvedMetadata = EngineNodeSupport.resolveAndValidateJsonLiteral(
							metadata, scope, configMap, "metadata", "Storage-engine", nodeLabel);
					pixelBuilder.append(", metadata=[").append(resolvedMetadata).append("]");
				}
				pixelBuilder.append(");");
				pixel = pixelBuilder.toString();
				break;
			}
			case "delete": {
				String storagePath = EngineNodeSupport.required(config, "storagePath", "Storage-engine", nodeLabel);
				pixel = "DeleteFromStorage(storage=[" + encodedEngineId +
						"], storagePath=[" + EngineNodeSupport.resolveEncoded(storagePath, scope, configMap) + "]);";
				break;
			}
			case "read-base64": {
				String storagePath = EngineNodeSupport.required(config, "storagePath", "Storage-engine", nodeLabel);
				pixel = "GetStorageFileAsBase64(storage=[" + encodedEngineId +
						"], storagePath=[" + EngineNodeSupport.resolveEncoded(storagePath, scope, configMap) + "]);";
				break;
			}
			default: {
				// list
				String storagePath = EngineNodeSupport.optional(config, "storagePath", "/");
				pixel = "ListStoragePath(storage=[" + encodedEngineId +
						"], storagePath=[" + EngineNodeSupport.resolveEncoded(storagePath, scope, configMap) + "]);";
			}
		}

		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(ctx.node());
		return PixelExecutionUtils.runAndCollect(ctx.insight(), pixel, timeoutSeconds);
	}
}
