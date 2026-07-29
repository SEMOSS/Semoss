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

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IStorageEngine;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationExecutionUtils;
import prerna.util.Utility;

public final class StorageEngineNodeExecutor implements IAutomationNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(StorageEngineNodeExecutor.class);

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String engineId = required(config, AutomationConstants.CONFIG_ENGINE_ID, nodeLabel);
		String operation = optional(config, AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_LIST);
		String resolvedEngineId = AutomationExecutionUtils.resolve(engineId, scope, configMap);

		resolvedEngineId = SecurityQueryUtils.testUserEngineIdForAlias(ctx.insight().getUser(), resolvedEngineId);
		boolean mutating = AutomationConstants.OP_UPLOAD.equals(operation)
				|| AutomationConstants.OP_DELETE.equals(operation);
		boolean authorized = mutating
				? SecurityEngineUtils.userCanEditEngine(ctx.insight().getUser(), resolvedEngineId)
				: SecurityEngineUtils.userCanViewEngine(ctx.insight().getUser(), resolvedEngineId);
		if (!authorized) {
			throw new IllegalArgumentException(
					"Storage-engine node \"" + nodeLabel + "\": engine does not exist or user does not have access: " + resolvedEngineId);
		}

		IStorageEngine engine = Utility.getStorage(resolvedEngineId);
		if (engine == null) {
			throw new IllegalArgumentException("Storage-engine node \"" + nodeLabel + "\": engine not found: " + resolvedEngineId);
		}

		classLogger.debug("Storage-engine node \"{}\" executing operation={} via engine {}", nodeLabel, operation, resolvedEngineId);
		switch (operation) {
			case AutomationConstants.OP_DOWNLOAD: {
				String storagePath = required(config, AutomationConstants.CONFIG_STORAGE_PATH, nodeLabel);
				String filePath = required(config, AutomationConstants.CONFIG_FILE_PATH, nodeLabel);
				String resolvedStorage = AutomationExecutionUtils.resolve(storagePath, scope, configMap);
				String resolvedFile = AutomationExecutionUtils.resolve(filePath, scope, configMap);
				engine.copyToLocal(resolvedStorage, resolvedFile);
				return "Downloaded: " + resolvedStorage;
			}
			case AutomationConstants.OP_UPLOAD: {
				String storagePath = required(config, AutomationConstants.CONFIG_STORAGE_PATH, nodeLabel);
				String filePath = required(config, AutomationConstants.CONFIG_FILE_PATH, nodeLabel);
				String resolvedStorage = AutomationExecutionUtils.resolve(storagePath, scope, configMap);
				String resolvedFile = AutomationExecutionUtils.resolve(filePath, scope, configMap);
				engine.copyToStorage(resolvedFile, resolvedStorage, null);
				return "Uploaded: " + resolvedFile;
			}
			case AutomationConstants.OP_DELETE: {
				String storagePath = required(config, AutomationConstants.CONFIG_STORAGE_PATH, nodeLabel);
				String resolvedStorage = AutomationExecutionUtils.resolve(storagePath, scope, configMap);
				engine.deleteFromStorage(resolvedStorage);
				return "Deleted: " + resolvedStorage;
			}
			case AutomationConstants.OP_READ_BASE64: {
				String storagePath = required(config, AutomationConstants.CONFIG_STORAGE_PATH, nodeLabel);
				String resolvedStorage = AutomationExecutionUtils.resolve(storagePath, scope, configMap);
				byte[] bytes = engine.readBlobToMemory(resolvedStorage);
				return Base64.getEncoder().encodeToString(bytes);
			}
			default: {
				// list
				String storagePath = optional(config, AutomationConstants.CONFIG_STORAGE_PATH, AutomationConstants.DEFAULT_STORAGE_PATH);
				String resolvedStorage = AutomationExecutionUtils.resolve(storagePath, scope, configMap);
				List<String> files = engine.list(resolvedStorage);
				return files;
			}
		}
	}

	private static String required(Map<String, Object> config, String key, String nodeLabel) {
		Object v = config.get(key);
		if (v == null || v.toString().isBlank()) {
			throw new IllegalArgumentException("Storage-engine node \"" + nodeLabel + "\": '" + key + "' is required");
		}
		return v.toString();
	}

	private static String optional(Map<String, Object> config, String key, String def) {
		Object v = config.get(key);
		return (v == null || v.toString().isBlank()) ? def : v.toString();
	}
}
