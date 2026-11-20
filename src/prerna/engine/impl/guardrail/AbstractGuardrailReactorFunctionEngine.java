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
package prerna.engine.impl.guardrail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.function.AbstractReactorFunctionEngine;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public abstract class AbstractGuardrailReactorFunctionEngine extends AbstractReactorFunctionEngine
		implements IGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractGuardrailReactorFunctionEngine.class);

	protected static final String FILE_SEPARATOR = "/";

	protected String engineBaseFolder = null;
	protected String engineAppRootFolder = null;
	protected String engineVersionFolder = null;
	protected String engineAssetsFolder = null;

	protected boolean isMCPEnabled = false;

	/**
	 * Init the general smss values
	 * 
	 * @param builder
	 * @throws Exception
	 */
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}

	/**
	 * Init the general smss values
	 * 
	 * @param builder
	 * @throws Exception
	 */
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		// this is because of some silly stuff on databases
		if (this.smssProp.isEmpty()) {
			return;
		}

		// not basic, so normal flow
		this.engineId = smssProp.getProperty(Constants.ENGINE);
		this.engineName = smssProp.getProperty(Constants.ENGINE_ALIAS);

		String engineIdAndName = SmssUtilities.getUniqueName(engineName, engineId);

		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if (secretStore != null) {
			Map<String, Object> engineSecrets = secretStore.getEngineSecrets(getCatalogType(), this.engineId,
					this.engineName);
			if (engineSecrets == null || engineSecrets.isEmpty()) {
				classLogger.info("No secrets found for {}", engineIdAndName);
			} else {
				classLogger.info("Successfully pulled secrets for {}", engineIdAndName);
				this.smssProp.putAll(engineSecrets);
			}
		}

		IEngine.CATALOG_TYPE eType = getCatalogType();
		this.engineBaseFolder = EngineUtility.getSpecificEngineBaseFolder(eType, engineIdAndName);
		this.engineAppRootFolder = EngineUtility.getSpecificEngineAppRootFolder(eType, engineIdAndName);
		this.engineVersionFolder = EngineUtility.getSpecificEngineVersionFolder(eType, engineIdAndName);
		this.engineAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(eType, engineIdAndName);

		// make sure we always have an assets folder and all the directories leading up
		// to it
		{
			File f = new File(this.engineAssetsFolder);
			if (!f.exists() || !f.isDirectory()) {
				f.mkdirs();
				// this means you have a legacy structure
				// i will move everything you have into the assets folder
				// with exception of .mv.db files
				Path assetsPath = Path.of(this.engineAssetsFolder);
				try (Stream<Path> stream = Files.list(Path.of(this.engineBaseFolder))) {
					stream.forEach(item -> {
						// skip if the item is already within app_root or app_root/versions
						// this would really only be for the engine image
						String fileName = item.getFileName().toString();
						if (item.toString().replace("\\", "/").contains("/" + Constants.APP_ROOT_FOLDER + "/")
								|| fileName.equals(Constants.APP_ROOT_FOLDER)) {
							return; // skip
						}

						if (!fileName.endsWith(".mv.db") && !fileName.endsWith(".jnl")
								&& !fileName.endsWith(".sqlite")) {
							try {
								Path targetPath = assetsPath.resolve(item.getFileName());
								classLogger.info("Performing asset restructure for {} -> {}", item, targetPath);
								Files.move(item, targetPath, StandardCopyOption.REPLACE_EXISTING);
							} catch (IOException e) {
								classLogger.error(
										"Failed to move legacy guardrail asset '{}' to '{}' while restructuring assets for {}",
										item, assetsPath.resolve(item.getFileName()), engineIdAndName, e);
							}
						} else {
							classLogger.info("Ignoring asset restructure for {}", item);
						}
					});
				}
			}
			if (!AssetUtility.isGit(this.engineVersionFolder)) {
				GitRepoUtils.init(this.engineVersionFolder);
			}
		}

		this.isMCPEnabled = Boolean.parseBoolean(smssProp.getProperty(Constants.MCP_ENABLED) + "");
	}

	@Override
	public void delete() {
		IEngine.CATALOG_TYPE eType = getCatalogType();
		String engineIdAndName = SmssUtilities.getUniqueName(this.engineName, this.engineId);
		classLogger.debug("Delete {} engine {}", eType, engineIdAndName);
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error("Failed to close {} engine {} during delete", eType, engineIdAndName, e);
		}

		File engineFolder = new File(this.engineBaseFolder);
		if (engineFolder.exists()) {
			classLogger.info("Deleting {} engine folder {}", eType, engineFolder);
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error("Failed to delete {} engine folder {}", eType, engineFolder, e);
			}
		} else {
			classLogger.info("{} engine folder {} does not exist", eType, engineFolder);
		}

		classLogger.info("Deleting {} engine smss {}", eType, this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch (IOException e) {
			classLogger.error("Failed to delete {} engine smss {}", eType, this.smssFilePath, e);
		}

		// remove from DIHelper
		UploadUtilities.removeEngineFromDIHelper(this.engineId);

		// remove from secret store
		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if (secretStore != null) {
			secretStore.deleteEngineSecrets(getCatalogType(), this.engineId, this.engineName);
		}
	}

	@Override
	public CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.GUARDRAIL;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return getGuardrailType().getGuardrailName();
	}

}
