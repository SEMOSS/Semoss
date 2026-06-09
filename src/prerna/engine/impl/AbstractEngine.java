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
package prerna.engine.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.engine.api.IEngine;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public abstract class AbstractEngine implements IEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractEngine.class);

	protected static final String FILE_SEPARATOR = "/";

	protected String smssFilePath = null;
	protected CaseInsensitiveProperties origSmssProp = null;
	protected CaseInsensitiveProperties smssProp = null;

	protected String engineId = null;
	protected String engineName = null;
	protected String displayName = null;

	protected String engineBaseFolder = null;
	protected String engineAppRootFolder = null;
	protected String engineVersionFolder = null;
	protected String engineAssetsFolder = null;

	protected boolean keepInputOutput = true;
	// to define custom log4j2.xml at an engine level
	// to isolate tenant logs
	protected volatile LoggerContext engineSpecificLoggerCtx;

	protected boolean isMCPEnabled = false;

	/**
	 * This is if we have an engine with no assets Or for database, connection but
	 * no OWL
	 */
	protected boolean isBasic = false;

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
		// is basic, no real folder structure
		if (this.isBasic) {
			if (smssProp.containsKey(Constants.ENGINE)) {
				this.engineId = smssProp.getProperty(Constants.ENGINE);
			}
			if (smssProp.containsKey(Constants.ENGINE_ALIAS)) {
				this.engineName = smssProp.getProperty(Constants.ENGINE_ALIAS);
			}
			String smssDisplayName = smssProp.getProperty(Constants.ENGINE_DISPLAY_NAME);
			this.displayName = (smssDisplayName != null && !smssDisplayName.trim().isEmpty()) ? smssDisplayName
					: this.engineName;
			return;
		}

		// not basic, so normal flow
		this.engineId = smssProp.getProperty(Constants.ENGINE);
		this.engineName = smssProp.getProperty(Constants.ENGINE_ALIAS);
		String smssDisplayName = smssProp.getProperty(Constants.ENGINE_DISPLAY_NAME);
		this.displayName = (smssDisplayName != null && !smssDisplayName.trim().isEmpty()) ? smssDisplayName
				: this.engineName;

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
								classLogger.info("Performing asset restructure for {} > {}", item, targetPath);
								Files.move(item, targetPath, StandardCopyOption.REPLACE_EXISTING);
							} catch (IOException e) {
								classLogger.error("Failed to move legacy engine asset '{}' to '{}' during restructure",
										item, assetsPath.resolve(item.getFileName()), e);
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
		if (smssProp.getProperty(Constants.KEEP_INPUT_OUTPUT) != null) {
			this.keepInputOutput = Boolean.parseBoolean(smssProp.getProperty(Constants.KEEP_INPUT_OUTPUT) + "");
		}
	}

	@Override
	public void delete() {
		IEngine.CATALOG_TYPE eType = getCatalogType();
		classLogger.debug("Delete {} engine {}", eType, SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error("Failed to close {} engine {} during delete", eType,
					SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
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
			classLogger.error("Failed to delete {} engine smss file {}", eType, smssFile, e);
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
	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

	@Override
	public String getEngineId() {
		return this.engineId;
	}

	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	@Override
	public String getEngineName() {
		return this.engineName;
	}

	@Override
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	@Override
	public String getDisplayName() {
		return (this.displayName != null && !this.displayName.trim().isEmpty()) ? this.displayName : this.engineName;
	}

	@Override
	public void setSmssFilePath(String smssFilePath) {
		this.smssFilePath = smssFilePath;
	}

	@Override
	public String getSmssFilePath() {
		return this.smssFilePath;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		if (smssProp instanceof CaseInsensitiveProperties) {
			this.origSmssProp = (CaseInsensitiveProperties) smssProp;
			this.smssProp = new CaseInsensitiveProperties(smssProp);
		} else {
			this.origSmssProp = new CaseInsensitiveProperties(smssProp);
			this.smssProp = new CaseInsensitiveProperties(smssProp);
		}
	}

	@Override
	public CaseInsensitiveProperties getSmssProp() {
		return this.smssProp;
	}

	@Override
	public CaseInsensitiveProperties getOrigSmssProp() {
		return this.origSmssProp;
	}

	@Override
	public boolean isBasic() {
		return this.isBasic;
	}

	@Override
	public void setBasic(boolean isBasic) {
		this.isBasic = isBasic;
	}

	@Override
	public boolean isMCPEnabled() {
		return this.isMCPEnabled;
	}

	@Override
	public boolean keepInputOutput() {
		return this.keepInputOutput;
	}

	@Override
	public Logger getEngineLogger(String loggerName) {
		if (this.engineSpecificLoggerCtx != null) {
			return this.engineSpecificLoggerCtx.getLogger(loggerName);
		}

		File log4j2 = new File(this.engineAssetsFolder + "/log4j2.xml");
		if (!log4j2.exists() || !log4j2.isFile()) {
			return null;
		}

		if (engineSpecificLoggerCtx == null) {
			synchronized (this) {
				if (engineSpecificLoggerCtx == null) {
					ClassLoader isolatedLoader = new URLClassLoader(new URL[0], null);
					engineSpecificLoggerCtx = Configurator.initialize(this.engineId, isolatedLoader,
							"file:" + log4j2.getAbsolutePath());
				}
			}
		}

		return this.engineSpecificLoggerCtx.getLogger(loggerName);
	}

}
