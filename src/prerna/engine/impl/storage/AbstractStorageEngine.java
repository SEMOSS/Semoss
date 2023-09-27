package prerna.engine.impl.storage;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractStorageEngine implements IStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractDatabaseEngine.class);
	
	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	protected String engineId = null;
	protected String engineName = null;
	protected Properties smssProp = null;
	protected String smssFilePath = null;
	
	/**
	 * Init the general storage values
	 * @param builder
	 * @throws Exception 
	 */
	public void open(String smssFilePath) throws Exception {
		this.smssFilePath = smssFilePath;
		this.open(Utility.loadProperties(smssFilePath));
	}
	
	/**
	 * Init the general storage values
	 * @param builder
	 * @throws Exception 
	 */
	public void open(Properties smssProp) throws Exception {
		this.smssProp = smssProp;
		this.engineId = smssProp.getProperty(Constants.ENGINE);
		this.engineName = smssProp.getProperty(Constants.ENGINE_ALIAS);
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
	public void setSmssFilePath(String smssFilePath) {
		this.smssFilePath = smssFilePath;
	}
	
	@Override
	public String getSmssFilePath() {
		return this.smssFilePath;
	}
	
	@Override
	public void setSmssProp(Properties smssProp) {
		this.smssProp = smssProp;
	}
	
	@Override
	public Properties getSmssProp() {
		return this.smssProp;
	}
	
	@Override
	public Properties getOrigSmssProp() {
		return this.smssProp;
	}
	
	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.STORAGE;
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getStorageType().toString();
	}
	
	@Override
	public void delete() {
		classLogger.debug("Delete storage engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		File engineFolder = new File(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "/" + Constants.STORAGE_FOLDER + "/" + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		if(engineFolder.exists()) {
			classLogger.info("Delete storage engine folder " + engineFolder);
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			classLogger.info("Storage engine folder " + engineFolder + " does not exist");
		}
		
		classLogger.info("Deleting storage engine smss " + this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// remove from DIHelper
		String engineIds = (String)DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		engineIds = engineIds.replace(";" + this.engineId, "");
		// in case we are at the start
		engineIds = engineIds.replace(this.engineId + ";", "");
		DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineIds);
		DIHelper.getInstance().removeEngineProperty(this.engineId);
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
	
}
