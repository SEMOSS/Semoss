package prerna.engine.impl.venv;

import java.io.File;
import java.io.IOException;

import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IVenvEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public abstract class AbstractVenvEngine implements IVenvEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractVenvEngine.class);
	
	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties smssProp = null;
	protected String smssFilePath = null;
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}
	
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
		return IEngine.CATALOG_TYPE.VENV;
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getVenvType().toString();
	}

	@Override
	public void delete() {
		classLogger.debug("Delete model engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		File engineFolder = new File(
				EngineUtility.getSpecificEngineBaseFolder
					(getCatalogType(), this.engineId, this.engineName)
				);
		if(engineFolder.exists()) {
			classLogger.info("Delete model engine folder " + engineFolder);
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			classLogger.info("Model engine folder " + engineFolder + " does not exist");
		}
		
		classLogger.info("Deleting model engine smss " + this.smssFilePath);
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
