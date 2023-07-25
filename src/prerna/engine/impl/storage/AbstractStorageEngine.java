package prerna.engine.impl.storage;

import java.util.Properties;

import prerna.engine.api.IStorage;
import prerna.util.Constants;

public abstract class AbstractStorageEngine implements IStorage {

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
	public void connect(Properties smssProp) throws Exception {
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
	
	
}
