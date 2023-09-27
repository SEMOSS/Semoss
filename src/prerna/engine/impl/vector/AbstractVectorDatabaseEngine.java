package prerna.engine.impl.vector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import prerna.engine.api.IEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.RDBMSUtility;

public abstract class AbstractVectorDatabaseEngine implements IVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractVectorDatabaseEngine.class);
	
	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties smssProp = null;
	protected String smssFilePath = null;
	
	protected String encoderName = null;
	protected String encoderType = null;
	protected String connectionURL = null;

	
	// string substitute vars
	Map vars = new HashMap();
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
		this.connectionURL = this.smssProp.getProperty(Constants.CONNECTION_URL);
		if(this.getVectorDatabaseType() == VectorDatabaseTypeEnum.FAISS) {
			this.connectionURL = RDBMSUtility.fillParameterizedFileConnectionUrl(this.connectionURL, this.engineId, this.engineName);
			this.smssProp.put(Constants.CONNECTION_URL, this.connectionURL);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Properties getOrigSmssProp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.VECTOR;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getVectorDatabaseType().toString();
	}
	
	@Override
	public void delete() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
}
