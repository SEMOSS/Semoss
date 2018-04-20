package prerna.engine.impl.app;

import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AppEngine extends AbstractEngine {

	/**
	 * Overriding the default behavior
	 * Do not need to do anything except load the insights database
	 */
	public void openDB(String propFile) {
		this.baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		this.prop = Utility.loadProperties(propFile);
		
		// since the URL is most likely parameterized for sharing
		// create the param hash to fill it in
		Hashtable <String, String> paramHash = new Hashtable <String, String>();
		paramHash.put("BaseFolder", baseFolder);
		if(this.engineName != null) {
			paramHash.put("engine", getEngineName());
		}
		
		String insightDatabaseLoc = prop.getProperty(Constants.RDBMS_INSIGHTS);
		insightDatabaseLoc = Utility.fillParam2(insightDatabaseLoc, paramHash);

		// now open up and set the insights rdbms
		this.insightRDBMS = new RDBMSNativeEngine();
		Properties prop = new Properties();
		prop.put(Constants.DRIVER, insightDriver);
		prop.put(Constants.RDBMS_TYPE, insightRDBMSType);
		String connURL = connectionURLStart + baseFolder + "/" + insightDatabaseLoc + connectionURLEnd;
		prop.put(Constants.CONNECTION_URL, connURL);
		prop.put(Constants.USERNAME, insightUsername);
		this.insightRDBMS.setProperties(prop);
		this.insightRDBMS.openDB(null);
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		return ENGINE_TYPE.APP;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	
	/*
	 * Need to clean the interface to allow for what we are doing
	 * APP is a wrapper around a set of insights (parameterized insights)
	 * Where we allow the swapping of data on the insights
	 * 
	 * Since there is no data, the below are not needed
	 */
	
	@Override
	public Object execQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

}
