package prerna.engine.impl.app;

import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.impl.AbstractEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AppEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(AppEngine.class);
	
	/**
	 * Overriding the default behavior
	 * Do not need to do anything except load the insights database
	 */
	public void openDB(String propFile) {
		this.baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		this.propFile = propFile;
		this.prop = Utility.loadProperties(propFile);
		
		// get id & name
		this.engineId = this.prop.getProperty(Constants.ENGINE);
		this.engineName = this.prop.getProperty(Constants.ENGINE_ALIAS);
		
		// only need to load the insights database
		// there is no data in this app - so no OWL
		this.loadInsightsRdbms();
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
		return null;
	}

	@Override
	public void insertData(String query) {
		LOGGER.info("There is no data to store for an AppEngine!");
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		return null;
	}

	@Override
	public void removeData(String query) {
		LOGGER.info("There is no data to store for an AppEngine!");
	}

	@Override
	public void commit() {
		LOGGER.info("There is no data to store for an AppEngine!");
	}
}
