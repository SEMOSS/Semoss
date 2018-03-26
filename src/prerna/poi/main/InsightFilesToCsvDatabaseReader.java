package prerna.poi.main;

import prerna.engine.api.IEngine;

public class InsightFilesToCsvDatabaseReader extends AbstractEngineCreator {

	public enum DATABASE_TYPE {RDBMS, R}

	/**
	 * Default constructor
	 */
	public InsightFilesToCsvDatabaseReader() {
		
	}
	
	/**
	 * Take the csv file and generate a engine backed by it
	 * @return
	 */
	public IEngine generateEngineFromCsvFile(DATABASE_TYPE type, String appName, String fileLocation) {
		// initialize the engine
		if(type == DATABASE_TYPE.RDBMS) {
			openRdbmsEngineWithoutConnection(appName);
		} else {
			openREngineWithoutConnection(appName);
		}
		
		// regardless of the engine type
		// we will still have the same OWL
		
		
		
		// we do need to address the bifurcation in the SMSS file
		
		
		return this.engine;
	}
	
}
