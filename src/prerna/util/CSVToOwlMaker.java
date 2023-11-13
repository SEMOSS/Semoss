package prerna.util;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;

public class CSVToOwlMaker {

	private static final Logger classLogger = LogManager.getLogger(CSVToOwlMaker.class);
	
	/**
	 * Create OWL from CSV
	 * OWLEngine will be released at end of method
	 * @param owlEngine
	 * @param csvFile
	 * @param owlFileLocation
	 * @param dbType
	 * @param addUniqueId
	 * @throws Exception
	 */
	public void makeFlatOwl(WriteOWLEngine owlEngine, String csvFile, String owlFileLocation, IDatabaseEngine.DATABASE_TYPE dbType, boolean addUniqueId) throws Exception {
		try {
			// get the headers + types + additional types
			// based on the csv parsing
			// and then generate a new OWL file
	
			CSVFileHelper helper = new CSVFileHelper();
			// parse and collect headers
			helper.parse(csvFile);
			helper.collectHeaders();
	
			String [] headers = helper.getHeaders();
			Object [][] typePredictions = helper.predictTypes();
	
			String fileName = Utility.getOriginalFileName(csvFile);
			String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
	
			owlEngine.addConcept(cleanTableName, null, null);
			if(addUniqueId) {
				String identityColumn = cleanTableName + "_UNIQUE_ROW_ID";
				owlEngine.addProp(cleanTableName, identityColumn, "LONG", null);
			}
			
			for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
				String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
				owlEngine.addProp(cleanTableName, cleanHeader, typePredictions[headerIndex][0].toString(), (String) typePredictions[headerIndex][1]);
			}
			
			try {
				owlEngine.commit();
				owlEngine.export();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} finally {
			owlEngine.close();
		}
	}

}
