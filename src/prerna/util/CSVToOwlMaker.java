package prerna.util;

import java.io.IOException;

import prerna.engine.api.IEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;

public class CSVToOwlMaker {

	/**
	 * 
	 * @param csvFile
	 * @param owlFileLocation
	 * @param engineType
	 */
	public void makeOwl(String csvFile, String owlFileLocation, IEngine.ENGINE_TYPE engineType) {
		// really simple
		// load the csv
		// find what the types are
		// generate a owl file based on headers
		// later move into semantic blending
		
		
		CSVFileHelper helper = new CSVFileHelper();
		// parse and collect headers
		helper.parse(csvFile);
		helper.collectHeaders();
		
		String [] headers = helper.getHeaders();
		Object [][] typePredictions = helper.predictTypes();
		
		String fileName = Utility.getOriginalFileName(csvFile);
		// make the table name based on the fileName
		String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
		
		String identityColumn = cleanTableName + "_UNIQUE_ROW_ID";

		Owler owler = new Owler(owlFileLocation, engineType);
				
		// need to add metadata
		// the unique row id becomes the primary key for every other 
		// column in the csv file
		// TODO: should we add it as LONG, or as VARCHAR... 
		//		as a VARCHAR, it would make it default to always (and only) be used with counts via the UI
		//		don't see why a person would every want to do sum/avg/etc. on it....
		owler.addConcept(cleanTableName, identityColumn, "LONG");
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
			owler.addProp(cleanTableName, identityColumn, cleanHeader, typePredictions[headerIndex][1].toString());
		}
		owler.commit();
		try {
			owler.export();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String [] args) {
		DIHelper.getInstance().loadCoreProp("C:/Users/pkapaleeswaran/workspacej3/MonolithDev3/RDF_Map_web.prop");

		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		String owlFile = "C:/Users/pkapaleeswaran/workspacej3/datasets/MovieOWL.OWL";
		CSVToOwlMaker maker = new CSVToOwlMaker();
		maker.makeOwl(fileName,owlFile, IEngine.ENGINE_TYPE.RDBMS);
	}
}
