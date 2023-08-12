package prerna.util;

import java.io.IOException;

import prerna.engine.api.IDatabase;
import prerna.engine.api.impl.util.Owler;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.poi.main.helper.CSVFileHelper;

public class CSVToOwlMaker {

	/**
	 * 
	 * @param csvFile
	 * @param owlFileLocation
	 * @param dbType
	 */
	public void makeFlatOwl(String csvFile, String owlFileLocation, IDatabase.DATABASE_TYPE dbType, boolean addUniqueId) {
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

		Owler owler = new Owler(owlFileLocation, dbType);
		owler.addConcept(cleanTableName, null, null);
		if(addUniqueId) {
			String identityColumn = cleanTableName + "_UNIQUE_ROW_ID";
			owler.addProp(cleanTableName, identityColumn, "LONG", null);
		}
		
		for(int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			String cleanHeader = RDBMSEngineCreationHelper.cleanTableName(headers[headerIndex]);
			owler.addProp(cleanTableName, cleanHeader, typePredictions[headerIndex][0].toString(), (String) typePredictions[headerIndex][1]);
		}
		
		owler.commit();
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String [] args) {
		DIHelper.getInstance().loadCoreProp("C:/Users/pkapaleeswaran/workspacej3/MonolithDev3/RDF_Map_web.prop");

		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		String owlFile = "C:/Users/pkapaleeswaran/workspacej3/datasets/MovieOWL.OWL";
		CSVToOwlMaker maker = new CSVToOwlMaker();
		maker.makeFlatOwl(fileName, owlFile, IDatabase.DATABASE_TYPE.RDBMS, true);
	}
}
