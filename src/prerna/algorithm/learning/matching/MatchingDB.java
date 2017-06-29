package prerna.algorithm.learning.matching;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.helper.ImportOptions;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class MatchingDB {
	String baseFolder;
	//DB name
	public static final String MATCHING_RDBMS_DB = "MatchingRDBMSDatabase";
	
	//Table name
	public static final String MATCH_ID = "match_id";
	
	//Column names
	public static final String SOURCE_DATABASE = "source_database";
	

	public MatchingDB(String baseFolder) {
		this.baseFolder = baseFolder;
	}

	public void saveDB(String dbType) {
		// save both db types rdf and rdbms
		if (dbType.equals("")) {
			importDB(ImportOptions.DB_TYPE.RDF.toString());
			importDB(ImportOptions.DB_TYPE.RDBMS.toString());
		}
		
		if (dbType.equals(ImportOptions.DB_TYPE.RDF.toString())) {
			importDB(ImportOptions.DB_TYPE.RDF.toString());
		}
		
		if (dbType.equals(ImportOptions.DB_TYPE.RDBMS.toString())) {
			importDB(ImportOptions.DB_TYPE.RDBMS.toString());
		}

	}

	/**
	 * This method
	 * 
	 * @param dbType
	 */
	private void importDB(String dbType) {
		ImportOptions options = getOptions(dbType);
		ImportDataProcessor importer = new ImportDataProcessor();
		options.setImportMethod(ImportOptions.IMPORT_METHOD.CREATE_NEW);
		try {
			importer.runProcessor(options);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private ImportOptions getOptions(String dbType) {
		ImportOptions options = new ImportOptions();
		options.setImportType(ImportOptions.IMPORT_TYPE.CSV);
		options.setBaseFolder(baseFolder);
		options.setAutoLoad(false);
		// Improves performance
		options.setAllowDuplicates(true);

		if (dbType.equals(ImportOptions.DB_TYPE.RDF.toString())) {
			String matchingDbName = "MatchingRDFDatabase";
			options.setDbName(matchingDbName);
			options.setDbType(ImportOptions.DB_TYPE.RDF);
			
			String baseMatchingFolder = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\"
					+ Constants.R_MATCHING_FOLDER;
			baseMatchingFolder = baseMatchingFolder.replace("\\", "/");
			
			// Grab the csv directory Semoss/R/Matching/Temp/rdf
			String baseRDFDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdf";
			baseRDFDirectory = baseRDFDirectory.replace("\\", "/");

			// Semoss/R/Matching/Temp/rdf/MatchingCsvs
			String rdfCsvDirectory = baseRDFDirectory + "\\" + Constants.R_MATCHING_CSVS_FOLDER;
			rdfCsvDirectory = rdfCsvDirectory.replace("\\", "/");

			// Grab the prop directory Semoss/R/Matching/Temp/rdf/MatchingProp
			String rdfPropDirectory = baseRDFDirectory  + "\\"
					+ Constants.R_MATCHING_PROP_FOLDER;
			rdfPropDirectory = rdfPropDirectory.replace("\\", "/");

			// Set all the csv files
			File csvDirectoryFile = new File(rdfCsvDirectory);
			options.setFileLocation(
					rdfCsvDirectory + "/" + StringUtils.join(csvDirectoryFile.list(), ";" + rdfCsvDirectory + "/"));

			// Set all the prop files
			File propDirectoryFile = new File(rdfPropDirectory);
			options.setPropertyFiles(
					rdfPropDirectory + "/" + StringUtils.join(propDirectoryFile.list(), ";" + rdfPropDirectory + "/"));
		}
		if (dbType.equals(ImportOptions.DB_TYPE.RDBMS.toString())) {
			String matchingDbName = "MatchingRDBMSDatabase";
			options.setDbName(matchingDbName);
			String baseMatchingFolder = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\"
					+ Constants.R_MATCHING_FOLDER;
			baseMatchingFolder = baseMatchingFolder.replace("\\", "/");
			String rdbmsDirectory = baseMatchingFolder + "\\" + Constants.R_TEMP_FOLDER + "\\rdbms";
			rdbmsDirectory = rdbmsDirectory.replace("\\", "/");
			options.setDbType(ImportOptions.DB_TYPE.RDBMS);
			options.setFileLocation(rdbmsDirectory + "/0_flat_table.csv");
			options.setPropertyFiles(rdbmsDirectory + "/MatchingDatabase_0_flath2.prop");
		}
		return options;
	}
	
	/**
	 * Gets the source database from matching rdbms db
	 * @return
	 */
	public static ArrayList<String> getSourceDatabases() {
		RDBMSNativeEngine matchingEngine = (RDBMSNativeEngine) Utility.getEngine(MATCHING_RDBMS_DB);
		ArrayList<String> sourceEngines = new ArrayList<>();

		// Get all source databases
		if (matchingEngine != null) {
			String sourceDBQuery = "SELECT DISTINCT " + SOURCE_DATABASE + " FROM " + MATCH_ID + ";";

			Map<String, Object> values = matchingEngine.execQuery(sourceDBQuery);
			ResultSet rs = (ResultSet) values.get(RDBMSNativeEngine.RESULTSET_OBJECT);

			try {
				while (rs.next()) {
					sourceEngines.add(rs.getString(1));
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

		}
		return sourceEngines;
		
	}

}
