package prerna.algorithm.learning.matching;

import java.io.File;

import org.apache.commons.lang.StringUtils;

import prerna.poi.main.helper.ImportOptions;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class MatchingDB {
	String baseFolder;

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

}
