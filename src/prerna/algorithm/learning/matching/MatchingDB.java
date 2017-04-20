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
			String csvDirectory = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\" + Constants.R_MATCHING_CSVS_FOLDER;
			csvDirectory = csvDirectory.replace("\\", "/");

			// Grab the prop directory
			String propDirectory = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\"
					+ Constants.R_MATCHING_PROP_FOLDER;
			propDirectory = propDirectory.replace("\\", "/");

			options.setDbType(ImportOptions.DB_TYPE.RDF);

			// Set all the csv files
			File csvDirectoryFile = new File(csvDirectory);
			options.setFileLocation(
					csvDirectory + "/" + StringUtils.join(csvDirectoryFile.list(), ";" + csvDirectory + "/"));

			// Set all the prop files
			File propDirectoryFile = new File(propDirectory);
			options.setPropertyFiles(
					propDirectory + "/" + StringUtils.join(propDirectoryFile.list(), ";" + propDirectory + "/"));
		}
		if (dbType.equals(ImportOptions.DB_TYPE.RDBMS.toString())) {
			String matchingDbName = "MatchingRDMBSDatabase";
			options.setDbName(matchingDbName);
			String h2Directory = baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\h2Matching\\";
			h2Directory = h2Directory.replace("\\", "/");
			options.setDbType(ImportOptions.DB_TYPE.RDBMS);
			options.setFileLocation(h2Directory + "/0_flat_table.csv");
			options.setPropertyFiles(h2Directory + "/MatchingDatabase_0_flath2.prop");
		}
		return options;
	}

}
