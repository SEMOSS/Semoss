package prerna.reactor.algorithms;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RunDatabaseDescriptionGeneratorReactor extends AbstractRFrameReactor {

	private static final String SIZE = "size";
	private static final String WORDS = "words";
	protected static final String CLASS_NAME = RunDatabaseDescriptionGeneratorReactor.class.getName();

	// RunDatabaseDescriptionGenerator(app=["appID"], size=["small"] words=["100"]);
	public RunDatabaseDescriptionGeneratorReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), SIZE, WORDS };
	}

	@Override
	public NounMetadata execute() {
		// get inputs
		init();
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		StringBuilder rsb = new StringBuilder();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String size = this.keyValue.get(this.keysToGet[1]);
		String token = this.keyValue.get(this.keysToGet[2]);
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		if (database == null) {
			throw new IllegalArgumentException("Must define the database to pull data from");
		}

		// logger message
		logger.info("Generating Database Description...");

		// source the files
		rsb.append("source(\"" + baseFolder.replace("\\", "/") + "/R/AnalyticsRoutineScripts/proceed.R\");");

		// check if packages are installed
		String[] packages = { "gpt2", "reticulate" };
		this.rJavaTranslator.checkPackages(packages);

		// get current database input
		String curr_db = getCurrentDbTable(database, rsb);

		// run function
		String tempResult = "result_" + Utility.getRandomString(8);
		rsb.append(tempResult + " <- infer_db_desc(" + curr_db + ", \"" + size + "\", total_tokens=" + token + ");");
		// System.out.println(rsb.toString());
		this.rJavaTranslator.runR(rsb.toString());
		String result = this.rJavaTranslator.getString(tempResult);

		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + curr_db + ", " + tempResult + "); gc();");

		// create return description
		NounMetadata noun = new NounMetadata(result, PixelDataType.CONST_STRING);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Database Description generated successfully!"));
		// return all
		return noun;

	}

	/**
	 * Generate the 2 data.tables based on the table structure and relationships
	 * and returns back the results from the algorithm
	 * 
	 * @param query
	 * @param allApps
	 * @param engineFilters
	 * @return
	 */
	private String getCurrentDbTable(IDatabaseEngine database, StringBuilder rStr) {
		String rDbTable = "dbTable_" + Utility.getRandomString(10);

		// first get the total number of cols
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(database.getEngineId());
		int totalColCount = allTableCols.size();

		// start building script
		String rColNames = "c(";
		String rTableNames = "c(";

		// create R vector of appid, tables, and columns
		for (int i = 0; i < totalColCount; i++) {
			Object[] entry = allTableCols.get(i);
			String table = entry[0].toString();
			String column = entry[1].toString();
			if (entry[0] != null && entry[1] != null) {
				if (i == 0) {
					rTableNames += "'" + table + "'";
					rColNames += "'" + column + "'";
				} else {
					rTableNames += ",'" + table + "'";
					rColNames += ",'" + column + "'";
				}
			}
		}

		// close all the arrays created
		rTableNames += ")";
		rColNames += ")";

		// create the session tables
		rStr.append(rDbTable + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + ");");
		this.rJavaTranslator.runR(rStr.toString());

		return rDbTable;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SIZE)) {
			return "The text column to run the sentiment analysis on";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}