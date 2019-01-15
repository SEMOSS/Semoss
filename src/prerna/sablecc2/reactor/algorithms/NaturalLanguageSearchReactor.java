package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NaturalLanguageSearchReactor extends AbstractRFrameReactor {
	protected static final String CLASS_NAME = NaturalLanguageSearchReactor.class.getName();

	// Test with movie db: NaturalLanguageSearch("What movie titles have IMDBScore greater than 8","[app_id]")

	public NaturalLanguageSearchReactor() {

		this.keysToGet = new String[] {
				// The search query that the user enters
				ReactorKeysEnum.QUERY_KEY.toString(), ReactorKeysEnum.APP.getKey() };
	}

	@Override
	public NounMetadata execute() {

		init();
		organizeKeys();
		int stepCounter = 1;
		Logger logger = this.getLogger(CLASS_NAME);
		String engineId = this.keyValue.get(this.keysToGet[1]);
		// we may have the alias
		if (AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if (!SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have access to database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
			if (!MasterDatabaseUtility.getAllEngineIds().contains(engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist");
			}
		}
		IEngine engine = Utility.getEngine(engineId);
		// Check to make sure that the database is an RDBMS
		if (!(engine instanceof RDBMSNativeEngine)) {
			// do not create frame because this only works for rdbms databases
			String message = "Database is not an RDBMS Database";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// Check Packages
		logger.info(stepCounter + ". Checking R Packages");
		String[] packages = new String[] { "udpipe" };
		this.rJavaTranslator.checkPackages(packages);
		logger.info(stepCounter + ". Done");
		stepCounter++;

		// Check to make sure that needed files exist before searching
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File algo = new File(baseFolder + "\\R\\AnalyticsRoutineScripts\\nli_db.R");
		if (!algo.exists()) {
			String message = "Necessary files missing to generate search results.";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		} else {
			// Initialize variables
			String result = "";

			// Generate string to run in R
			StringBuilder rsb = new StringBuilder();
			StringBuilder sb = new StringBuilder();
			logger.info(stepCounter + ". Loading R scripts to perform natural language search");
			String wd = "wd" + Utility.getRandomString(5);
			sb.append(wd + "<- getwd();");
			sb.append("setwd(\"" + baseFolder + "\\R\\AnalyticsRoutineScripts\");\n");
			sb.append("source(\"nli_db.R\");\n");
			sb.append("library(udpipe);\n");
			this.rJavaTranslator.runR(sb.toString().replace("\\", "/"));
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// Getting database info
			logger.info(stepCounter + ". Getting Database schema to generate descriptions");
			String rTempTable = "NaturalLangTable";
			List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
			String engineName = Utility.getEngine(engineId).getEngineName();
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// Iterate through each column and read into R
			logger.info(stepCounter + ". Storing Database schema in R");
			String rColNames = "Column <- c(";
			String rTableNames = "Table <- c(";
			int colCount = allTableCols.size();
			int colCounter = 0;

			// create R vector of table columns and table rows
			for (Object[] tableCol : allTableCols) {
				String table = tableCol[0] + "";
				String col = tableCol[1] + "";
				if (!table.toLowerCase().equals(col.toLowerCase())) {
					rColNames += "'";
					rTableNames += "'";
					if (colCounter < colCount - 1) {
						rColNames += (col.toLowerCase() + "', ");
						rTableNames += (table.toLowerCase() + "', ");
					} else if (colCounter == colCount - 1) {
						rColNames += (col.toLowerCase() + "')");
						rTableNames += (table.toLowerCase() + "')");
					} else {
						System.out.println("test: colcount error");
					}
				}
				colCounter++;
			}

			// Create data frame from above vectors
			rsb.append(rColNames + ";\n");
			rsb.append(rTableNames + ";\n");
			rsb.append(rTempTable + " <- data.frame(Column, Table);\n");
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// Run R Routine based on new dataframe
			logger.info(stepCounter + ". Returning SQL syntax");
			String query = this.keyValue.get(this.keysToGet[0]);
			rsb.append("result <- nlidb_mgr(\"" + query + "\"," + rTempTable + ");\n");
			this.rJavaTranslator.runR(rsb.toString());
			result = this.rJavaTranslator.getString("result;\n");
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// now that we have the query let's query the database
			logger.info(stepCounter + ". Querying database and returing data in Frame");

			IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(engine, result);
			String[] headers = it.getHeaders();
			SemossDataType[] types = it.getTypes();

			// create a frame
			H2Frame frame = new H2Frame(headers);
			Map<String, SemossDataType> tempDataType = new Hashtable<String, SemossDataType>();
			for (int i = 0; i < headers.length; i++) {
				tempDataType.put(headers[i], types[i]);
			}

			// insert data from database into frame
			frame.addRowsViaIterator(it, tempDataType);
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// garbage cleanup
			String gc = "rm(\"analyze_adj\",     \"analyze_noun\",    \"analyze_prep\",    \"build_sql\",      \r\n"
					+ "\"Column\",          \"conaVWE8z2\",      \"db_match\",        \"extract_subtree\",\r\n"
					+ "\"get_chunks\",      \"get_parent\",      \"get_pos\",         \"map_nouns\",      \r\n"
					+ "\"NaturalLangTable\",\"nlidb_mgr\",       \"parse_question\",  \"replace_words\",  \r\n"
					+ "\"result\",          \"Table\",           \"tagger\", \"" + wd + "\");\n";
			this.rJavaTranslator.runR(gc);

			// set frame for the insight
			this.insight.setDataMaker(frame);
			return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);

		}
	}
}