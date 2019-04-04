package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NLSQueryHelperReactor extends AbstractRFrameReactor {

	/**
	 * Returns predicted next word of an NLP Search as String array
	 */

	protected static final String CLASS_NAME = NLSQueryHelperReactor.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// make sure that the user even wants this running
	// if not, just always return null
	protected static final String HELP_ON = "helpOn";

	public NLSQueryHelperReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.APP.getKey(), HELP_ON };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		boolean helpOn = getHelpOn();

		// if user wants this off, then check first and return null if so
		if (!helpOn) {
			String[] emptyArray = new String[0];
			return new NounMetadata(emptyArray, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}

		// otherwise, proceed with the reactor
		String[] packages = new String[] { "data.table", "stringr", "stringdist", "udpipe", "tokenizers", "openNLP", "openNLPmodels.en" };
		this.rJavaTranslator.checkPackages(packages);
		String query = this.keyValue.get(this.keysToGet[0]);
		List<String> engineFilters = getEngineIds();
		boolean hasFilters = !engineFilters.isEmpty();
		// only run if at least x words have been typed
		if (query.equals("") || query.isEmpty()) {
			String[] firstWordArray = { "What", "Which", "Who", "Select", "How" };
			return new NounMetadata(firstWordArray, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}

		// Check to make sure that needed files exist before building recommendation
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File algo1 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "nli_db.R");
		File algo2 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "word_vectors.R");
		File history = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "nlq_history.txt");
		File historyRDS = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "nli_training.rds");
		if (!algo1.exists() || !algo2.exists() || !history.exists()) {
			String message = "Necessary files missing to generate search results.";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// check to make sure the RDS exists
		// if it does not, then will need to run nli_history_mgr (30 sec)
		// so do not allow this feature until rds exists
		// will be created on a scheduled basis
		if (!historyRDS.exists()) {
			String[] emptyArray = new String[0];
			return new NounMetadata(emptyArray, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}

		// Generate string to initialize R console
		StringBuilder sb = new StringBuilder();
		String wd = "wd" + Utility.getRandomString(5);
		sb.append(wd + "<- getwd();");
		sb.append(("setwd(\"" + baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts\");").replace("\\", "/"));
		sb.append("source(\"nli_db.R\");");
		sb.append("source(\"word_vectors.R\");");
		sb.append(RSyntaxHelper.loadPackages(packages));
		this.rJavaTranslator.runR(sb.toString());

		// Collect all the apps that we will iterate through
		if (hasFilters) {
			// need to validate that the user has access to these ids
			if (AbstractSecurityUtils.securityEnabled()) {
				List<String> userIds = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
				// make sure our ids are a complete subset of the user ids
				// user defined list must always be a subset of all the engine
				// ids
				if (!userIds.containsAll(engineFilters)) {
					throw new IllegalArgumentException("Attempting to filter to app ids that user does not have access to or do not exist");
				}
			} else {
				List<String> allIds = MasterDatabaseUtility.getAllEngineIds();
				if (!allIds.containsAll(engineFilters)) {
					throw new IllegalArgumentException("Attempting to filter to app ids that not exist");
				}
			}
		} else {
			if (AbstractSecurityUtils.securityEnabled()) {
				engineFilters = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			}
		}

		// get matrix of data from local master
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engineFilters);

		// run script and get array in alphabetical order
		String[] retData = generateAndRunScript(query, allTableCols);
		Arrays.sort(retData);

		// reset working directory
		this.rJavaTranslator.runR("setwd(\"" + wd + "\");");

		// return data to the front end
		return new NounMetadata(retData, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/**
	 * Get the list of engines
	 * 
	 * @return
	 */
	private List<String> getEngineIds() {
		List<String> engineFilters = new Vector<String>();
		GenRowStruct engineGrs = this.store.getNoun(this.keysToGet[1]);
		for (int i = 0; i < engineGrs.size(); i++) {
			engineFilters.add(engineGrs.get(i).toString());
		}

		return engineFilters;
	}

	/**
	 * Generate and run the script, returning array of strings
	 * 
	 * @param query
	 * @param allTableCols
	 * @return
	 */
	private String[] generateAndRunScript(String query, List<Object[]> allTableCols) {
		// Getting database info
		String rTempTable = "NaturalLangTable" + Utility.getRandomString(8);
		String rHistoryTable = "historyTable" + Utility.getRandomString(8);
		String result = "result" + Utility.getRandomString(8);
		String wordVector = "wordVector_" + this.getSessionId().substring(0, 10);
		StringBuilder rsb = new StringBuilder();

		String rAppIds = "c(";
		String rTableNames = "c(";
		String rColNames = "c(";
		String rColTypes = "c(";

		int colCount = allTableCols.size();
		// create R vector of appid, tables, and columns
		for (int i = 0; i < colCount; i++) {
			Object[] entry = allTableCols.get(i);
			String appId = entry[0].toString();
			String table = entry[1].toString();
			String column = entry[2].toString();
			String dataType = entry[3].toString();
			if (i == 0) {
				rAppIds += "'" + appId + "'";
				rTableNames += "'" + table + "'";
				rColNames += "'" + column + "'";
				rColTypes += "'" + dataType + "'";
			} else {
				rAppIds += ",'" + appId + "'";
				rTableNames += ",'" + table + "'";
				rColNames += ",'" + column + "'";
				rColTypes += ",'" + dataType + "'";
			}
		}

		rAppIds += ")";
		rTableNames += ")";
		rColNames += ")";
		rColTypes += ")";

		rsb.append(rTempTable + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + " , AppID = " + rAppIds + ", Datatype = " + rColTypes + ", stringsAsFactors = FALSE);");
		rsb.append("if(!exists(\"" + wordVector + "\")) { " + wordVector + " <- readRDS(\"glove.rds\") };");
		rsb.append(result + "<- get_next_word(\"" + query + "\", " + rTempTable + ", \"next\" , my_vectors = " + wordVector + ");");
		this.rJavaTranslator.runR(rsb.toString());

		// get back the data
		String[] list = this.rJavaTranslator.getStringArray(result);

		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm( " + rTempTable + " , " + rHistoryTable + " , " + result + " ); gc();");

		// return list
		return list;
	}

	private boolean getHelpOn() {
		GenRowStruct overrideGrs = this.store.getNoun(this.keysToGet[2]);
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}
}