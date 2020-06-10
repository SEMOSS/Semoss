package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.AssetUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NLSQueryHelperReactor extends AbstractRFrameReactor {

	/**
	 * Returns predicted next word of an NLP Search as String array
	 */

	protected static final String CLASS_NAME = NLSQueryHelperReactor.class.getName();

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
		String[] packages = new String[] { "igraph" , "SteinerNet" , "data.table" };
		this.rJavaTranslator.checkPackages(packages);
		String query = this.keyValue.get(this.keysToGet[0]);
		List<String> engineFilters = getEngineIds();
		
		// if there were no engine filters, use all engines
		if (engineFilters.size() > 0) {
			// need to validate that the user has access to these ids
			if (AbstractSecurityUtils.securityEnabled()) {
				List<String> userIds = SecurityQueryUtils.getFullUserEngineIds(this.insight.getUser());
				// make sure our ids are a complete subset of the user ids
				// user defined list must always be a subset of all the engine ids
				if (!userIds.containsAll(engineFilters)) {
					throw new IllegalArgumentException(
							"Attempting to filter to app ids that user does not have access to or do not exist");
				}
			} else {
				List<String> allIds = MasterDatabaseUtility.getAllEngineIds();
				if (!allIds.containsAll(engineFilters)) {
					throw new IllegalArgumentException("Attempting to filter to app ids that not exist");
				}
			}
		} else {
			if (AbstractSecurityUtils.securityEnabled()) {
				engineFilters = SecurityQueryUtils.getFullUserEngineIds(this.insight.getUser());
			} else {
				engineFilters = MasterDatabaseUtility.getAllEngineIds();
			}
		}

		// Generate string to initialize R console
		this.rJavaTranslator.runR(RSyntaxHelper.loadPackages(packages));
		
		//for demo only
		// TODO: pull asset app
		//set default paths
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String savePath = baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts";
		if (AbstractSecurityUtils.securityEnabled()) {
			User user = this.insight.getUser();
			String appId = user.getAssetEngineId(user.getPrimaryLogin());
			String appName = "Asset";
			if (appId != null && !(appId.isEmpty())) {
				savePath = AssetUtility.getAppAssetVersionFolder(appName, appId) + DIR_SEPARATOR + "assets";
			}
		}
		savePath = savePath.replace("\\", "/");

		
		// set the working directly
		String wd = "wd" + Utility.getRandomString(6);
		StringBuilder rsb = new StringBuilder();
		rsb.append(wd + " <- "+ "getwd();");
		rsb.append("setwd(\"" + savePath + "\");");
		this.rJavaTranslator.runR(rsb.toString());
		
		// cluster tables if needed
		String nldrPath1 = savePath + DIR_SEPARATOR + "nldr_membership.rds";
		String nldrPath2 = savePath + DIR_SEPARATOR + "nldr_db.rds";
		String nldrPath3 = savePath + DIR_SEPARATOR + "nldr_joins.rds";	
		File nldrMembership = new File(nldrPath1);
		File nldrDb = new File(nldrPath2);
		File nldrJoins = new File(nldrPath3);
		long replaceTime = System.currentTimeMillis() - ((long)1 * 24 * 60 * 60 * 1000);
		if(!nldrDb.exists() || !nldrJoins.exists() || !nldrMembership.exists() || nldrMembership.lastModified() < replaceTime ) {
			createRdsFiles();
		}

		// run script and get array in alphabetical order
		String[] retData = generateAndRunScript(query, engineFilters);
		Arrays.sort(retData);
		
		// run Garbage Cleanup
		this.rJavaTranslator.runR("setwd(" + wd + ")");
		this.rJavaTranslator.executeEmptyR("rm( " + wd + "); gc();");

		// return data to the front end
		return new NounMetadata(retData, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private void createRdsFiles() {
		StringBuilder sessionTableBuilder = new StringBuilder();
		
		// source the files
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String filePath = (baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR).replace("\\", "/");
		sessionTableBuilder.append("source(\""+ filePath + "data_inquiry_guide.R\");");
		sessionTableBuilder.append("source(\""+ filePath + "data_inquiry_assembly.R\");");
		
		// use all the apps
		List<String> engineFilters = null;
		if (AbstractSecurityUtils.securityEnabled()) {
			engineFilters = SecurityQueryUtils.getFullUserEngineIds(this.insight.getUser());
		} else {
			engineFilters = MasterDatabaseUtility.getAllEngineIds();
		}

		// first get the total number of cols and relationships
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engineFilters);
		List<String[]> allRelations = MasterDatabaseUtility.getRelationships(engineFilters);
		int totalNumRels = allRelations.size();
		int totalColCount = allTableCols.size();

		// start building script
		String rAppIds = "c(";
		String rTableNames = "c(";
		String rColNames = "c(";
		String rColTypes = "c(";
		String rPrimKey = "c(";

		// create R vector of appid, tables, and columns
		for (int i = 0; i < totalColCount; i++) {
			Object[] entry = allTableCols.get(i);
			String appId = entry[0].toString();
			String table = entry[1].toString();
			if (entry[0] != null && entry[1] != null && entry[2] != null && entry[3] != null && entry[4] != null) {
				String column = entry[2].toString();
				String dataType = entry[3].toString();
				String pk = entry[4].toString().toUpperCase();

				if (i == 0) {
					rAppIds += "'" + appId + "'";
					rTableNames += "'" + appId + "._." + table + "'";
					rColNames += "'" + column + "'";
					rColTypes += "'" + dataType + "'";
					rPrimKey += "'" + pk + "'";
				} else {
					rAppIds += ",'" + appId + "'";
					rTableNames += ",'" + appId + "._." + table + "'";
					rColNames += ",'" + column + "'";
					rColTypes += ",'" + dataType + "'";
					rPrimKey += ",'" + pk + "'";
				}
			}
		}

		// create R vector of table columns and table rows
		String rAppIDsJoin = "c(";
		String rTbl1 = "c(";
		String rTbl2 = "c(";
		String rJoinBy1 = "c(";
		String rJoinBy2 = "c(";

		int firstRel = 0;
		for (int i = 0; i < totalNumRels; i++) {
			String[] entry = allRelations.get(i);
			String appId = entry[0];
			String rel = entry[3];

			String[] relSplit = rel.split("\\.");
			if (relSplit.length == 4) {
				// this is RDBMS
				String sourceTable = relSplit[0];
				String sourceColumn = relSplit[1];
				String targetTable = relSplit[2];
				String targetColumn = relSplit[3];

				// check by firstRel, not index of for loop
				// loop increments even if relSplit.length != 4
				// whereas firstRel only increases if something is added to frame
				if (firstRel == 0) {
					rAppIDsJoin += "'" + appId + "'";
					rTbl1 += "'" + appId + "._." + sourceTable + "'";
					rTbl2 += "'" + appId + "._." + targetTable + "'";
					rJoinBy1 += "'" + sourceColumn + "'";
					rJoinBy2 += "'" + targetColumn + "'";
				} else {
					rAppIDsJoin += ",'" + appId + "'";
					rTbl1 += ",'" + appId + "._." + sourceTable + "'";
					rTbl2 += ",'" + appId + "._." + targetTable + "'";
					rJoinBy1 += ",'" + sourceColumn + "'";
					rJoinBy2 += ",'" + targetColumn + "'";
				}

				if (sourceColumn.endsWith("_FK")) {
					// if column ends with a _FK, then add it to NaturalLangTable also
					rAppIds += ",'" + appId + "'";
					rTableNames += ",'" + appId + "._." + sourceTable + "'";
					rColNames += ",'" + sourceColumn + "'";
					rColTypes += ", 'STRING' ";
					rPrimKey += ", 'FALSE' ";
				}
				// no longer adding the first row to this data frame, increment..
				firstRel++;
			} else {
				// this is an RDF or Graph
				String sourceTable = entry[1];
				String sourceColumn = entry[1];
				String targetTable = entry[2];
				String targetColumn = entry[2];
				if (firstRel == 0) {
					rAppIDsJoin += "'" + appId + "'";
					rTbl1 += "'" + appId + "._." + sourceTable + "'";
					rTbl2 += "'" + appId + "._." + targetTable + "'";
					rJoinBy1 += "'" + sourceColumn + "'";
					rJoinBy2 += "'" + targetColumn + "'";
				} else {
					rAppIDsJoin += ",'" + appId + "'";
					rTbl1 += ",'" + appId + "._." + sourceTable + "'";
					rTbl2 += ",'" + appId + "._." + targetTable + "'";
					rJoinBy1 += ",'" + sourceColumn + "'";
					rJoinBy2 += ",'" + targetColumn + "'";
				}
				// no longer adding the first row to this data frame, increment..
				firstRel++;
			}
		}

		// close all the arrays created
		rAppIds += ")";
		rTableNames += ")";
		rColNames += ")";
		rColTypes += ")";
		rPrimKey += ")";
		rAppIDsJoin += ")";
		rTbl1 += ")";
		rTbl2 += ")";
		rJoinBy1 += ")";
		rJoinBy2 += ")";
		
		// address where there were no rels
		if(totalNumRels == 0) {
			rAppIDsJoin = "character(0)";
			rTbl1 = "character(0)";
			rTbl2 = "character(0)";
			rJoinBy1 = "character(0)";
			rJoinBy2 = "character(0)";
		}

		// create the session tables
		String db = "nldrDb" + Utility.getRandomString(5);
		String joins = "nldrJoins" + Utility.getRandomString(5);
		sessionTableBuilder.append(db + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames
				+ " , AppID = " + rAppIds + ", Datatype = " + rColTypes + ", Key = " + rPrimKey
				+ ", stringsAsFactors = FALSE);");
		sessionTableBuilder.append(
				joins + " <- data.frame(tbl1 = " + rTbl1 + " , tbl2 = " + rTbl2 + " , joinby1 = " + rJoinBy1
						+ " , joinby2 = " + rJoinBy2 + " , AppID = " + rAppIDsJoin + ", stringsAsFactors = FALSE);");

		
		// run the cluster tables function
		sessionTableBuilder.append("cluster_tables ("+db+","+joins+");");
		sessionTableBuilder.append("saveRDS ("+db+",\"nldr_db.rds\");");
		sessionTableBuilder.append("saveRDS ("+joins+",\"nldr_joins.rds\");");
		
		this.rJavaTranslator.runR(sessionTableBuilder.toString());
		this.rJavaTranslator.executeEmptyR("rm( " + db + "," + joins + " ); gc();");

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
	private String[] generateAndRunScript(String query, List<String> engineFilters) {
		StringBuilder rsb = new StringBuilder();
		String result = "result" + Utility.getRandomString(6);
		String rSessionTable = "db" + Utility.getRandomString(6);
		String rSessionJoinTable = "joins" + Utility.getRandomString(6);
		
		// source the files
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String filePath = (baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR).replace("\\", "/");
		rsb.append("source(\""+ filePath + "data_inquiry_guide.R\");");
		rsb.append("source(\""+ filePath + "data_inquiry_assembly.R\");");

		// read in the rds files
		rsb.append(rSessionTable + " <- readRDS(\"nldr_db.rds\");");
		rsb.append(rSessionJoinTable + " <- readRDS(\"nldr_joins.rds\");");
		
		// filter the rds files to the engineFilters
		String appFilters = "appFilters" + Utility.getRandomString(8);
		rsb.append(appFilters + " <- c(");
		String comma = "";
		for (String appId : engineFilters) {
			rsb.append(comma + " \"" + appId + "\" ");
			comma = " , ";
		}
		rsb.append(");");
		rsb.append(rSessionTable + " <- " + rSessionTable + "[" + rSessionTable + "$AppID %in% " + appFilters + " ,];");
		rsb.append(rSessionJoinTable + " <- " + rSessionJoinTable + "[" + rSessionJoinTable + "$AppID %in% " + appFilters + " ,];");
		
		// run the function
		rsb.append(result + " <- get_next_keyword("+rSessionTable+",\"" + query + "\");");
		this.rJavaTranslator.runR(rsb.toString());

		// get back the data
		String[] list = this.rJavaTranslator.getStringArray("unique(" + result + ");");

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