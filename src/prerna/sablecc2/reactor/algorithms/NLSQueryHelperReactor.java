package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
	protected static final String GLOBAL = "global";

	public NLSQueryHelperReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.APP.getKey(), HELP_ON,
				GLOBAL };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		boolean helpOn = getHelpOn();
		boolean global = getGlobal();

		// if user wants this off, then check first and return null if so
		if (!helpOn) {
			String[] emptyArray = new String[0];
			return new NounMetadata(emptyArray, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}

		// otherwise, proceed with the reactor
		String[] packages = new String[] { "igraph", "SteinerNet", "data.table" , "tools" };
		this.rJavaTranslator.checkPackages(packages);
		String query = this.keyValue.get(this.keysToGet[0]);
		List<String> engineFilters = getEngineIds();

		// Generate string to initialize R console
		this.rJavaTranslator.runR(RSyntaxHelper.loadPackages(packages));
		
		// need to validate that the user has access to these ids
		if (engineFilters.size() > 0) {
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
		
		//pull asset app
		//set default paths
		String savePath = baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts";
		if (AbstractSecurityUtils.securityEnabled()) {
			User user = this.insight.getUser();
			String appId = user.getAssetEngineId(user.getPrimaryLogin());
			String appName = "Asset";
			if (appId != null && !(appId.isEmpty())) {
				IEngine assetApp = Utility.getEngine(appId);
				savePath = AssetUtility.getAppAssetVersionFolder(appName, appId) + DIR_SEPARATOR + "assets";
				ClusterUtil.reactorPullFolder(assetApp, savePath);
			}
		}
		savePath = savePath.replace("\\", "/"); 
		
		// source the proper script
		StringBuilder sb = new StringBuilder();
		String wd = "wd" + Utility.getRandomString(5);
		String rFolderPath = baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR;
		sb.append(wd + "<- getwd();");
		sb.append(("setwd(\"" + savePath + "\");").replace("\\", "/"));
		sb.append(("source(\"" + rFolderPath + "template_assembly.R" + "\");").replace("\\", "/"));
		if(global) {
			sb.append(("source(\"" + rFolderPath + "template_db.R" + "\");").replace("\\", "/"));
		} else {
			sb.append(("source(\"" + rFolderPath + "template.R" + "\");").replace("\\", "/"));
		}
		
		this.rJavaTranslator.runR(sb.toString());
		
		// cluster tables if needed
		String nldrPath1 = savePath + DIR_SEPARATOR + "nldr_membership.rds";
		String nldrPath2 = savePath + DIR_SEPARATOR + "nldr_db.rds";
		String nldrPath3 = savePath + DIR_SEPARATOR + "nldr_joins.rds";	
		File nldrMembership = new File(nldrPath1);
		File nldrDb = new File(nldrPath2);
		File nldrJoins = new File(nldrPath3);
		long replaceTime = System.currentTimeMillis() - ((long)1 * 24 * 60 * 60 * 1000);
		if(global) {
			if(!nldrDb.exists() || !nldrJoins.exists() || !nldrMembership.exists() || nldrMembership.lastModified() < replaceTime ) {
				createRdsFiles();
			}
		}

		// handle differently depending on whether it is from the frame or global
		// convert json input into java map
		// query = "[{\"component\":\"select\",\"column\":[\"Rating\",\"Genre\"]},{\"component\":\"sum\",\"column\":\"MovieBudget\"},{\"component\":\"average\",\"column\":\"Revenue_Domestic\"},{\"component\":\"group\",\"column\":[\"Rating\",\"Genre\"]},{\"component\":\"where\",\"column\":\"Genre\",\"operation\":\"==\",\"value\":\"Drama\"},{\"component\":\"where\",\"column\":\"Rating\",\"operation\":\"==\",\"value\":\"R\"},{\"component\":\"having sum\",\"column\":\"MovieBudget\",\"operation\":\">\",\"value\":\"100\"},{\"component\":\"having average\",\"column\":\"Revenue_Domestic\",\"operation\":\">\",\"value\":\"100\"}]";
		// query = "[{\"component\":\"select\",\"column\":[\"Title\",\"Genre\"]},{\"component\": \"sum\"}]";

		String queryTable = getQueryTableFromJson(query);
		String colHeadersAndTypesFrame = getColHeadersAndTypes(global,engineFilters);
		Object[] retData = getDropdownItems(queryTable, colHeadersAndTypesFrame);
		
		// reset wd
		this.rJavaTranslator.runR("setwd(" + wd + ")");
		this.rJavaTranslator.executeEmptyR("rm( " + wd + "); gc();");

		// error catch -- if retData is null, return empty list
		// return error message??
		if (retData == null) {
			retData = new String[0];
		}
		
		// push asset app
		if (AbstractSecurityUtils.securityEnabled()) {
			User user = this.insight.getUser();
			String appId = user.getAssetEngineId(user.getPrimaryLogin());
			String appName = "Asset";
			if (appId != null && !(appId.isEmpty())) {
				IEngine assetApp = Utility.getEngine(appId);
				savePath = AssetUtility.getAppAssetVersionFolder(appName, appId) + DIR_SEPARATOR + "assets";
				ClusterUtil.reactorPushFolder(assetApp, savePath);
			}
		}

		// return data to the front end
		return new NounMetadata(retData, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private String getColHeadersAndTypes(boolean global, List<String> engineFilters) {
		String dbTable = "db_" + Utility.getRandomString(6);
		String rSessionTable = "NaturalLangTable" + this.getSessionId().substring(0, 10);
		
		if(global) {
			StringBuilder rsb = new StringBuilder();
			rsb.append(rSessionTable + " <- readRDS(\"nldr_db.rds\");");
			
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
			
			// we only need column and type column
			rsb.append(dbTable + " <- " + rSessionTable + ";");
			this.rJavaTranslator.runR(rsb.toString());
			
			// gc
			this.rJavaTranslator.executeEmptyR("rm(" + appFilters + "); gc();");
		} else {
			ITableDataFrame frame = this.getFrame();
			// build the dataframe of COLUMN and TYPE
			Map<String, SemossDataType> colHeadersAndTypes = frame.getMetaData().getHeaderToTypeMap();
			List<String> columnList = new Vector<String>();
			List<String> typeList = new Vector<String>();
			for (Map.Entry<String, SemossDataType> entry : colHeadersAndTypes.entrySet()) {
				String col = entry.getKey();
				String type = entry.getValue().toString();
				if (col.contains("__")) {
					col = col.split("__")[1];
				}
				columnList.add(col);

				if (type.equals("INT") || type.equals("DOUBLE")) {
					type = "NUMBER";
				}
				typeList.add(type);
			}
			// turn into R table
			String rColumns = RSyntaxHelper.createStringRColVec(columnList);
			String rTypes = RSyntaxHelper.createStringRColVec(typeList);
			this.rJavaTranslator.runR(dbTable + " <- data.frame(Column = " + rColumns + " , Datatype = " + rTypes
					+ ", stringsAsFactors = FALSE);");
		}
		
		return dbTable;
	}

	private Object[] getDropdownItems(String queryTable, String colHeadersAndTypesFrame) {
		// init
		String retList = "retList_" + Utility.getRandomString(6);
		StringBuilder rsb = new StringBuilder();
		Object[] dropdownOptions = null;

		// pass the query table and the new dataframe to the script
		rsb.append(retList + " <- analyze_request(" + colHeadersAndTypesFrame + "," + queryTable + ")");
		this.rJavaTranslator.runR(rsb.toString());

		// collect the array
		int requestRows = this.rJavaTranslator.getInt("nrow(" + queryTable + ")");
		if (requestRows > 0) {
			dropdownOptions = this.rJavaTranslator.getStringArray(retList);
		} else {
			// if its a blank template, then pass as a list of component lists
			int dropdownLength = this.rJavaTranslator.getInt("length(" + retList + ")");
			dropdownOptions = new Object[dropdownLength];
			for (int i = 0; i < dropdownLength; i++) {
				int rIndex = i + 1;
				String[] item = this.rJavaTranslator.getStringArray(retList + "[[" + rIndex + "]]");
				dropdownOptions[i] = item;
			}
		}

		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm( " + retList + "," + colHeadersAndTypesFrame + " ); gc();");

		// return
		return dropdownOptions;
	}

	private String getQueryTableFromJson(String queryJSON) {
		String retTable = "queryTable_" + Utility.getRandomString(6);
		
		// check for blank
		if(queryJSON == null || queryJSON.isEmpty()) {
			queryJSON = "[]";
		}

		// read string into list
		List<Map<String, Object>> optMap = new Vector<Map<String, Object>>();
		optMap = new Gson().fromJson(queryJSON, optMap.getClass());

		// start building script of
		List<String> componentList = new Vector<String>();
		List<String> elementList = new Vector<String>();
		List<String> valueList = new Vector<String>();

		// loop through the map
		for (Map<String, Object> component : optMap) {
			String comp = component.get("component").toString();

			// handle select and group
			String[] selectAndGroup = { "select", "average", "count", "max", "min", "sum", "group", "stdev",
					"unique count" };
			List<String> selectAndGroupList = Arrays.asList(selectAndGroup);
			if (selectAndGroupList.contains(comp)) {
				List<String> columns = new Vector<String>();

				// if aggregate, add the aggregate row
				if (!comp.equals("select") && !comp.equals("group")) {
					// change aggregate to select
					if (!comp.equals("group")) {
						comp = "select";
					}

					// so first add the aggregate row
					componentList.add(comp);
					elementList.add("aggregate");
					valueList.add(component.get("component").toString());

					// change the column to arraylist for below
					if (component.get("column") == null) {
						columns.add("?");
					} else {
						columns.add(component.get("column").toString());
					}

				} else {
					// change the column to arraylist for below
					if (component.get("column") == null) {
						columns.add("?");
					} else {
						columns = (List<String>) component.get("column");
					}
				}

				// then, add the component and columns
				for (String col : columns) {
					componentList.add(comp);
					elementList.add("column");
					valueList.add(col);
				}
			}

			// handle the based on
			else if (comp.startsWith("based on")) {
				String agg = comp.substring(9);
				comp = "based on";
				componentList.add(comp);
				componentList.add(comp);

				elementList.add("aggregate");
				valueList.add(agg);

				elementList.add("column");
				if (component.get("column") == null) {
					valueList.add("?");
				} else {
					valueList.add(component.get("column").toString());
				}
			}

			// handle where and having
			else if (comp.equals("where") || comp.startsWith("having")) {
				if (comp.startsWith("having")) {
					String agg = comp.substring(7);
					comp = "having";
					componentList.add(comp);
					elementList.add("aggregate");
					valueList.add(agg);
				}

				componentList.add(comp);
				componentList.add(comp);
				componentList.add(comp);

				elementList.add("column");
				if (component.get("column") == null) {
					valueList.add("?");
				} else {
					valueList.add(component.get("column").toString());
				}

				elementList.add("is");
				if (component.get("operation") == null) {
					valueList.add("?");
				} else {
					valueList.add(component.get("operation").toString());
				}

				elementList.add("value");
				if (component.get("value") == null) {
					valueList.add("?");
				} else {
					valueList.add(component.get("value").toString());
				}
			}

			// handle sort and rank
			else if (comp.equals("sort") || comp.equals("rank") || comp.equals("position")) {
				componentList.add(comp);
				componentList.add(comp);

				elementList.add("column");
				if (component.get("column") == null) {
					valueList.add("?");
				} else {
					valueList.add(component.get("column").toString());
				}

				elementList.add("is");
				if (component.get("operation") == null) {
					valueList.add("?");
				} else {
					valueList.add(component.get("operation").toString());
				}

				if (!comp.equals("sort")) {
					componentList.add(comp);
					elementList.add("value");
					if (component.get("value") == null) {
						valueList.add("?");
					} else {
						valueList.add(component.get("value").toString());
					}
				}

				// handle position
				else if (comp.equals("position")) {
					componentList.add(comp);
					componentList.add(comp);
					componentList.add(comp);

					elementList.add("is");
					if (component.get("operation") == null) {
						valueList.add("?");
					} else {
						valueList.add(component.get("operation").toString());
					}

					elementList.add("value");
					if (component.get("value") == null) {
						valueList.add("?");
					} else {
						valueList.add(component.get("value").toString());
					}

					elementList.add("column");
					if (component.get("column") == null) {
						valueList.add("?");
					} else {
						valueList.add(component.get("column").toString());
					}

				}
			}
		}

		// turn into strings
		String rComponent = RSyntaxHelper.createStringRColVec(componentList);
		String rElement = RSyntaxHelper.createStringRColVec(elementList);
		String rValue = RSyntaxHelper.createStringRColVec(valueList);

		// turn into R table
		String script = retTable + " <- data.frame(Component = " + rComponent + " , Element = " + rElement
				+ ", Value = " + rValue + ", stringsAsFactors = FALSE);";
		this.rJavaTranslator.runR(script);

		return retTable;
	}

	private void createRdsFiles() {
		StringBuilder sessionTableBuilder = new StringBuilder();

		// source the files
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String filePath = (baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR)
				.replace("\\", "/");
		sessionTableBuilder.append("source(\"" + filePath + "data_inquiry_guide.R\");");
		sessionTableBuilder.append("source(\"" + filePath + "data_inquiry_assembly.R\");");

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
		if (totalNumRels == 0) {
			rAppIDsJoin = "character(0)";
			rTbl1 = "character(0)";
			rTbl2 = "character(0)";
			rJoinBy1 = "character(0)";
			rJoinBy2 = "character(0)";
		}

		// create the session tables
		String db = "nldrDb" + Utility.getRandomString(5);
		String joins = "nldrJoins" + Utility.getRandomString(5);
		sessionTableBuilder.append(
				db + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + " , AppID = " + rAppIds
						+ ", Datatype = " + rColTypes + ", Key = " + rPrimKey + ", stringsAsFactors = FALSE);");
		sessionTableBuilder.append(joins + " <- data.frame(tbl1 = " + rTbl1 + " , tbl2 = " + rTbl2 + " , joinby1 = "
				+ rJoinBy1 + " , joinby2 = " + rJoinBy2 + " , AppID = " + rAppIDsJoin + ", stringsAsFactors = FALSE);");

		// run the cluster tables function
		sessionTableBuilder.append("cluster_tables (" + db + "," + joins + ");");
		sessionTableBuilder.append("saveRDS (" + db + ",\"nldr_db.rds\");");
		sessionTableBuilder.append("saveRDS (" + joins + ",\"nldr_joins.rds\");");

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

	private boolean getHelpOn() {
		GenRowStruct overrideGrs = this.store.getNoun(this.keysToGet[2]);
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}

	private boolean getGlobal() {
		GenRowStruct overrideGrs = this.store.getNoun(this.keysToGet[3]);
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}
}