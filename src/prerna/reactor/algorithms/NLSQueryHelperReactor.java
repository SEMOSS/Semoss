package prerna.reactor.algorithms;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
	
	// R variables to pass through background session
	protected static final String NLDR_DB = "nldr_db";
	protected static final String NLDR_JOINS = "nldr_joins";
	protected static final String NLDR_MEMBERSHIP = "nldr_membership";

	public NLSQueryHelperReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.DATABASE.getKey(), HELP_ON,
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
		List<String> dbFilters = getDatabaseIds();

		// Generate string to initialize R console
		this.rJavaTranslator.runR(RSyntaxHelper.loadPackages(packages));
		
		// need to validate that the user has access to these ids
		if (dbFilters.size() > 0) {
			List<String> databaseIds = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
			// make sure our ids are a complete subset of the user ids
			// user defined list must always be a subset of all the engine ids
			if (!databaseIds.containsAll(dbFilters)) {
				throw new IllegalArgumentException("Attempting to filter to database ids that user does not have access to or do not exist");
			}
		} else {
			dbFilters = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
		}

		// source the proper script
		StringBuilder sb = new StringBuilder();
		String rFolderPath = baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR;
		sb.append(("source(\"" + rFolderPath + "template_assembly.R" + "\");").replace("\\", "/"));
		if(global) {
			sb.append(("source(\"" + rFolderPath + "template_db.R" + "\");").replace("\\", "/"));
		} else {
			sb.append(("source(\"" + rFolderPath + "template.R" + "\");").replace("\\", "/"));
		}
		
		this.rJavaTranslator.runR(sb.toString());

		// handle differently depending on whether it is from the frame or global
		// convert json input into java map
		String queryTable = getQueryTableFromJson(query);
		String colHeadersAndTypesFrame = getColHeadersAndTypes(global,dbFilters);
		Object[] retData = getDropdownItems(queryTable, colHeadersAndTypesFrame);

		// error catch -- if retData is null, return empty list
		// return error message??
		if (retData == null) {
			retData = new String[0];
		}

		// return data to the front end
		return new NounMetadata(retData, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private String getColHeadersAndTypes(boolean global, List<String> dbFilters) {
		String dbTable = "db_" + Utility.getRandomString(6);
		String rSessionTable = "NaturalLangTable" + this.getSessionId().substring(0, 10);
		
		if(global) {
			StringBuilder rsb = new StringBuilder();
			rsb.append(rSessionTable + " <- " + NLDR_DB + ";");
			
			// filter the rds files to the engineFilters
			String appFilters = "appFilters" + Utility.getRandomString(8);
			rsb.append(appFilters + " <- c(");
			String comma = "";
			for (String dbId : dbFilters) {
				rsb.append(comma + " \"" + dbId + "\" ");
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
		if (getGlobal()) {
			rsb.append(retList + " <- analyze_request(" + colHeadersAndTypesFrame + "," + queryTable + "," + NLDR_MEMBERSHIP + ")");
		} else {
			rsb.append(retList + " <- analyze_request(" + colHeadersAndTypesFrame + "," + queryTable + ")");
		}
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
					"unique count", "distribution" };
			List<String> selectAndGroupList = Arrays.asList(selectAndGroup);
			if (selectAndGroupList.contains(comp)) {
				List<String> columns = new Vector<String>();

				// if aggregate, add the aggregate row
				if (!comp.equals("select") && !comp.equals("group") && !comp.equals("distribution")) {
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
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/**
	 * Get the list of engines
	 * 
	 * @return
	 */
	private List<String> getDatabaseIds() {
		List<String> dbFilters = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		for (int i = 0; i < grs.size(); i++) {
			dbFilters.add(grs.get(i).toString());
		}

		return dbFilters;
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