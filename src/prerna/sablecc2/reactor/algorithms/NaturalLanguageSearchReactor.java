package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.h2.util.StringUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Generates pixel to dynamically create insight based on Natural Language
 * Search
 *
 */

public class NaturalLanguageSearchReactor extends AbstractRFrameReactor {
	protected static final String CLASS_NAME = NaturalLanguageSearchReactor.class.getName();

	public NaturalLanguageSearchReactor() {

		this.keysToGet = new String[] {
				// The search query that the user enters
				ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		int stepCounter = 1;
		Logger logger = this.getLogger(CLASS_NAME);
		GenRowStruct engineIdArray = this.store.getNoun(this.keysToGet[1]);
		String query = this.keyValue.get(this.keysToGet[0]);
		String finalPixel = "";
		ArrayList<Object> returnList = new ArrayList<Object>();

		// Check Packages
		logger.info(stepCounter + ". Checking R Packages and Necessary Files");
		String[] packages = new String[] { "udpipe" };
		this.rJavaTranslator.checkPackages(packages);
		stepCounter++;

		// Check to make sure that needed files exist before searching
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File algo1 = new File(baseFolder + "\\R\\AnalyticsRoutineScripts\\nli_db.R");
		File algo2 = new File(baseFolder + "\\R\\AnalyticsRoutineScripts\\db_pixel.R");
		File algo3 = new File(baseFolder + "\\R\\AnalyticsRoutineScripts\\english-ud-2.0-170801.udpipe");
		if (!algo1.exists() || !algo2.exists() || !algo3.exists()) {
			String message = "Necessary files missing to generate search results.";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		} else {
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// Generate string to initialize R console
			StringBuilder sb = new StringBuilder();
			logger.info(stepCounter + ". Loading R scripts to perform natural language search");
			String wd = "wd" + Utility.getRandomString(5);
			sb.append(wd + "<- getwd();\n");
			sb.append("setwd(\"" + baseFolder + "\\R\\AnalyticsRoutineScripts\");\n");
			sb.append("source(\"nli_db.r\");\n");
			sb.append("source(\"db_pixel.r\");\n");
			sb.append("library(udpipe);\n");
			this.rJavaTranslator.runR(sb.toString().replace("\\", "/"));
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// Collect all the apps that we will iterate through
			logger.info(stepCounter + ". Collecting apps to iterate through");
			List<String> cleanEngineIdList = new ArrayList<String>();
			if (engineIdArray != null && !engineIdArray.isEmpty()) {
				// At least one app was provided
				// Add each app in the array into the clean engine id list
				for (int i = 0; i < engineIdArray.size(); i++) {
					cleanEngineIdList.add(engineIdArray.get(i).toString());
				}
			} else {
				// no apps were provided
				// Get all apps that the user has access to
				if (AbstractSecurityUtils.securityEnabled()) {
					cleanEngineIdList = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
				} else {
					cleanEngineIdList = MasterDatabaseUtility.getAllEngineIds();
				}
			}
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// Loop through the apps and call the method
			logger.info(stepCounter + ". Building data frames");
			for (String eng : cleanEngineIdList) {
				SelectQueryStruct resultQuery = null;
				if (queryInCols(eng, query)) {
					resultQuery = addEngineToNLS(eng, query);
				}
				if (resultQuery != null) {
					HashMap<String, Object> frames = new HashMap<String, Object>();
					frames.put("app_id", eng);
					frames.put("query_struct", resultQuery);
					returnList.add(frames);
				}
			}
			logger.info(stepCounter + ". Done");
			stepCounter++;

			// if no results were found, return error message
			if (returnList.isEmpty()) {
				NounMetadata noun = new NounMetadata("Natural Language Search provided no results",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				return noun;
			}

			// get the top query struct
			// TODO: Decide better way to choose the query struct
			logger.info(stepCounter + ". Returning Pixel to generate data frame");
			SelectQueryStruct returnQs = ((HashMap<String, SelectQueryStruct>) returnList.get(0)).get("query_struct");
			String returnAppId = ((HashMap<String, String>) returnList.get(0)).get("app_id");
			finalPixel = buildPixelFromQs(returnQs, returnAppId);
			finalPixel += "Panel ( 0 ) | SetPanelLabel(\"" + Utility.getEngine(returnAppId).getEngineName() + ": " + query + "\");";
			logger.info(stepCounter + ". Done");
			stepCounter++;
		}

		return new NounMetadata(finalPixel, PixelDataType.CONST_STRING, PixelOperationType.UNEXECUTED_PIXELS);
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/**
	 * Build the query structure based on the list of sql components
	 * 
	 * @param list
	 * @return
	 */

	public SelectQueryStruct buildQueryStruct(List<Object[]> list) {
		SelectQueryStruct qs = new SelectQueryStruct();
		for (Object[] row : list) {
			String[] rowData = Arrays.copyOf(row, row.length, String[].class);

			// Check what type of row it is, then add to qs by case
			switch (rowData[1]) {

			// Add Selectors
			case "select":
				String engineID = rowData[0];
				boolean agg = false;
				String selectConcept = rowData[2];
				String selectProperty = rowData[3];
				if (!rowData[4].isEmpty()) {
					agg = true;
				}
				IEngine engineSel = Utility.getEngine(engineID);

				QueryColumnSelector selector = new QueryColumnSelector();
				selector.setTable(selectConcept);
				// this is a hack we used in advanced federate
				if (engineSel.getParentOfProperty(selectProperty + "/" + selectConcept) == null && !agg) {
					// we couldn't find a parent for this property
					// this means it is a concept itself
					// and we should only use table

					// handle it if it is a primary key
					selector.setColumn(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER);
				} else if (agg) {
					// handle it if it is an aggregate
					selector.setColumn(selectProperty);
					switch (rowData[4]) {
					case "min":
						QueryFunctionSelector min = new QueryFunctionSelector();
						min.setFunction(QueryFunctionHelper.MIN);
						min.addInnerSelector(selector);
						qs.addSelector(min);
						break;

					case "avg":
						QueryFunctionSelector avg = new QueryFunctionSelector();
						avg.setFunction(QueryFunctionHelper.AVERAGE_1);
						avg.addInnerSelector(selector);
						qs.addSelector(avg);
						break;

					case "max":
						QueryFunctionSelector max = new QueryFunctionSelector();
						max.setFunction(QueryFunctionHelper.MAX);
						max.addInnerSelector(selector);
						qs.addSelector(max);
						break;

					case "sum":
						QueryFunctionSelector sum = new QueryFunctionSelector();
						sum.setFunction(QueryFunctionHelper.SUM);
						sum.addInnerSelector(selector);
						qs.addSelector(sum);
						break;

					case "count":
						// currently runs UniqueCount instead
						QueryFunctionSelector uniqueCount = new QueryFunctionSelector();
						uniqueCount.setFunction(QueryFunctionHelper.UNIQUE_COUNT);
						uniqueCount.addInnerSelector(selector);
						qs.addSelector(uniqueCount);
						break;

					}
					// if aggregate, end
					break;
				} else {
					// it is normal column
					selector.setColumn(selectProperty);
				}
				qs.addSelector(selector);
				break;

			// Add Joins
			case "from":
				if (!rowData[3].isEmpty()) {
					String fromConcept = rowData[2];
					String toConcept = rowData[4];
					String joinType = "inner.join";
					qs.addRelation(fromConcept, toConcept, joinType);
				}
				break;

			// Add Filters on Column
			case "where":
				String engineId = rowData[0];
				String whereTable = rowData[2];
				String whereCol = rowData[3];
				String whereOperator = rowData[4];
				String whereValue = rowData[5];
				String whereValue2 = rowData[6];
				QueryColumnSelector colSelector = null;
				// this is a hack we used in advanced federate
				IEngine engine = Utility.getEngine(engineId);
				if (engine.getParentOfProperty(whereCol + "/" + whereTable) == null) {
					// we couldn't find a parent for this property
					// this means it is a concept itself
					// and we should only use table
					colSelector = new QueryColumnSelector(whereTable);
				} else {
					colSelector = new QueryColumnSelector(whereTable + "__" + whereCol);
				}
				qs.addSelector(colSelector);
				// Set default to be string and check for numbers in each case
				PixelDataType WhereType = PixelDataType.CONST_STRING;
				if (whereOperator.equals("=")) {
					if (StringUtils.isNumber(whereValue) || StringUtils.isNumber(whereValue2)) {
						WhereType = PixelDataType.CONST_DECIMAL;
					}
					if (!whereValue2.isEmpty()) {
						// this means that it is being set equal to another column
						whereValue = whereValue2;
						if (engine.getParentOfProperty(whereCol + "/" + whereTable) == null) {
							// we couldn't find a parent for this property
							// this means it is a concept itself
							// and we should only use table
							colSelector = new QueryColumnSelector(whereTable);
						} else {
							colSelector = new QueryColumnSelector(whereTable + "__" + whereCol);
						}
						qs.addSelector(colSelector);
						// Do I need to change to:
						WhereType = PixelDataType.COLUMN;
					}
				} else if (whereOperator.equals(">") || whereOperator.equals("<")) {
					if (StringUtils.isNumber(whereValue) || StringUtils.isNumber(whereValue2)) {
						WhereType = PixelDataType.CONST_DECIMAL;
					}
					if (!whereValue2.isEmpty()) {
						// this means that it is being set as greater than another column

						if (engine.getParentOfProperty(whereValue2 + "/" + whereValue) == null) {
							// we couldn't find a parent for this property
							// this means it is a concept itself
							// and we should only use table
							colSelector = new QueryColumnSelector(whereValue);
						} else {
							colSelector = new QueryColumnSelector(whereValue + "__" + whereValue2);
						}
						qs.addSelector(colSelector);
						WhereType = PixelDataType.COLUMN;
						whereValue = whereValue2;
					}
				} else if (whereOperator.equals("between")) {
					// between only exists for two integers in the current schema being sent to me
					WhereType = PixelDataType.CONST_DECIMAL;

					// Add the filter for the less than whereValue2 first
					whereOperator = "<";
					qs.addExplicitFilter(new SimpleQueryFilter(new NounMetadata(colSelector, PixelDataType.COLUMN),
							whereOperator, (new NounMetadata(whereValue2, WhereType))));

					// Now set the whereOperator to be greater than whereValue
					whereOperator = ">";

					// we do not have functionality to filter it to be between the values of two
					// columns
				}
				whereValue = whereValue.replaceAll(" ", "_");

				qs.addExplicitFilter(new SimpleQueryFilter(new NounMetadata(colSelector, PixelDataType.COLUMN),
						whereOperator, (new NounMetadata(whereValue, WhereType))));

				break;

			// Add Groups
			case "group":
				String groupConcept = rowData[2];
				String groupProperty = rowData[3];
				qs.addGroupBy(groupConcept, groupProperty);

				break;

			// Add Filters on Group (Needs Testing)
			case "having":
				String havingCol = rowData[3];
				String havingAgg = rowData[4];
				String havingOperator = rowData[5];
				PixelDataType havingType = PixelDataType.CONST_STRING;
				String havingValue = rowData[6];
				String havingValue2 = rowData[7];
				String havingValueAgg = rowData[8];

				// Check the Type of the operator and build filter
				// NOTE -- It can only be compared to another number or string
				if (!havingValueAgg.isEmpty()) {
					// the aggregate is being compared to another aggregate

					// this is what happens on the min and max -- does this functionality exist?

					return null;
				} else {
					List<IQuerySelector> selectors = qs.getSelectors();
					// transform aggregate to the semoss syntax
					switch (havingAgg) {
					case "avg":
						havingAgg = "Average";
						break;
					case "sum":
						havingAgg = "Sum";
						break;
					case "min":
						havingAgg = "Min";
						break;
					case "max":
						havingAgg = "Max";
						break;
					case "count":
						havingAgg = "UniqueCount";
						break;
					}
					for (IQuerySelector sel : selectors) {
						// get the selector that has the correct aggregate from the having clause
						if (sel.toString().contains(havingAgg) && sel.toString().contains(havingCol)) {
							if (havingOperator.equals("=")) {
								if (StringUtils.isNumber(havingValue) || StringUtils.isNumber(havingValue2)) {
									havingType = PixelDataType.CONST_DECIMAL;
								}
								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
										havingOperator, (new NounMetadata(havingValue, havingType))));
							} else if (havingOperator.equals("<") || havingOperator.equals(">")) {
								havingType = PixelDataType.CONST_DECIMAL;
								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
										havingOperator, (new NounMetadata(havingValue, havingType))));
							} else if (havingOperator.equals("between")) {
								havingType = PixelDataType.CONST_DECIMAL;
								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
										">", (new NounMetadata(havingValue, havingType))));
								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
										"<", (new NounMetadata(havingValue2, havingType))));
							}
						}
					}
				}
				break;

			// Error Check
			case "Error":
				return null;
			}
		}

		return qs;

	}

	/**
	 * Run the R script on the engine and return query struct
	 * 
	 * @param engineId
	 * @param query
	 * @return
	 */

	public SelectQueryStruct addEngineToNLS(String engineId, String query) {
		// Confirm that we have access to the app
		// we may have the alias
		if (AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if (!SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException(
						"Database " + engineId + " does not exist or user does not have access to database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
			if (!MasterDatabaseUtility.getAllEngineIds().contains(engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist");
			}
		}

		// Getting database info
		String rTempTable = "NaturalLangTable" + Utility.getRandomString(8);
		String rJoinTable = "JoinTable" + Utility.getRandomString(8);
		String result = "result" + Utility.getRandomString(8);
		StringBuilder rsb = new StringBuilder();
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engineId);

		String rColNames = "c(";
		String rTableNames = "c(";
		String rAppIDs_df = "c(";
		int colCount = allTableCols.size();
		int colCounter = 0;

		// create R vector of table columns and table rows
		for (Object[] tableCol : allTableCols) {
			String table = tableCol[0] + "";
			String col = tableCol[1] + "";
			if (true) {
				rColNames += "'";
				rTableNames += "'";
				rAppIDs_df += "'";
				if (colCounter < colCount - 1) {
					rColNames += (col + "', ");
					rTableNames += (table + "', ");
					rAppIDs_df += (engineId + "', ");
				} else if (colCounter == colCount - 1) {
					rColNames += (col + "'");
					rTableNames += (table + "'");
					rAppIDs_df += (engineId + "'");
				}
			}
			colCounter++;
		}

		// create R vector of table columns and table rows
		String rTbl1 = "c(";
		String rTbl2 = "c(";
		String rJoinBy1 = "c(";
		String rJoinBy2 = "c(";
		String rAppIDs_join = "c(";

		List<String[]> relationships = Utility.getEngine(engineId).getRelationships(true);

		colCounter = 0;
		for (String[] rel : relationships) {
			String[] joinPieces = rel[2].split("/")[5].split("\\.");
			String tbl1 = "";
			String joinby1 = "";
			String tbl2 = "";
			String joinby2 = "";
			if (joinPieces.length == 4) {
				tbl1 = "'" + joinPieces[0];
				joinby1 = "'" + joinPieces[1];
				tbl2 = "'" + joinPieces[2];
				joinby2 = "'" + joinPieces[3];
				if (colCounter < relationships.size() - 1) {
					rTbl1 += (tbl1 + "', ");
					rTbl2 += (tbl2 + "', ");
					rJoinBy1 += (joinby1 + "', ");
					rJoinBy2 += (joinby2 + "', ");
					rAppIDs_join += ("'" + engineId + "', ");

				} else if (colCounter == relationships.size() - 1) {
					rTbl1 += (tbl1 + "'");
					rTbl2 += (tbl2 + "'");
					rJoinBy1 += (joinby1 + "'");
					rJoinBy2 += (joinby2 + "'");
					rAppIDs_join += ("'" + engineId + "'");
				} else {
					System.out.println("Test: Thats weird");
				}
				if (joinby1.endsWith("_FK")) {
					// if column ends with a _FK, then add it to NaturalLangTable also
					rColNames += (", " + joinby1 + "'");
					rTableNames += (", " + tbl1 + "'");
					rAppIDs_df += (", \'" + engineId + "'");
				}
			} else {
				// TODO: Not sure how to handle this...
			}
			colCounter++;
		}

		// Create data frame from above columns vectors
		rColNames += ")";
		rTableNames += ")";
		rAppIDs_df += ")";
		rsb.append(rTempTable + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + " , AppID = "
				+ rAppIDs_df + ");\n");

		// Create data frame from above join vectors
		rTbl1 += ")";
		rTbl2 += ")";
		rJoinBy1 += ")";
		rJoinBy2 += ")";
		rAppIDs_join += ")";
		rsb.append(rJoinTable + " <- data.frame(tbl1 = " + rTbl1 + " , tbl2 = " + rTbl2 + " , joinby1 = " + rJoinBy1
				+ " , joinby2 = " + rJoinBy2 + " , AppID = " + rAppIDs_join + " );\n");

		if (relationships.isEmpty()) {
			rsb.append(result + " <- nliapp_mgr(\"" + query + "\"," + rTempTable + ");\n");
		} else {
			rsb.append(result + " <- nliapp_mgr(\"" + query + "\"," + rTempTable + "," + rJoinTable + ");\n");
		}
		this.rJavaTranslator.runR(rsb.toString());

		// Read in the data
		String[] headerOrdering = new String[] { "appid", "part", "item1", "item2", "item3", "item4", "item5", "item6",
				"item7" };
		List<Object[]> list = this.rJavaTranslator.getBulkDataRow(result, headerOrdering);

		// now that we have the query let's query the database
		SelectQueryStruct queryStruct = buildQueryStruct(list);
		// check to make sure valid SQL was returned
		if (queryStruct == null) {
			return null;
		}

		return queryStruct;
	}

	/**
	 * Make sure query matches with a column name
	 * 
	 * @param engineId
	 * @param query
	 * @return
	 */

	public boolean queryInCols(String engineId, String query) {
		// Check to see if one of the words in the query matches with a column name in
		// the app

		String[] queryPieces = query.split(" ");
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
		for (Object[] tableCol : allTableCols) {
			String table = tableCol[0] + "";
			String col = tableCol[1] + "";

			// check for word as column!
			for (String piece : queryPieces) {
				piece = piece.replaceAll(" ", "_");
				if (piece.equalsIgnoreCase(col) || piece.equalsIgnoreCase(col + "s")
						|| piece.equalsIgnoreCase(col + "es")) {
					return true;
				}
				if (piece.endsWith("ies")) {
					piece = piece.substring(0, piece.length() - 3) + "y";
					if (piece.equalsIgnoreCase(col) || piece.equalsIgnoreCase(col + "s")) {
						return true;
					}
				}
			}
		}

		return false;

	}

	/**
	 * Build the pixel based on the query struct and app id
	 * 
	 * @param qs
	 * @param appId
	 * @return
	 */

	public String buildPixelFromQs(SelectQueryStruct qs, String appId) {
		StringBuilder psb = new StringBuilder();
		// run default pixels setting up panel
		psb.append("AddPanel ( 0 ) ;");
		psb.append(
				"Panel ( 0 ) | AddPanelConfig ( config = [ { \"config\" : { \"type\" : \"STANDARD\" , \"opacity\" : 100 } } ] ) ;");
		psb.append(
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;");
		psb.append(
				"Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"<encode>{\"app_id\":\"NEWSEMOSSAPP\"}</encode>\" );");
		String frameName = "FRAME_" + Utility.getRandomString(5);
		psb.append("CreateFrame ( GRID ) .as ( [ '" + frameName + "' ] );");

		// pull from the appId
		psb.append("Database ( database = [ \"" + appId + "\" ] ) | ");

		// pull the correct columns
		List<IQuerySelector> selectors = qs.getSelectors();
		psb.append("Select ( ");
		String separator = "";
		for (IQuerySelector sel : selectors) {
			String selToAdd = sel.toString();
			psb.append(separator);
			separator = " , ";
			psb.append(selToAdd);
		}
		psb.append(" ) ");

		// alias the columns
		psb.append(".as ( [ ");
		separator = "";
		boolean agg = false;
		List<String> groupedCols = new ArrayList<String>();
		for (IQuerySelector sel : selectors) {
			String selAliasToAdd = sel.getAlias();
			psb.append(separator);
			separator = " , ";
			psb.append(selAliasToAdd);
			if (selAliasToAdd.contains("Average") || selAliasToAdd.contains("Sum") || selAliasToAdd.contains("Count")
					|| selAliasToAdd.contains("Min") || selAliasToAdd.contains("Max")
					|| selAliasToAdd.contains("UniqueCount")) {
				agg = true;
			} else {
				// prep for if we have to do a group
				groupedCols.add(sel.toString());
			}
		}
		psb.append(" ] ) | ");

		// if needing aggregate, create the group bys
		if (agg) {
			psb.append("Group ( ");
			separator = "";
			for (String col : groupedCols) {
				psb.append(separator);
				separator = " , ";
				psb.append(col);
			}
			psb.append(" ) | ");
		}

		// bring in the filters
		// TODO: Make this work for Having filters
		List<Map<String, Object>> filters = qs.getExplicitFilters().getFormatedFilters();
		filters.addAll(qs.getHavingFilters().getFormatedFilters());
		if (!filters.isEmpty()) {
			separator = "";
			psb.append("Filter ( ");
			System.out.println("");
			for (Map<String, Object> filter : filters) {
				String filterString = (String) filter.get("filterStr");
				String[] filterPieces = filterString.split(" ");
				String colName = filterPieces[0];
				String operator = filterPieces[1];
				String val = filterPieces[2];
				psb.append(separator);
				separator = " ) | Filter ( ";
				psb.append(colName);
				if (operator.equals("=") && !StringUtils.isNumber(val)) {
					operator = " == ";
					psb.append(operator);
					psb.append("[\"" + val + "\"] ");
				} else {
					operator = " " + operator + " ";
					psb.append(operator);
					psb.append(" " + val + " ");
				}

			}
			psb.append(") | ");
		}

		// bring in the relations
		Set<String[]> relations = qs.getRelations();
		if (!relations.isEmpty()) {
			separator = "";
			psb.append("Join ( ");
			for (String[] rel : relations) {
				String col1 = rel[0];
				String joinType = rel[1];
				String col2 = rel[2];
				psb.append(separator);
				separator = " , ";
				psb.append("( " + col1 + ", ");
				psb.append(joinType + ", ");
				psb.append(col2 + " ) ");
			}
			psb.append(") | ");
		}

		// final import statement
		psb.append("Import ( frame = [ " + frameName + " ] ) ;");

		// now that we have the data, visualize in grid
		psb.append("Panel ( 0 ) | SetPanelView ( \"visualization\" );");
		psb.append(
				"Frame () | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ;");

		return psb.toString();
	}

}