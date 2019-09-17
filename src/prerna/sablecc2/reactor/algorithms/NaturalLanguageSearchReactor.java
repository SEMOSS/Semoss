package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
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

public class NaturalLanguageSearchReactor extends AbstractRFrameReactor {

	/**
	 * Generates pixel to dynamically create insight based on Natural Language
	 * search
	 */

	protected static final String CLASS_NAME = NaturalLanguageSearchReactor.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static LinkedHashMap<String, String> appIdToTypeStore = new LinkedHashMap<String, String>(250);

	public NaturalLanguageSearchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		int stepCounter = 1;
		Logger logger = this.getLogger(CLASS_NAME);
		String query = this.keyValue.get(this.keysToGet[0]);
		List<String> engineFilters = getEngineIds();
		boolean hasFilters = !engineFilters.isEmpty();

		// Check Packages
		logger.info(stepCounter + ". Checking R Packages and Necessary Files");
		String[] packages = new String[] { "data.table", "plyr", "udpipe", "stringdist", "igraph", "SteinerNet" };
		this.rJavaTranslator.checkPackages(packages);

		// Check to make sure that needed files exist before searching
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File algo1 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "nli_db.R");
		File algo2 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "db_pixel.R");
		File algo3 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "english-ud-2.0-170801.udpipe");
		if (!algo1.exists() || !algo2.exists() || !algo3.exists()) {
			String message = "Necessary files missing to generate search results.";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		logger.info(stepCounter + ". Done");
		stepCounter++;

		// Generate string to initialize R console
		StringBuilder sb = new StringBuilder();
		logger.info(stepCounter + ". Loading R scripts to perform natural language search");
		String wd = "wd" + Utility.getRandomString(5);
		sb.append(wd + "<- getwd();");
		sb.append(("setwd(\"" + baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts\");").replace("\\", "/"));
		sb.append("source(\"nli_db.R\");");
		sb.append("source(\"db_pixel.R\");");
		this.rJavaTranslator.runR(sb.toString());
		logger.info(stepCounter + ". Done");
		stepCounter++;

		// Collect all the apps that we will iterate through
		logger.info(stepCounter + ". Collecting apps to iterate through");
		if (hasFilters) {
			// need to validate that the user has access to these ids
			if (AbstractSecurityUtils.securityEnabled()) {
				List<String> userIds = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
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
				engineFilters = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			}
		}
		logger.info(stepCounter + ". Done");
		stepCounter++;

		logger.info(stepCounter + ". Generating search results");
		List<Object[]> retData = generateAndRunScript(query, !hasFilters, engineFilters);
		logger.info(stepCounter + ". Done");

		logger.info(stepCounter + ". Generating pixel return from results");
		List<Map<String, Object>> returnPixels = generatePixels(retData, query);
		logger.info(stepCounter + ". Done");

		// reset working directory and run garbage cleanup
		this.rJavaTranslator.runR("setwd(\"" + wd + "\");");
		this.rJavaTranslator.executeEmptyR("rm(" + wd + ",build_join_clause," + "build_pixel,"
				+ "build_pixel_aggr_select," + "build_pixel_from," + "build_pixel_group," + "build_pixel_having,"
				+ "build_pixel_single_select," + "build_pixel_where," + "connect_tables," + "db_match," + "filter_apps,"
				+ "get_alias," + "get_conj," + "get_having," + "get_select," + "get_start," + "get_subtree,"
				+ "get_where," + "join_clause_mgr," + "map_aggr," + "map_dbitems," + "min_joins," + "nliapp_mgr,"
				+ "optimize_joins," + "parse_aggr," + "parse_question," + "parse_question_mgr," + "parse_request,"
				+ "refine_parsing," + "replace_words," + "select_having," + "select_where," + "select_where_helper,"
				+ "tag_dbitems," + "tagger," + "translate_token," + "trim," + "validate_pixel," + "validate_select,"
				+ "verify_joins," + "where_helper" + "); gc();");

		return new NounMetadata(returnPixels, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Generate the R script
	 */

	/**
	 * Generate the 2 data.tables based on the table structure and relationships and
	 * returns back the results from the algorithm
	 * 
	 * @param query
	 * @param allApps
	 * @param engineFilters
	 * @return
	 */
	private List<Object[]> generateAndRunScript(String query, boolean allApps, List<String> engineFilters) {
		String rSessionTable = "NaturalLangTable" + this.getSessionId().substring(0, 10);
		String rSessionJoinTable = "JoinTable" + this.getSessionId().substring(0, 10);
		String tempResult = "result" + Utility.getRandomString(8);
		String gc = tempResult;
		StringBuilder rsb = new StringBuilder();

		// Create session table to cache if session tables dont exist
		boolean tablesExist = this.rJavaTranslator.getBoolean("exists(\"" + rSessionTable + "\")");
		if (!tablesExist) {
			// first get the total number of cols and relationships
			List<Object[]> allTableCols = MasterDatabaseUtility
					.getAllTablesAndColumns(SecurityQueryUtils.getUserEngineIds(this.insight.getUser()));
			List<String[]> allRelations = MasterDatabaseUtility
					.getRelationships(SecurityQueryUtils.getUserEngineIds(this.insight.getUser()));
			int totalNumRels = allRelations.size();
			int totalColCount = allTableCols.size();

			// start building script
			StringBuilder sessionTableBuilder = new StringBuilder();
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
						rTableNames += "'" + table + "'";
						rColNames += "'" + column + "'";
						rColTypes += "'" + dataType + "'";
						rPrimKey += "'" + pk + "'";
					} else {
						rAppIds += ",'" + appId + "'";
						rTableNames += ",'" + table + "'";
						rColNames += ",'" + column + "'";
						rColTypes += ",'" + dataType + "'";
						rPrimKey += ",'" + pk + "'";
					}
				}
			}

			// create R vector of table columns and table rows
			String rAppIDs_join = "c(";
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
						rAppIDs_join += "'" + appId + "'";
						rTbl1 += "'" + sourceTable + "'";
						rTbl2 += "'" + targetTable + "'";
						rJoinBy1 += "'" + sourceColumn + "'";
						rJoinBy2 += "'" + targetColumn + "'";
					} else {
						rAppIDs_join += ",'" + appId + "'";
						rTbl1 += ",'" + sourceTable + "'";
						rTbl2 += ",'" + targetTable + "'";
						rJoinBy1 += ",'" + sourceColumn + "'";
						rJoinBy2 += ",'" + targetColumn + "'";
					}

					if (sourceColumn.endsWith("_FK")) {
						// if column ends with a _FK, then add it to NaturalLangTable also
						rAppIds += ",'" + appId + "'";
						rTableNames += ",'" + sourceTable + "'";
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
						rAppIDs_join += "'" + appId + "'";
						rTbl1 += "'" + sourceTable + "'";
						rTbl2 += "'" + targetTable + "'";
						rJoinBy1 += "'" + sourceColumn + "'";
						rJoinBy2 += "'" + targetColumn + "'";
					} else {
						rAppIDs_join += ",'" + appId + "'";
						rTbl1 += ",'" + sourceTable + "'";
						rTbl2 += ",'" + targetTable + "'";
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
			rAppIDs_join += ")";
			rTbl1 += ")";
			rTbl2 += ")";
			rJoinBy1 += ")";
			rJoinBy2 += ")";

			// create the session tables
			sessionTableBuilder.append(rSessionTable + " <- data.frame(Column = " + rColNames + " , Table = "
					+ rTableNames + " , AppID = " + rAppIds + ", Datatype = " + rColTypes + ", Key = " + rPrimKey
					+ ", stringsAsFactors = FALSE);");
			sessionTableBuilder.append(rSessionJoinTable + " <- data.frame(tbl1 = " + rTbl1 + " , tbl2 = " + rTbl2
					+ " , joinby1 = " + rJoinBy1 + " , joinby2 = " + rJoinBy2 + " , AppID = " + rAppIDs_join
					+ ", stringsAsFactors = FALSE);");
			this.rJavaTranslator.runR(sessionTableBuilder.toString());
		}

		if (allApps) {
			// lets run the function on all apps (i.e. the session tables)
			rsb.append(tempResult + " <- nliapp_mgr_global(\"" + query + "\"," + rSessionTable + "," + rSessionJoinTable
					+ ");");

		} else {
			// filter the session tables to only the ones needed and run function
			String rTempTable = "NaturalLangTable" + Utility.getRandomString(8);
			String rTempJoinTable = "JoinTable" + Utility.getRandomString(8);

			// create the app list to filter to in R
			String appFilters = "appFilters" + Utility.getRandomString(8);
			rsb.append(appFilters + " <- c(");
			String comma = "";
			for (String appId : engineFilters) {
				rsb.append(comma + " \"" + appId + "\" ");
				comma = " , ";
			}
			rsb.append(");");

			// filter the session tables
			rsb.append(rTempTable + " <- " + rSessionTable + "[" + rSessionTable + "$AppID %in% " + appFilters + " ,];");
			rsb.append(rTempJoinTable + " <- " + rSessionJoinTable + "[" + rSessionJoinTable + "$AppID %in% " + appFilters + " ,];");

			// run the function
			rsb.append(tempResult + " <- nliapp_mgr_global(\"" + query + "\"," + rTempTable + "," + rTempJoinTable + ");");
			gc += (" , " + rTempTable + " , " + rTempJoinTable + " , " + appFilters);
		}
		
		this.rJavaTranslator.runR(rsb.toString());
		
		// get back the data
		String[] headerOrdering = new String[] { "label", "appid", "appid2", "part", "item1", "item2", "item3", "item4",
				"item5", "item6", "item7", "phase", "pick" };
		List<Object[]> list = this.rJavaTranslator.getBulkDataRow(tempResult, headerOrdering);

		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + gc + "); gc();");

		return list;
	}

	/**
	 * Generate the maps with the query information
	 * 
	 * @param retData
	 * @param queryInput
	 * @return
	 */
	private List<Map<String, Object>> generatePixels(List<Object[]> retData, String queryInput) {
		// we do not know how many rows associate with the same QS
		// but we know the algorithm only returns one QS per engine
		// and the rows are ordered with regards to how the engine comes back
		Map<String, SelectQueryStruct> qsList = new LinkedHashMap<String, SelectQueryStruct>();
		// when this value doesn't match the previous, we know to add a new QS
		String currAppId = null;
		String label = null;
		Collection<String> primKeys = null;
		SelectQueryStruct curQs = null;

		// need to store the collection of "Combined" qs's and their joins that I am
		// holding to make sure I don't duplicate and can also use this to push/pull to add additional rows 
		Map<String, SelectQueryStruct> combinedQs = new HashMap<String, SelectQueryStruct>();
		
		// use the joinCombinedResult to merge in the pixel later
		List<Object[]> joinCombinedResult = new Vector<Object[]>();
		
		// use the these vectors to handle grouping/having/dropping unneeded cols
		List<Object[]> aggregateCols = new Vector<Object[]>();
		List<Object[]> combinedHavingRows = new Vector<Object[]>();
		LinkedHashSet<String> colsToDrop = new LinkedHashSet<String>();
		LinkedHashSet<String> pickedCols = new LinkedHashSet<String>();
		LinkedHashSet<String> groupedCols = new LinkedHashSet<String>();

		for (int i = 0; i < retData.size(); i++) {
			Object[] row = retData.get(i);
			// if it is an error
			// continue through the loop
			String part = row[3].toString();
			String rowLabel = row[0].toString();
			boolean combined = rowLabel.equals("combined");
			if (part.equalsIgnoreCase("error")) {
				continue;
			}
			if (label == null || !label.equals(rowLabel)) {
				label = rowLabel;
			}

			// figure out whether this row is the first of a new qs
			String rowAppId = row[1].toString();
			if (combined && !combinedQs.containsKey(rowAppId)) {
				// this is the combined result where the qs is not created yet
				// meaning it is the first of a certain select of a combined entry
				currAppId = rowAppId;
				curQs = new SelectQueryStruct();
				curQs.setEngineId(currAppId);
				curQs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qsList.put("Multiple" + combinedQs.size(), curQs);
				combinedQs.put(currAppId, curQs);
				primKeys = MasterDatabaseUtility.getPKColumnsWithData(currAppId);
			} else if (!combined && currAppId == null) {
				// this is the first one of a non-combined
				currAppId = rowAppId;
				curQs = new SelectQueryStruct();
				curQs.setEngineId(currAppId);
				curQs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qsList.put(label, curQs);
				primKeys = MasterDatabaseUtility.getPKColumnsWithData(currAppId);
			} else if (!combined && !currAppId.equals(rowAppId)) {
				// okay this row is now starting a new QS
				// we gotta init another one
				currAppId = rowAppId;
				curQs = new SelectQueryStruct();
				curQs.setEngineId(currAppId);
				curQs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qsList.put(label, curQs);
				primKeys = MasterDatabaseUtility.getPKColumnsWithData(currAppId);

			}

			// if this is a combined row, pull the qs that matches the appid
			if (combined) {
				currAppId = rowAppId;
				curQs = combinedQs.get(currAppId);
			}
			
			// check what type of row it is, then add to qs by case
			if (part.equalsIgnoreCase("select")) {
				String selectConcept = row[4].toString();
				String selectProperty = row[5].toString();
				boolean agg = !row[6].toString().isEmpty();

				IQuerySelector selector = null;
				// TODO: need to properly send/receive null values in case there is a
				// property with the same name as the node
				if (primKeys.contains(selectProperty)) {
					selector = new QueryColumnSelector(selectConcept);
				} else {
					selector = new QueryColumnSelector(selectConcept + "__" + selectProperty);
				}

				// grab the pick cols and otherwise get columns to drop
				// do not need to add agg columns at this point
				if (!agg && combined && row[12].toString().equals("yes")) {
					pickedCols.add(selector.getAlias());
					groupedCols.add(selector.getAlias());
				} else if (agg && combined && row[12].toString().equals("yes")) {
					pickedCols.add(row[6] + "_" + row[5]);
				} else if (combined && !row[12].toString().equals("yes")) {
					colsToDrop.add(selector.getAlias());
				}

				if (agg) {
					// if it is combined, then just import the data for now and save the agg for later
					if (combined) {
						curQs.addSelector(selector);
						aggregateCols.add(row);
					} else {
						QueryFunctionSelector fSelector = new QueryFunctionSelector();
						fSelector.setFunction(row[6].toString());
						fSelector.addInnerSelector(selector);
						// add the selector
						curQs.addSelector(fSelector);
					}
				} else {
					curQs.addSelector(selector);
				}

			} else if (part.equalsIgnoreCase("from")) {
				// if the two appids are filled in but are not equal, this is a join across query structures
				// therefore, do not add relation but add to a list to be used later
				if (!row[1].equals(row[2]) && !row[1].toString().isEmpty() && !row[2].toString().isEmpty()) {
					// store this row to help build merge pixel later
					joinCombinedResult.add(row);
				} else if (!row[5].toString().isEmpty()) {
					// this is a join within the same database
					String fromConcept = row[4].toString();
					String toConcept = row[6].toString();
					String joinType = "inner.join";
					curQs.addRelation(fromConcept, toConcept, joinType);
				}

			} else if (part.equalsIgnoreCase("where")) {
				String whereTable = row[4].toString();
				String whereCol = row[5].toString();
				String comparator = row[6].toString();
				// if where value 2 is empty
				// where value is a scalar
				// if where value 2 is not empty
				// what means where value is a table name
				// and where value 2 is a column name
				Object whereValue = row[7];
				Object whereValue2 = row[8];
				Object whereValueAgg = row[9];

				// if it is a table
				// we do not know the correct primary key
				// so we exec a query to determine if we should use the current selectedProperty
				// or keep it as PRIM_KEY_PLACEHOLDER
				IQuerySelector selector = null;
				if (primKeys.contains(whereCol)) {
					selector = new QueryColumnSelector(whereTable);
				} else {
					selector = new QueryColumnSelector(whereTable + "__" + whereCol);
				}
				NounMetadata lhs = new NounMetadata(selector, PixelDataType.COLUMN);

				if (!whereValueAgg.toString().isEmpty()) {
					// let us address the portion when we have a
					// min or max on another column
					// so WhereValueAgg is min/max
					// WhereValue is Table and WhereValue2 is Column

					QueryFunctionSelector fSelector = new QueryFunctionSelector();
					fSelector.setFunction(whereValueAgg.toString());
					fSelector.addInnerSelector(new QueryColumnSelector(whereValue + "__" + whereValue2));
					// add the selector
					curQs.addSelector(fSelector);

					// add rhs of where
					NounMetadata rhs = new NounMetadata(fSelector, PixelDataType.COLUMN);
					SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
					curQs.addExplicitFilter(filter);

				} else if (!whereValue2.toString().isEmpty() && !comparator.equals("between")) {
					// let us address the portion when we have another column
					// so whereValue2 is empty and comparator is not between

					// my rhs is another column
					NounMetadata rhs = new NounMetadata(new QueryColumnSelector(whereValue + "__" + whereValue2),
							PixelDataType.COLUMN);
					// add this filter
					SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
					curQs.addExplicitFilter(filter);
				} else {
					// we have to consider the comparators
					// so i can do the correct types
					if (comparator.contains(">") || comparator.contains("<")) {
						// it must numeric
						NounMetadata rhs = new NounMetadata(whereValue, PixelDataType.CONST_DECIMAL);

						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
						curQs.addExplicitFilter(filter);
					} else if (comparator.equals("between")) {
						// still numeric
						// but i need 2 filters

						// add the lower bound filter
						NounMetadata rhs = new NounMetadata(whereValue, PixelDataType.CONST_DECIMAL);
						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, ">", rhs);
						curQs.addExplicitFilter(filter);

						// add the upper bound filter
						rhs = new NounMetadata(whereValue2, PixelDataType.CONST_DECIMAL);
						// add this filter
						filter = new SimpleQueryFilter(lhs, "<", rhs);
						curQs.addExplicitFilter(filter);
					} else {
						PixelDataType type = PixelDataType.CONST_STRING;
						if (whereValue instanceof Number) {
							type = PixelDataType.CONST_DECIMAL;
						}

						NounMetadata rhs = new NounMetadata(whereValue, type);
						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
						curQs.addExplicitFilter(filter);
					}
				}
			} else if (part.equalsIgnoreCase("having")) {
				// if it is a combined having, store this row and handle later
				if (combined) {
					combinedHavingRows.add(row);
					continue;
				}
				String havingTable = row[4].toString();
				String havingCol = row[5].toString();
				String havingAgg = row[6].toString();
				String comparator = row[7].toString();
				// if having value 2 is empty
				// having value is a scalar
				// if having value 2 is not empty
				// that means having value is a table name
				// and having value 2 is a column name
				// and having value agg is the aggregate function

				Object havingValue = row[8];
				Object havingValue2 = row[9];
				Object havingValueAgg = row[10];

				// if it is a table
				// we do not know the correct primary key
				// so we exec a query to determine if we should use the current selectedProperty
				// or keep it as PRIM_KEY_PLACEHOLDER
				IQuerySelector selector = null;
				if (primKeys.contains(havingCol)) {
					selector = new QueryColumnSelector(havingTable);
				} else {
					selector = new QueryColumnSelector(havingTable + "__" + havingCol);
				}
				QueryFunctionSelector fSelector = new QueryFunctionSelector();
				fSelector.setFunction(havingAgg);
				fSelector.addInnerSelector(selector);
				// add the selector
				// curQs.addSelector(fSelector);

				// add lhs of having
				NounMetadata lhs = new NounMetadata(fSelector, PixelDataType.COLUMN);

				// add rhs of having
				// let us first address the portion when we have another aggregate
				if (!havingValueAgg.toString().isEmpty()) {
					// THIS DOESN'T WORK VERY WELL... COMPLICATED QUERY THAT REQUIRES A SUBQUERY
					if (havingValueAgg.toString().equalsIgnoreCase("max")) {
						// add an order + limit
						curQs.setLimit(1);
						QueryColumnOrderBySelector orderBy = new QueryColumnOrderBySelector(
								havingAgg + "(" + havingTable + "__" + havingCol + ")");
						orderBy.setSortDir(QueryColumnOrderBySelector.ORDER_BY_DIRECTION.DESC.toString());
						curQs.addOrderBy(orderBy);
					} else if (havingValueAgg.toString().equalsIgnoreCase("min")) {
						// add an order + limit
						curQs.setLimit(1);
						QueryColumnOrderBySelector orderBy = new QueryColumnOrderBySelector(
								havingAgg + "(" + havingTable + "__" + havingCol + ")");
						curQs.addOrderBy(orderBy);
					}

					// my rhs is another column agg
					IQuerySelector selectorR = null;
					// TODO: need to properly send/receive null values in case there is a
					// property with the same name as the node
					if (primKeys.contains(havingValue2.toString())) {
						selector = new QueryColumnSelector(havingTable);
					} else {
						selector = new QueryColumnSelector(havingTable + "__" + havingValue2);
					}

					QueryFunctionSelector fSelectorR = new QueryFunctionSelector();
					fSelectorR.setFunction(havingValueAgg.toString());
					fSelectorR.addInnerSelector(selectorR);

					// add this filter
					NounMetadata rhs = new NounMetadata(fSelectorR, PixelDataType.COLUMN);
					SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
					curQs.addHavingFilter(filter);
				} else {
					// we have to consider the comparators
					// so i can do the correct types
					if (comparator.contains(">") || comparator.contains("<")) {
						// it must numeric
						NounMetadata rhs = new NounMetadata(havingValue, PixelDataType.CONST_DECIMAL);

						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
						curQs.addHavingFilter(filter);

					} else if (comparator.equals("between")) {
						// still numeric
						// but i need 2 filters

						// add the lower bound filter
						NounMetadata rhs = new NounMetadata(havingValue, PixelDataType.CONST_DECIMAL);
						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, ">", rhs);
						curQs.addHavingFilter(filter);

						// add the upper bound filter
						rhs = new NounMetadata(havingValue2, PixelDataType.CONST_DECIMAL);
						// add this filter
						filter = new SimpleQueryFilter(lhs, "<", rhs);
						curQs.addHavingFilter(filter);

					} else {
						// this must be an equals or not equals...

						PixelDataType type = PixelDataType.CONST_STRING;
						if (havingValue instanceof Number) {
							type = PixelDataType.CONST_DECIMAL;
						}

						NounMetadata rhs = new NounMetadata(havingValue, type);
						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
						curQs.addHavingFilter(filter);

					}
				}

			} else if (part.equalsIgnoreCase("group")) {
				String groupConcept = row[4].toString();
				String groupProperty = row[5].toString();
				// do not group the havings in this portion
				if (combined) {
					continue;
				} else {
					// if it is a table
					// we do not know the correct primary key
					// so we exec a query to determine if we should use the current selectedProperty
					// or keep it as PRIM_KEY_PLACEHOLDER
					if (primKeys.contains(groupProperty)) {
						curQs.addGroupBy(groupConcept, null);
					} else {
						curQs.addGroupBy(groupConcept, groupProperty);
					}
				}
			}
		}
		
		// retMap is full of maps with key = label and value = pixel
		List<Map<String, Object>> retMap = new Vector<Map<String, Object>>();
		Map<String, Object> map = new HashMap<String, Object>();
		
		// track when the entry changes and setup other vars
		String curEntry = null;
		String frameName = "FRAME_" + Utility.getRandomString(5);
		String finalPixel = "";
		LinkedHashSet<String> prevAppIds = new LinkedHashSet<String>();
		int entryCount = 1;

		for (Entry<String, SelectQueryStruct> entry : qsList.entrySet()) {
			// first lets check if it is combined
			if (entry.getKey().contains("Multiple")) {
				// if this is the first instance of a combined result, then start a new map
				if (curEntry == null || !curEntry.contains("Multiple")) {
					// start the new map
					map = new HashMap<String, Object>();
					curEntry = entry.getKey();
					finalPixel = "";

					// process the qs
					SelectQueryStruct qs = entry.getValue();
					finalPixel += buildImportPixelFromQs(qs, qs.getEngineId(), frameName, false);

					// in the case where there is only one combined qs, lets return
					if (entryCount == qsList.entrySet().size()) {
						// return map
						map.put("app_id", "Multiple Apps");
						map.put("app_name", "Multiple Apps");
						map.put("frame_name", frameName);
						finalPixel += dropUnwantedCols(colsToDrop, groupedCols);
						finalPixel += addGroupingsAndHavings(aggregateCols, groupedCols, combinedHavingRows);
						finalPixel += "Panel ( 0 ) | SetPanelLabel(\"Multiple Apps: " + queryInput + "\");";
						finalPixel += "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;";
						finalPixel += (frameName + " | PredictViz(app=[\"Multiple\"],columns=" + pickedCols + ");");
						map.put("pixel", getStartPixel(frameName) + finalPixel);
						map.put("layout", "NLP");
						map.put("columns", pickedCols);
						retMap.add(map);

					}
					// store the previous app id for when we join across db's later
					prevAppIds.add(qs.getEngineId());
					entryCount++;

				}
				// if this is the last result, lets return the map. we are done
				else if (entryCount == qsList.entrySet().size()) {
					// process the qs
					SelectQueryStruct qs = entry.getValue();
					finalPixel += buildImportPixelFromQs(qs, qs.getEngineId(), frameName, true);
					finalPixel += addMergePixel(qs, prevAppIds , joinCombinedResult, frameName);
					
					// return map
					map.put("app_id", "Multiple Apps");
					map.put("app_name", "Multiple Apps");
					map.put("frame_name", frameName);
					finalPixel += dropUnwantedCols(colsToDrop, groupedCols);
					finalPixel += addGroupingsAndHavings(aggregateCols, groupedCols, combinedHavingRows);
					finalPixel += "Panel ( 0 ) | SetPanelLabel(\"Multiple Apps: " + queryInput + "\");";
					finalPixel += "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;";
					finalPixel += (frameName + " | PredictViz(app=[\"Multiple\"],columns=" + pickedCols + ");");
					map.put("pixel", getStartPixel(frameName) + finalPixel);
					map.put("layout", "NLP");
					map.put("columns", pickedCols);
					retMap.add(map);

				}
				// this is a continuation of a previous result
				else {
					// add to the existing pixel
					SelectQueryStruct qs = entry.getValue();
					finalPixel += buildImportPixelFromQs(qs, qs.getEngineId(), frameName, true);
					finalPixel += addMergePixel(qs, prevAppIds, joinCombinedResult, frameName);
					entryCount++;
					
					// store the previous app id for when we join across db's later
					prevAppIds.add(qs.getEngineId());
				}
			} else {
				// if the result is not combined, then there is only one qs
				// put it in the map and then return
				curEntry = entry.getKey();
				map = new HashMap<String, Object>();
				SelectQueryStruct qs = entry.getValue();
				String appId = qs.getEngineId();
				String appName = MasterDatabaseUtility.getEngineAliasForId(appId);
				map.put("app_id", appId);
				map.put("app_name", appName);
				map.put("frame_name", frameName);
				finalPixel = buildImportPixelFromQs(qs, appId, frameName, false);
				finalPixel += "Panel ( 0 ) | SetPanelLabel(\"" + appName + ": " + queryInput + "\");";
				finalPixel += "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;";
				finalPixel += (frameName + " | PredictViz(app=[\"" + appId + "\"],columns=" + getSelectorAliases(qs.getSelectors()) + ");");
				map.put("pixel", getStartPixel(frameName) + finalPixel);
				map.put("layout", "NLP");
				map.put("columns", getSelectorAliases(qs.getSelectors()));
				retMap.add(map);
				finalPixel = "";
				entryCount++;

			}
		}
		
		// return the map
		return retMap;
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/**
	 * Build the pixel based on the query struct and app id
	 * 
	 * @param qs
	 * @param appId
	 * @param merge
	 * @return
	 */
	public String buildImportPixelFromQs(SelectQueryStruct qs, String appId, String frameName, boolean merge) {
		StringBuilder psb = new StringBuilder();
		QUERY_STRUCT_TYPE type = qs.getQsType();

		if (type == QUERY_STRUCT_TYPE.ENGINE) {
			// pull from the appId
			psb.append("Database ( database = [ \"" + appId + "\" ] ) | ");
		} else if (type == QUERY_STRUCT_TYPE.FRAME) {
			psb.append("Frame ( ) | ");
		}

		// pull the correct columns
		Map<String, String> qsToAlias = new HashMap<String, String>();
		List<IQuerySelector> selectors = qs.getSelectors();
		StringBuilder aliasStringBuilder = new StringBuilder();
		aliasStringBuilder.append(".as ( [ ");
		psb.append("Select ( ");
		String separator = "";

		// loop through the selectors and store their name and alias
		for (IQuerySelector sel : selectors) {
			String selToAdd = sel.toString();
			String selAliasToAdd = sel.getAlias();
			psb.append(separator);
			aliasStringBuilder.append(separator);
			separator = " , ";
			psb.append(selToAdd);
			aliasStringBuilder.append(selAliasToAdd);
			// track list in case we need it
			qsToAlias.put(sel.getQueryStructName().toUpperCase(), selAliasToAdd);
		}
		aliasStringBuilder.append(" ] ) | ");
		psb.append(" ) ");
		psb.append(aliasStringBuilder);

		// bring in the group bys
		List<QueryColumnSelector> groupList = qs.getGroupBy();
		// loop through the groups and store their name and alias
		if (!groupList.isEmpty()) {
			psb.append("Group ( ");
			separator = "";
			for (IQuerySelector group : groupList) {
				psb.append(separator);
				separator = " , ";
				psb.append(group.toString());
			}
			psb.append(" ) | ");
		}

		// bring in the filters
		List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
		if (!filters.isEmpty()) {
			for (IQueryFilter f : filters) {
				// start a new filter
				psb.append("Filter ( ");

				// assume we only have simple
				SimpleQueryFilter simpleF = (SimpleQueryFilter) f;
				// left hand side is always a column
				NounMetadata lhs = simpleF.getLComparison();
				psb.append(lhs.getValue() + "");
				if (simpleF.getComparator().equals("=")) {
					psb.append(" == ");
				} else {
					psb.append(" ").append(simpleF.getComparator()).append(" ");
				}
				NounMetadata rhs = simpleF.getRComparison();
				PixelDataType rhsType = rhs.getNounType();
				if (rhsType == PixelDataType.COLUMN) {
					psb.append(rhs.getValue() + "");
				} else if (rhsType == PixelDataType.CONST_STRING) {
					Object val = rhs.getValue();
					if (val instanceof List) {
						List<String> vList = (List<String>) val;
						int size = vList.size();
						psb.append("[");
						for (int i = 0; i < size; i++) {
							if (i == 0) {
								psb.append("\"").append(vList.get(i)).append("\"");
							} else {
								psb.append(",\"").append(vList.get(i)).append("\"");
							}
						}
						psb.append("]");
					} else {
						// if it is an RDF database make sure that the wild card is *
						String value = rhs.getValue().toString();
						if (value.contains("%") && getAppTypeFromId(appId).equals("TYPE:RDF")) {
							value = value.replaceAll("%", "/.*");
						}
						psb.append("\"" + value + "\"");
					}
				} else {
					Object val = rhs.getValue();
					if (val instanceof List) {
						List<String> vList = (List<String>) val;
						int size = vList.size();
						psb.append("[");
						for (int i = 0; i < size; i++) {
							if (i == 0) {
								psb.append(vList.get(i));

							} else {
								psb.append(", ").append(vList.get(i));
							}
						}
						psb.append("]");
					} else {
						psb.append(rhs.getValue() + "");
					}
				}

				// close this filter
				psb.append(") | ");
			}
		}

		// bring in the having filters
		List<IQueryFilter> havingFilters = qs.getHavingFilters().getFilters();
		if (!havingFilters.isEmpty()) {
			// start a new filter
			for (IQueryFilter f : havingFilters) {
				psb.append("Having ( ");

				// assume we only have simple
				SimpleQueryFilter simpleF = (SimpleQueryFilter) f;

				// left hand side is always an aggregate column
				NounMetadata lhs = simpleF.getLComparison();
				psb.append(lhs.getValue() + "");

				if (simpleF.getComparator().equals("=")) {
					psb.append(" == ");
				} else {
					psb.append(" ").append(simpleF.getComparator()).append(" ");
				}

				// right hand side can be many things
				NounMetadata rhs = simpleF.getRComparison();
				PixelDataType rhsType = rhs.getNounType();
				if (rhsType == PixelDataType.COLUMN) {
					psb.append(rhs.getValue() + "");
				} else if (rhsType == PixelDataType.CONST_STRING) {
					Object val = rhs.getValue();
					if (val instanceof List) {
						List<String> vList = (List<String>) val;
						int size = vList.size();
						psb.append("[");
						for (int i = 0; i < size; i++) {
							if (i == 0) {
								psb.append("\"").append(vList.get(i)).append("\"");

							} else {
								psb.append(",\"").append(vList.get(i)).append("\"");
							}
						}
						psb.append("]");
					} else {
						psb.append("\"" + rhs.getValue() + "\"");
					}
				} else {
					// it is a number
					Object val = rhs.getValue();
					if (val instanceof List) {
						List<String> vList = (List<String>) val;
						int size = vList.size();
						psb.append("[");
						for (int i = 0; i < size; i++) {
							if (i == 0) {
								psb.append(vList.get(i));

							} else {
								psb.append(", ").append(vList.get(i));
							}
						}
						psb.append("]");
					} else {
						psb.append(rhs.getValue() + "");
					}
				}

				// close this filter
				psb.append(") | ");
			}
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

		List<QueryColumnOrderBySelector> orderBys = qs.getOrderBy();
		if (orderBys != null && !orderBys.isEmpty()) {
			StringBuilder b = new StringBuilder();
			StringBuilder b2 = new StringBuilder();
			int i = 0;
			for (QueryColumnOrderBySelector orderBy : orderBys) {
				if (i > 0) {
					b.append(", ");
					b2.append(", ");
				}
				if (qsToAlias.containsKey(orderBy.getQueryStructName().toUpperCase())) {
					b.append(qsToAlias.get(orderBy.getQueryStructName().toUpperCase()));
				} else {
					b.append(orderBy.getQueryStructName());
				}
				b2.append(orderBy.getSortDirString());
				i++;
			}
			psb.append("Order(columns=[").append(b.toString()).append("], sort=[").append(b2.toString())
					.append("]) | ");
		}

		if (qs.getLimit() > 0) {
			psb.append("Limit(").append(qs.getLimit()).append(") | ");
		}

		// final import statement
		if (!merge) {
			psb.append("Import ( frame = [ " + frameName + " ] ) ;");

		}

		// return the pixel
		return psb.toString();
	}
	
	
	/**
	 * get the pixel to merge the db's together
	 * @param qs 
	 * @param prevAppId -- to make sure its the correct join 
	 * @param joinCombinedResults -- the rows to join across
	 * @param frameName -- to perform the  join pixel
	 * @param qs 
	 * 
	 * @return
	 */
	private String addMergePixel(SelectQueryStruct qs, LinkedHashSet<String> prevAppIds, List<Object[]> joinCombinedResult, String frameName) {
		// Merge ( joins = [ ( System , right.outer.join , EKTROPY_ITEMS_0722__System )
		// ] ) ;
		String appId = qs.getEngineId();
		String mergeCol = "";
		String mergeString = "Merge (joins = [ (";
		for (Object[] joinRow : joinCombinedResult) {
			// figure out which qs needs to merge, whether
			// its first, second, what the column is, etc.
			if(joinRow[1].equals(appId) && prevAppIds.contains(joinRow[2].toString())) {
				mergeCol = joinRow[5].toString();
			} else if (prevAppIds.contains(joinRow[1].toString()) && joinRow[2].equals(appId)) {
				mergeCol = joinRow[7].toString();
			}  
		}
		mergeString += mergeCol + " , inner.join , " + mergeCol + " ) ] );";
		return mergeString;
	}
	
	
	/**
	 * get the pre-data import pixel
	 * @param frameName
	 * 
	 * @return
	 */
	private String getStartPixel(String frameName) {
		
		String startPixel = "AddPanel ( 0 ) ;";
		startPixel += "Panel ( 0 ) | AddPanelConfig ( config = [ { \"config\" : { \"type\" : \"STANDARD\" , \"opacity\" : 100 } } ] ) ;";
		startPixel += "Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , "
				+ "\"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" :"
				+ " \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , "
				+ "\"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ; Panel ( 0 ) | RetrievePanelEvents ( ) ;";
		startPixel += "CreateFrame ( R ) .as ( [ '" + frameName + "' ] );";
		
		return startPixel;
	}
	
	
	/**
	 * Drop the columns that were not "picked"
	 * @param colsToDrop -- columns that were not picked by the query
	 * @param groupedCols -- groupedCols to double check that they werent picked elsewhere
	 * @return
	 */
	private String dropUnwantedCols(LinkedHashSet<String> colsToDrop, LinkedHashSet<String> groupedCols) {
		StringBuilder psb = new StringBuilder();
		String colDropString = "";
		boolean dropAtLeastOne = false;

		// lets build the string
		String comma = "";
		for (String col : colsToDrop) {
			// make sure that it wasn't "picked" somewhere else
			if (groupedCols.contains(col)) {
				continue;
			}
			dropAtLeastOne = true;
			colDropString += comma + "\"" + col + "\"";
			comma = " , ";
		}

		// Now lets drop the columns that was the aggregate
		if (dropAtLeastOne) {
			psb.append("DropColumn ( columns = [ ");
			psb.append(colDropString);
			psb.append(" ] );");
		}

		return psb.toString();
	}
	
	
	/**
	 * Get the selectors' aliases as a list
	 * @param aggregateCols -- rows that were aggregates in the R return
	 * @param groupedCols -- the columns that we are grouping the aggregates on
	 * @return
	 */
	private String addGroupingsAndHavings(List<Object[]> aggregateCols, LinkedHashSet<String> groupedCols,
			List<Object[]> combinedHavingRows) {
		// if there were no aggregates, then ignore this
		if (aggregateCols == null || aggregateCols.isEmpty()) {
			return "";
		}

		// create the frame qs and other vars
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.FRAME);
		String colDropString = "";
		String mergeString = "Merge ( joins = [ ";

		// add the selectors for the groupedCols
		// also add them into groupby
		String comma = "";
		for (String col : groupedCols) {
			qs.addSelector(new QueryColumnSelector(col));
			qs.addGroupBy(col, null);
			mergeString += comma + " ( " + col + " , inner.join , " + col + " ) ";
			comma = ",";
			// qs.addRelation(col, col, "inner.join");
		}
		mergeString += "] ) ; ";

		// add the selectors for the aggregate columns
		comma = "";
		for (Object[] aggCol : aggregateCols) {
			QueryFunctionSelector fSelector = new QueryFunctionSelector();
			fSelector.setFunction(aggCol[6].toString());
			fSelector.addInnerSelector(new QueryColumnSelector(aggCol[5].toString()));
			// add the selector
			qs.addSelector(fSelector);

			// also build the string to drop this column
			colDropString += comma + "\"" + aggCol[5].toString() + "\"";
			comma = " , ";
		}

		for (Object[] row : combinedHavingRows) {
			String lCol = row[6] + "_" + row[5];
			String comparator = row[7].toString();

			// if having value 2 is empty
			// having value is a scalar
			// if having value 2 is not empty
			// that means having value is a table name
			// and having value 2 is a column name
			// and having value agg is the aggregate function

			Object havingValue = row[8];
			Object havingValue2 = row[9];
			Object havingValueAgg = row[10];

			// if it is a table
			// we do not know the correct primary key
			// so we exec a query to determine if we should use the current selectedProperty
			// or keep it as PRIM_KEY_PLACEHOLDER
			IQuerySelector selector = new QueryColumnSelector(lCol);

			// add lhs of having
			NounMetadata lhs = new NounMetadata(selector, PixelDataType.COLUMN);

			// add rhs of having
			// let us first address the portion when we have another aggregate
			if (!havingValueAgg.toString().isEmpty()) {
				// my rhs is another column agg
				IQuerySelector selectorR = new QueryColumnSelector(havingValue2.toString());
				QueryFunctionSelector fSelectorR = new QueryFunctionSelector();
				fSelectorR.setFunction(havingValueAgg.toString());
				fSelectorR.addInnerSelector(selectorR);

				// add this filter
				NounMetadata rhs = new NounMetadata(fSelectorR, PixelDataType.COLUMN);
				SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
				qs.addHavingFilter(filter);
			} else {
				// we have to consider the comparators
				// so i can do the correct types
				if (comparator.contains(">") || comparator.contains("<")) {
					// it must numeric
					NounMetadata rhs = new NounMetadata(havingValue, PixelDataType.CONST_DECIMAL);

					// add this filter
					SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
					qs.addHavingFilter(filter);

				} else if (comparator.equals("between")) {
					// still numeric
					// but i need 2 filters

					// add the lower bound filter
					NounMetadata rhs = new NounMetadata(havingValue, PixelDataType.CONST_DECIMAL);
					// add this filter
					SimpleQueryFilter filter = new SimpleQueryFilter(lhs, ">", rhs);
					qs.addHavingFilter(filter);

					// add the upper bound filter
					rhs = new NounMetadata(havingValue2, PixelDataType.CONST_DECIMAL);
					// add this filter
					filter = new SimpleQueryFilter(lhs, "<", rhs);
					qs.addHavingFilter(filter);

				} else {
					// this must be an equals or not equals...

					PixelDataType type = PixelDataType.CONST_STRING;
					if (havingValue instanceof Number) {
						type = PixelDataType.CONST_DECIMAL;
					}

					NounMetadata rhs = new NounMetadata(havingValue, type);
					// add this filter
					SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
					qs.addHavingFilter(filter);

				}
			}

		}

		// create the string and run it
		StringBuilder psb = new StringBuilder();
		psb.append(buildImportPixelFromQs(qs, null, null, true));
		psb.append(mergeString);

		// Now lets drop the columns that was the aggregate
		psb.append("DropColumn ( columns = [ ");
		psb.append(colDropString);
		psb.append(" ] );");

		// return
		return psb.toString();
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Get input from noun store
	 */

	/**
	 * Get input engine ids
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
	 * Get the selectors' aliases as a list
	 * @param qs selectors
	 * @return
	 */
	private List<String> getSelectorAliases(List<IQuerySelector> selectors) {
		List<String> aliases = new Vector<String>();
		for (IQuerySelector sel : selectors) {
			aliases.add(sel.getAlias());
		}
		return aliases;
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Utility
	 */

	/**
	 * Utilize a static cache so we do not query local master to get the type of an
	 * engine every time
	 * 
	 * @param appId
	 * @return
	 */
	private static String getAppTypeFromId(String appId) {
		String type = appIdToTypeStore.get(appId);
		if (type != null) {
			return type;
		}

		// store the result so we don't need to query all the time
		type = MasterDatabaseUtility.getEngineTypeForId(appId);
		appIdToTypeStore.put(appId, type);
		if (appIdToTypeStore.size() > 200) {
			synchronized (appIdToTypeStore) {
				if (appIdToTypeStore.size() > 100) {
					// it should be ordered from first to last
					Iterator<String> it = appIdToTypeStore.keySet().iterator();
					int counter = 0;
					while (it.hasNext() && counter < 100) {
						appIdToTypeStore.remove(it.next());
					}
				}
			}
		}
		return type;
	}

}