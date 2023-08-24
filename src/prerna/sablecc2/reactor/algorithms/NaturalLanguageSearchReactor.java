package prerna.sablecc2.reactor.algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NaturalLanguageSearchReactor extends AbstractRFrameReactor {

	/**
	 * Generates pixel to dynamically create insight based on Natural Language
	 * search
	 */

	private static final Logger classLogger = LogManager.getLogger(NaturalLanguageSearchReactor.class);
	
	protected static final String CLASS_NAME = NaturalLanguageSearchReactor.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	protected static final String GLOBAL = "global";
	
	protected static final String NLDR_DB = "nldr_db";
	protected static final String NLDR_JOINS = "nldr_joins";
	protected static final String NLDR_MEMBERSHIP = "nldr_membership";
	protected static final String IS_GLOBAL = "IS_GLOCAL";
	

	private static LinkedHashMap<String, String> databaseIdToTypeStore = new LinkedHashMap<>(250);
	
	public NaturalLanguageSearchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.DATABASE.getKey(), GLOBAL , ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		int stepCounter = 1;
		Logger logger = this.getLogger(CLASS_NAME);
		String query = this.keyValue.get(this.keysToGet[0]);
		List<String> dbFilters = getDatabaseIds();
		boolean hasFilters = !dbFilters.isEmpty();
		boolean global = getGlobal();
		String panelId = getPanelId();
		
		// start logger
		logger.info(stepCounter + ". Performing Natural Language Search");
		stepCounter++;

		// Check Packages
		String[] packages = new String[] { "data.table", "plyr", "udpipe", "stringdist", "igraph", "SteinerNet" };
		this.rJavaTranslator.checkPackages(packages);
		
		// Collect all the apps that we will iterate through
		if (hasFilters) {
			// need to validate that the user has access to these ids
			List<String> databaseIds = SecurityEngineUtils.getFullUserDatabaseIds(this.insight.getUser());
			// make sure our ids are a complete subset of the user ids
			// user defined list must always be a subset of all the engine ids
			if (!databaseIds.containsAll(dbFilters)) {
				throw new IllegalArgumentException("Attempting to filter to app ids that user does not have access to or do not exist");
			}
		} else {
			dbFilters = SecurityEngineUtils.getFullUserDatabaseIds(this.insight.getUser());
		}
		
		// init r tables for use between methods
		String rSessionTable = "NaturalLangTable" + this.getSessionId().substring(0, 10);
		String rSessionJoinTable = "JoinTable" + this.getSessionId().substring(0, 10);

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
		
		String queryString = "";
		query = buildNamedArray(query);
		queryString = getQStringFromArray(query);
		
		logger.info(stepCounter + ". Generating search results");
		stepCounter++;
		List<Object[]> retData = generateAndRunScript(query, dbFilters, rSessionTable, rSessionJoinTable, global);
		
		// check for error
		if(retData == null || retData.size() == 0) {
			List<Map<String, Object>> retMap = new Vector<>();
			NounMetadata noun = new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
			return noun;
		}
		
		logger.info(stepCounter + ". Generating pixel return from results");
		stepCounter++;
		List<Map<String, Object>> returnPixels = generatePixels(retData, query, rSessionTable, global, panelId, queryString);		
		
		return new NounMetadata(returnPixels, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private String getQStringFromArray(String query) {
		String retString = "";
		String space = "";
		
		String[] namedArray = this.rJavaTranslator.getStringArray(query);
		for(String ele : namedArray) {
			retString += space + ele;
			space = " ";
		}
		
		
		return retString;
	}

	private String buildNamedArray(String query) {
		// check for blank
		if(query == null || query.isEmpty()) {
			query = "[]";
		}
		
		// read string into list
		List<Map<String, Object>> optMap = new Vector<Map<String, Object>>();
		optMap = new Gson().fromJson(query, optMap.getClass());
		StringBuilder arrayRsb = new StringBuilder();
		StringBuilder namesRsb = new StringBuilder();
		String request = "request_" + Utility.getRandomString(6);
		String comma = "";
		
		// start rsb's
		arrayRsb.append(request + " <- c(");
		namesRsb.append("names(" + request + ") <- c(");
		
		// loop through the map
		for (Map<String, Object> component : optMap) {
			String comp = component.get("component").toString();
			String elemToAdd = "";
			String elemName = "";
			
			// handle select and group
			String[] selects = { "select", "group", "distribution" };
			String[] aggregates = { "average", "count", "max", "min", "sum", "stdev" , "unique count" };
			String[] dates = { "dayname ", "monthname ", "week ", "quarter ", "year " };
			List<String> selectsList = Arrays.asList(selects);
			List<String> aggregatesList = Arrays.asList(aggregates);
			List<String> datesList = Arrays.asList(dates);
			if (selectsList.contains(comp) || aggregatesList.contains(comp) || datesList.contains(comp)) {
				List<String> columns = new Vector<String>();
				
				// if aggregate, add the aggregate row
				if (!selectsList.contains(comp)) {
					// change aggregate to select
					if(!comp.equals("group")) {
						elemToAdd += "select ";
						elemName = "select";
					}
					
					// so first add the aggregate row
					// add 'f' prior to any aggregate function 
					if(comp.equals("unique count")) {
						elemToAdd += "funiquecount";
					} else {
						elemToAdd += ("f" + comp);
					}
					
					// change the column to arraylist for below
					columns.add(component.get("column").toString());
					
				} else {
					// change the column to arraylist for below
					elemName = comp;
					elemToAdd += comp;
					columns = (List<String>) component.get("column");
				}
				
				// then, add the component and columns
				for(String col : columns) {
					// add the 'f' if its a date -- assuming the full thing gets passed as a column?
					if(Stream.of(dates).anyMatch(col::startsWith)) {
						elemToAdd += " f" + col;
					} else {
						elemToAdd += " " + col;
					}
				}
			}
			
			// handle the based on
			else if(comp.startsWith("based on")) {
				elemName = "based on";
				String agg = comp.substring(9);
				
				// need to include the aggregate 'f'
				if(agg.equals("unique count")) {
					elemToAdd += (elemName + " funiquecount");
				} else {
					elemToAdd += (elemName + " f" + agg);
				}
				
				
				elemToAdd += " " + component.get("column");
				
			}
			
			// handle where and having
			else if (comp.equals("where") || comp.startsWith("having")) {
				if(comp.startsWith("having")) {
					elemName = "having";
					if(comp.substring(7).equals("unique count")) {
						elemToAdd += "having funiquecount";
					} else {
						elemToAdd += ("having f" + comp.substring(7));
					}
				} else {
					elemName = "where";
					elemToAdd += "where";
				}
				
				
				elemToAdd += " " + component.get("column").toString();
				
				// catch the between
				if(component.get("operation").toString().startsWith("between")) {
					ArrayList<Integer> values = (ArrayList<Integer>) component.get("value");
					elemToAdd += " between";
					elemToAdd += " " + values.get(0) + " and " + values.get(1);
				} else {
					String op = component.get("operation").toString();
					if(op.equals("before")) {
						elemToAdd += " <";
					} else if(op.equals("after")) {
						elemToAdd += " >";
					} else {
						elemToAdd += " " + op;
					}					
					
					elemToAdd += " " + component.get("value").toString();
				}
			}
			
			// handle sort and rank
			else if (comp.equals("sort") || comp.equals("rank")) {
				elemName = comp;
				elemToAdd += comp;
				elemToAdd += " " + component.get("column").toString();
				elemToAdd += " " + component.get("operation").toString();
				
				if(!comp.equals("sort")) {
					elemToAdd += " " + component.get("value").toString();
				}
			}
			
			// handle position
			else if(comp.equals("position")) {
				elemName = comp;
				elemToAdd += comp;
				elemToAdd += " " + component.get("operation").toString();
				elemToAdd += " " + component.get("value").toString();
				elemToAdd += " " + component.get("column").toString();
			}
			
			// put it into the rsb
			arrayRsb.append(comma + "'" + elemToAdd + "'");
			namesRsb.append(comma + "'" + elemName + "'");
			comma = ",";
		}

		// wrap up arrays
		arrayRsb.append(");");
		namesRsb.append(");");
		
		// run arrays in r
		this.rJavaTranslator.runR(arrayRsb.toString() + namesRsb.toString());
		
		return request;
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
	 * @param dbFilters
	 * @return
	 */
	private List<Object[]> generateAndRunScript(String query, List<String> dbFilters, String rSessionTable, String rSessionJoinTable, boolean global) {
		String tempResult = "result" + Utility.getRandomString(8);
		StringBuilder rsb = new StringBuilder();
		String gcToAdd = "";
		
		// read in the rds files if global
		// use the frame columns if not
		if(global) {
			rsb.append(rSessionTable + " <- " + NLDR_DB + ";");
			rsb.append(rSessionJoinTable + " <- " + NLDR_JOINS + ";");
			
			// filter the rds files to the engineFilters
			String appFilters = "appFilters" + Utility.getRandomString(8);
			gcToAdd += "," + appFilters;
			rsb.append(appFilters + " <- c(");
			String comma = "";
			for (String databaseId : dbFilters) {
				rsb.append(comma + " \"" + databaseId + "\" ");
				comma = " , ";
			}
			rsb.append(");");
			rsb.append(rSessionTable + " <- " + rSessionTable + "[" + rSessionTable + "$AppID %in% " + appFilters + " ,];");
			rsb.append(rSessionJoinTable + " <- " + rSessionJoinTable + "[" + rSessionJoinTable + "$AppID %in% " + appFilters + " ,];");

		} else {
			// build the dataframe of COLUMN and TYPE
			ITableDataFrame frame = this.getFrame();
			Map<String, SemossDataType> colHeadersAndTypes = frame.getMetaData().getHeaderToTypeMap();
			List<String> columnList = new Vector<String>();
			List<String> tableList = new Vector<String>();
			List<String> typeList = new Vector<String>();
			List<String> pkList = new Vector<String>();
			for(Map.Entry<String,SemossDataType> entry : colHeadersAndTypes.entrySet()) {
				String col = entry.getKey();
				String type = entry.getValue().toString();
				if(col.contains("__")) {
					col = col.split("__")[1];
				}
				columnList.add(col);
				
				if(type.equals("INT") || type.equals("DOUBLE")) {
					type = "NUMBER";
				}
				typeList.add(type);
				pkList.add("FALSE");
				tableList.add(frame.getName() + "._." + frame.getName());
			}
			// turn into R table
			String rColumns = RSyntaxHelper.createStringRColVec(columnList);
			String rTables = RSyntaxHelper.createStringRColVec(tableList);
			String rTypes = RSyntaxHelper.createStringRColVec(typeList);
			String rPK = RSyntaxHelper.createStringRColVec(pkList);
			rsb.append(rSessionTable + " <- data.frame(Table = " + rTables + ", Column = " + rColumns + " , Datatype = " + rTypes
					+ ", Key = " + rPK + ", stringsAsFactors = FALSE);");
			rsb.append(rSessionJoinTable + " <- data.frame(tbl1 = character(0) , tbl2 = character(0) , joinby1 = character(0)"
					+ ", joinby2 = character(0) , AppID = character(0), stringsAsFactors = FALSE);");
		}
		// lets run the function on the filtered apps
		if (global) {
			rsb.append(tempResult + " <- exec_componentized_query(" + rSessionTable + "," + rSessionJoinTable + "," + query + "," + NLDR_MEMBERSHIP + ");");
		} else {
			rsb.append(tempResult + " <- exec_componentized_query(" + rSessionTable + "," + rSessionJoinTable + "," + query + ");");
		}
		// run it
		this.rJavaTranslator.runR(rsb.toString());

		// get back the data
		String[] headerOrdering = this.rJavaTranslator.getColumns(tempResult);
		List<Object[]> list = this.rJavaTranslator.getBulkDataRow(tempResult, headerOrdering);
		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + tempResult + gcToAdd + "); gc();");

		return list;
	}

	/**
	 * Generate the maps with the query information
	 * 
	 * @param retData
	 * @param queryInput
	 * @param queryString 
	 * @return
	 */
	private List<Map<String, Object>> generatePixels(List<Object[]> retData, String queryInput, String rSessionTable, boolean global, String panelId, String queryString) {
		// we do not know how many rows associate with the same QS
		// but we know the algorithm only returns one QS per engine
		// and the rows are ordered with regards to how the engine comes back
		Map<String, SelectQueryStruct> qsList = new LinkedHashMap<>();
		// when this value doesn't match the previous, we know to add a new QS
		String currDatabaseId = null;
		String label = null;
		SelectQueryStruct curQs = null;

		// need to store the collection of "Combined" qs's and their joins that I am
		// holding to make sure I don't duplicate and can also use this to push/pull to
		// add additional rows
		Map<String, SelectQueryStruct> combinedQs = new HashMap<>();

		// use the joinCombinedResult to merge in the pixel later
		List<Object[]> joinCombinedResult = new Vector<>();

		// use the these vectors to handle grouping/having/dropping unneeded cols
		List<Object[]> aggregateCols = new Vector<>();
		List<Object[]> combinedHavingRows = new Vector<>();
		LinkedHashSet<String> colsToDrop = new LinkedHashSet<>();
		LinkedHashSet<String> pickedCols = new LinkedHashSet<>();
		LinkedHashSet<String> groupedCols = new LinkedHashSet<>();
		boolean hasDateGroup = false;

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
			String rowDatabaseId = row[1].toString();
			if (combined && !combinedQs.containsKey(rowDatabaseId)) {
				// this is the combined result where the qs is not created yet
				// meaning it is the first of a certain select of a combined entry
				currDatabaseId = rowDatabaseId;
				curQs = new SelectQueryStruct();
				if(global) {
					curQs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
					curQs.setEngineId(currDatabaseId);
				} else {
					curQs.setQsType(QUERY_STRUCT_TYPE.FRAME);
				}
				qsList.put("Multiple" + combinedQs.size(), curQs);
				combinedQs.put(currDatabaseId, curQs);
			} else if (!combined && currDatabaseId == null) {
				// this is the first one of a non-combined
				currDatabaseId = rowDatabaseId;
				curQs = new SelectQueryStruct();
				if(global) {
					curQs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
					curQs.setEngineId(currDatabaseId);
				} else {
					curQs.setQsType(QUERY_STRUCT_TYPE.FRAME);
				}
				qsList.put(label, curQs);
			} else if (!combined && currDatabaseId != null && !currDatabaseId.equals(rowDatabaseId)) {
				// okay this row is now starting a new QS
				// we gotta init another one
				currDatabaseId = rowDatabaseId;
				curQs = new SelectQueryStruct();
				if(global) {
					curQs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
					curQs.setEngineId(currDatabaseId);
				} else {
					curQs.setQsType(QUERY_STRUCT_TYPE.FRAME);
				}
				qsList.put(label, curQs);
			} else if (!combined && currDatabaseId != null && currDatabaseId.equals(rowDatabaseId) && !qsList.containsKey(label)) {
				// How do we handle multiple queries from the same database
				curQs = new SelectQueryStruct();
				if (global) {
					curQs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
					curQs.setEngineId(currDatabaseId);
				} else {
					curQs.setQsType(QUERY_STRUCT_TYPE.FRAME);
				}
				qsList.put(label, curQs);
			}

			// if this is a combined row, pull the qs that matches the appid
			if (combined) {
				currDatabaseId = rowDatabaseId;
				curQs = combinedQs.get(currDatabaseId);
			}

			if (curQs == null) {
				throw new NullPointerException("curQs (Query Struct) should not be null here.");
			}

			// check what type of row it is, then add to qs by case
			if (part.equalsIgnoreCase("select")) {
				String selectConcept = row[4].toString();
				String selectProperty = row[5].toString();
				boolean agg = !row[6].toString().isEmpty();

				IQuerySelector selector = null;
				// need to properly send/receive null values in case there is a
				// property with the same name as the node
				boolean isPK = checkForPK(selectConcept, selectProperty, rSessionTable, currDatabaseId, global);
				if (isPK) {
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
					// convert avg to average to match selector alias
					if (agg && row[6].toString().equals("Avg")) {
						pickedCols.add("Average_" + row[5]);
					} else {
						pickedCols.add(row[6] + "_" + row[5]);
					}
				} else if (combined && !row[12].toString().equals("yes")) {
					colsToDrop.add(selector.getAlias());
				}

				if (agg) {
					// if it is combined, then just import the data for now and save the agg for
					// later
					if (combined) {
						curQs.addSelector(selector);
						aggregateCols.add(row);
					} else {
						QueryFunctionSelector fSelector = new QueryFunctionSelector();
						String func = row[6].toString();
						fSelector.setFunction(func);
						fSelector.addInnerSelector(selector);
						
						
						// check if it was a date group
						String[] dates = { "DAYNAME", "MONTHNAME", "WEEK", "QUARTER", "YEAR" };
						if(Stream.of(dates).anyMatch(func.toUpperCase()::startsWith)) {
							fSelector.setDataType("String");
							hasDateGroup = true;
						}
						
						// add the selector
						curQs.addSelector(fSelector);
					}
				} else {
					curQs.addSelector(selector);
				}
			} else if (part.equalsIgnoreCase("from")) {
				// if the two appids are filled in but are not equal, this is a join across
				// query structures
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
				// need to properly send/receive null values in case there is a
				// property with the same name as the node
				boolean isPK = checkForPK(whereTable, whereCol, rSessionTable, currDatabaseId, global);
				if (isPK) {
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
				boolean isPK = checkForPK(havingTable, havingCol, rSessionTable, currDatabaseId, global);
				if (isPK) {
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
					isPK = checkForPK(havingTable, havingCol, rSessionTable, currDatabaseId, global);
					if (isPK) {
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
					boolean isPK = checkForPK(groupConcept, groupProperty, rSessionTable, currDatabaseId, global);
					if (isPK) {
						curQs.addGroupBy(groupConcept, null);
					} else if(hasDateGroup){
						// check if it was a date group
						String[] dates = { "DAYNAME", "MONTHNAME", "WEEK", "QUARTER", "YEAR" };
						if(Stream.of(dates).anyMatch(groupProperty.toUpperCase()::startsWith)) {
							QueryFunctionSelector fSelector = new QueryFunctionSelector();
							String[] dateGroup = groupProperty.split("_");
							QueryColumnSelector groupSelector = new QueryColumnSelector(groupConcept + "__" + dateGroup[1]);
							fSelector.setFunction(dateGroup[0]);
							fSelector.addInnerSelector(groupSelector);
							// add the selector
							curQs.addGroupBy(fSelector);
						}
						
					} else {
						curQs.addGroupBy(groupConcept, groupProperty);
					}
				}
			} else if (part.equalsIgnoreCase("rank")) {
				String rankTable = row[4].toString();
				String rankCol = row[5].toString();
				String rankDir = row[6].toString();
				String rankAmount = row[7].toString();
				
				// adjust rank direction
				if(rankDir.equals("top")) {
					rankDir = ORDER_BY_DIRECTION.DESC.toString();
				} else if (rankDir.equals("bottom")) {
					rankDir = ORDER_BY_DIRECTION.ASC.toString();
				}
				
				boolean isPK = checkForPK(rankTable, rankCol, rSessionTable, currDatabaseId, global);
				boolean isDerived = checkForDerived(rankTable, rankCol, rSessionTable, currDatabaseId, global);
				QueryColumnOrderBySelector orderBy = null;
				if (isPK) {
					orderBy = new QueryColumnOrderBySelector(rankTable);
				} else if(isDerived){
					orderBy = new QueryColumnOrderBySelector(rankCol);
				} else {
					orderBy = new QueryColumnOrderBySelector(rankTable + "__" + rankCol);
				}
				orderBy.setSortDir(rankDir);
				curQs.addOrderBy(orderBy);
				
				// handle both integer types
				int lim = Integer.parseInt(rankAmount);
				if (lim >= 0) {
					curQs.setLimit(lim);
				} else {
					curQs.setOffSet(Math.abs(lim));
				}
				
			} else if (part.equalsIgnoreCase("sort")) {
				String sortTable = row[4].toString();
				String sortCol = row[5].toString();
				String sortDir = row[6].toString();
				
				if(sortDir.equalsIgnoreCase("ascending")) {
					sortDir = ORDER_BY_DIRECTION.ASC.toString();
				} else if (sortDir.equals("descending")) {
					sortDir = ORDER_BY_DIRECTION.DESC.toString();
				}
				
				boolean isPK = checkForPK(sortTable, sortCol, rSessionTable, currDatabaseId, global);
				boolean isDerived = checkForDerived(sortTable, sortCol, rSessionTable, currDatabaseId, global);
				QueryColumnOrderBySelector orderBy = null;
				if (isPK) {
					orderBy = new QueryColumnOrderBySelector(sortTable);
				} else if(isDerived){
					orderBy = new QueryColumnOrderBySelector(sortCol);
				} else {
					orderBy = new QueryColumnOrderBySelector(sortTable + "__" + sortCol);
				}
				orderBy.setSortDir(sortDir);
				curQs.addOrderBy(orderBy);
			}
		}

		// retMap is full of maps with key = label and value = pixel
		List<Map<String, Object>> retMap = new Vector<>();
		Map<String, Object> map = new HashMap<>();

		// track when the entry changes and setup other vars
		String curEntry = null;
		String frameName = getCleanFrameName(queryString);
		String importPixel = "";
		String vizTypesPixel = "";
		String vizPixel = "";
		LinkedHashSet<String> prevDatabaseIds = new LinkedHashSet<>();
		int entryCount = 1;

		for (Entry<String, SelectQueryStruct> entry : qsList.entrySet()) {
			// make the framename unique with a counter
			frameName = frameName + "_" + retMap.size();
			
			// first lets check if it is combined
			if (entry.getKey().contains("Multiple")) {
				// if this is the first instance of a combined result, then start a new map
				if (curEntry == null || !curEntry.contains("Multiple")) {
					// start the new map
					map = new HashMap<>();
					curEntry = entry.getKey();
					importPixel = "";
					vizTypesPixel = "";
					vizPixel = "";

					// process the qs
					SelectQueryStruct qs = entry.getValue();
					importPixel += frameExistsPixel(frameName);
					importPixel += "CreateFrame ( R ) .as ( [ " + frameName + " ] );";
					importPixel += buildImportPixelFromQs(qs, qs.getEngineId(), frameName, false, global);

					// in the case where there is only one combined qs, lets return
					if (entryCount == qsList.entrySet().size()) {
						// return map
						map.put("database_id", "Multiple Apps");
						map.put("database_name", "Multiple Apps");
						map.put("frame_name", frameName);
						
						// put all three pixels in the map
						// import pixel
						importPixel += dropUnwantedCols(colsToDrop, groupedCols);
						importPixel += addGroupingsAndHavings(aggregateCols, groupedCols, combinedHavingRows, frameName, global);
						importPixel += "));";
						map.put("import_pixel", importPixel);
						
						// viz types pixel
						vizTypesPixel = (frameName + " | GetNLPVizOptions(database=[\"Multiple\"],columns=" + pickedCols + ");");
						map.put("viz_types_pixel", vizTypesPixel);
						
						// viz pixel
						vizPixel += getStartPixel(frameName, panelId);
						vizPixel += "Panel ( "+panelId+" ) | SetPanelLabel(\"" + queryString + "\");";
						vizPixel += "Panel ( "+panelId+" ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;";
						vizPixel += (frameName + " | PredictViz(database=[\"Multiple\"],columns=" + pickedCols + ",sortPixel=[\""+getSortPixel(qs,null,frameName)+"\"],panel=[" + panelId + "],vizSelection=[<viztype>]);");
						map.put("viz_pixel", "");
						
						// original workflow
						map.put("layout", "NLP");
						map.put("columns", pickedCols);
						retMap.add(map);

					}
					// store the previous app id for when we join across db's later
					prevDatabaseIds.add(qs.getEngineId());
					entryCount++;

				}
				// if this is the last result, lets return the map. we are done
				else if (entryCount == qsList.entrySet().size()) {
					// process the qs
					SelectQueryStruct qs = entry.getValue();
					
					// return map
					map.put("database_id", "Multiple Apps");
					map.put("database_name", "Multiple Apps");
					map.put("frame_name", frameName);
					
					// put all three pixels in the map
					importPixel += buildImportPixelFromQs(qs, qs.getEngineId(), frameName, true, global);
					importPixel += addMergePixel(qs, prevDatabaseIds, joinCombinedResult, frameName);
					importPixel += dropUnwantedCols(colsToDrop, groupedCols);
					importPixel += addGroupingsAndHavings(aggregateCols, groupedCols, combinedHavingRows, frameName, global);
					importPixel += "));";
					map.put("import_pixel", importPixel);
					
					// viz types pixel
					vizTypesPixel = (frameName + " | GetNLPVizOptions(database=[\"Multiple\"],columns=" + pickedCols + ");");
					map.put("viz_types_pixel", vizTypesPixel);
					
					// viz pixel
					vizPixel = getStartPixel(frameName, panelId);
					vizPixel += "Panel ( "+panelId+" ) | SetPanelLabel(\"" + queryString + "\");";
					vizPixel += "Panel ( "+panelId+" ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;";
					vizPixel += (frameName + " | PredictViz(database=[\"Multiple\"],columns=" + pickedCols + ",sortPixel=[\""+getSortPixel(qs,null,frameName)+"\"],panel=[" + panelId + "],vizSelection=[<viztype>]);");
					map.put("viz_pixel", vizPixel);
					
					// original workflow
					map.put("layout", "NLP");
					map.put("columns", pickedCols);
					retMap.add(map);

				}
				// this is a continuation of a previous result
				else {
					// add to the existing pixel
					SelectQueryStruct qs = entry.getValue();
					importPixel += buildImportPixelFromQs(qs, qs.getEngineId(), frameName, true, global);
					importPixel += addMergePixel(qs, prevDatabaseIds, joinCombinedResult, frameName);
					entryCount++;

					// store the previous app id for when we join across db's later
					prevDatabaseIds.add(qs.getEngineId());
				}
			} else {
				// if the result is not combined, then there is only one qs
				// put it in the map and then return
				curEntry = entry.getKey();
				map = new HashMap<>();
				SelectQueryStruct qs = entry.getValue();
				String appId = qs.getEngineId();
				String appName = MasterDatabaseUtility.getDatabaseAliasForId(appId);
				map.put("database_id", appId);
				map.put("database_name", appName);
				map.put("frame_name", frameName);
				
				// put all three pixels in the map
				// import pixel
				importPixel = frameExistsPixel(frameName);
				importPixel += "CreateFrame ( R ) .as ( [ " + frameName + " ] );";
				importPixel += buildImportPixelFromQs(qs, appId, frameName, false, global);
				importPixel += "));";
				map.put("import_pixel", importPixel);
				
				// viz types pixel
				vizTypesPixel = (frameName + " | GetNLPVizOptions(database=[\"" + appId + "\"],columns=" + getSelectorAliases(qs.getSelectors()) + ");");
				map.put("viz_types_pixel", vizTypesPixel);
				
				// viz pixel
				vizPixel = getStartPixel(frameName, panelId);
				vizPixel += "Panel ( "+panelId+" ) | SetPanelLabel(\"" + queryString + "\");";
				vizPixel += "Panel ( "+panelId+" ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;";
				vizPixel += (frameName + " | PredictViz(database=[\"" + appId + "\"],columns="
						+ getSelectorAliases(qs.getSelectors()) + ",sortPixel=[\""+getSortPixel(qs,null,frameName)+"\"],panel=[" + panelId + "],vizSelection=[<viztype>]);");
				map.put("viz_pixel", vizPixel);
				
				// original workflow
				map.put("layout", "NLP");
				map.put("columns", getSelectorAliases(qs.getSelectors()));
				retMap.add(map);
				importPixel = "";
				vizTypesPixel = "";
				vizPixel = "";
				entryCount++;
			}
		}

		// return the map
		return retMap;
	}

	private String getCleanFrameName(String queryString) {
		queryString = queryString.replaceAll("<=", "less than or equal to");
		queryString = queryString.replaceAll("- top", "offset_top");
		queryString = queryString.replaceAll("- bottom", "offset_bottom");
		queryString = queryString.replaceAll(">=", "greater than or equal to");
		queryString = queryString.replaceAll("!=", "not equal to");
		queryString = queryString.replaceAll("<", "less than");
		queryString = queryString.replaceAll(">", "greater than");
		queryString = queryString.replaceAll("=", "equals");
		queryString = queryString.replaceAll(" ", "_");
		queryString = queryString.replaceAll("-", "_");
		queryString = queryString.replaceAll("/", "_");
		queryString = queryString.replaceAll("\\\\", "_");

		
		return queryString;
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	/**
	 * Check the existing R table to determine if a column is a primary key in the
	 * particular table and app
	 * 
	 * @param concept
	 * @param property
	 * @param rSessionTable
	 * @param appId
	 * @return true or false
	 */

	private boolean checkForPK(String concept, String property, String rSessionTable, String appId, boolean global) {
		if(!global) {
			return false;
		}
		
		StringBuilder rsb = new StringBuilder();
		String alteredTable = appId + "._." + concept;
		rsb.append("unique(" + rSessionTable + "[");
		rsb.append(rSessionTable + "$AppID == \"" + appId + "\" & ");
		rsb.append(rSessionTable + "$Table == \"" + alteredTable + "\" & ");
		rsb.append(rSessionTable + "$Column == \"" + property + "\"");
		rsb.append(",]$Key)");
		
		int nrow = this.rJavaTranslator.getInt("length("+rsb.toString()+")");
		if(nrow == 0 ){
			return false;
		} 
		
		String key = this.rJavaTranslator.getString(rsb.toString());

		return Boolean.parseBoolean(key);
	}
	
	/**
	 * Check the existing R table to determine if a column is a primary key in the
	 * particular table and app
	 * 
	 * @param concept
	 * @param property
	 * @param rSessionTable
	 * @param appId
	 * @return true or false
	 */

	private boolean checkForDerived(String concept, String property, String rSessionTable, String appId, boolean global) {
		if(!global) {
			return false;
		}
		
		String splitProperty = "";
		
		String[] types = {"Average","Count","Max","Min","UniqueCount","Stdev"};
		boolean noMatches = true;
		for(String type : types) {
			if(property.startsWith(type)) {
				noMatches = false;
				splitProperty = property.substring(type.length() + 1);
			}
		}
		
		if(noMatches) {
			return false;
		}
		
		StringBuilder rsb = new StringBuilder();
		String alteredTable = appId + "._." + concept;
		rsb.append("nrow(" + rSessionTable + "[");
		rsb.append(rSessionTable + "$AppID == \"" + appId + "\" & ");
		rsb.append(rSessionTable + "$Table == \"" + alteredTable + "\" & ");
		rsb.append(rSessionTable + "$Column == \"" + splitProperty + "\"");
		rsb.append(",]);");
		
		int numRows = this.rJavaTranslator.getInt(rsb.toString());

		return numRows > 0;
	}
	
	/**
	 * Add the frameexists
	 * 
	 * @param frameName
	 * @return
	 */
		
	public String frameExistsPixel(String frameName) {
		// create the pixel
		StringBuilder psb = new StringBuilder();
		psb.append("if( ( ");
		psb.append("VariableExists ( '" + frameName + "' ) ");
		psb.append(") , (\"Frame Exists\") , (");
		
		// return
		return psb.toString();
		
	}
	
	/**
	 * Build the pixel based on the query struct and app id
	 * 
	 * @param qs
	 * @param databaseId
	 * @param frameName
	 * @param merge
	 * @return
	 */
	public String buildImportPixelFromQs(SelectQueryStruct qs, String databaseId, String frameName, boolean merge, boolean global) {
		StringBuilder psb = new StringBuilder();
		QUERY_STRUCT_TYPE type = qs.getQsType();
		
		// continue with import if false
		if (type == QUERY_STRUCT_TYPE.ENGINE) {
			// pull from the appId
			psb.append("Database ( database = [ \"" + databaseId + "\" ] ) | ");
		} else if (type == QUERY_STRUCT_TYPE.FRAME) {
			psb.append("Frame ( frame = [" + this.getFrame().getName() + "] ) | ");
		}

		// pull the correct columns
		Map<String, String> qsToAlias = new HashMap<>();
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
		List<IQuerySelector> groupList = qs.getGroupBy();
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
						if (value.contains("%") && getAppTypeFromId(databaseId).equals("TYPE:RDF")) {
							value = value.replace("%", "/.*");
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
						psb.append("\"" + rhs.getValue() + "\"");
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
		Set<IRelation> relations = qs.getRelations();
		if (!relations.isEmpty()) {
			separator = "";
			psb.append("Join ( ");
			for (IRelation relationship : relations) {
				if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
					BasicRelationship rel = (BasicRelationship) relationship;
					String col1 = rel.getFromConcept();
					String joinType = rel.getJoinType();
					String col2 = rel.getToConcept();
					psb.append(separator);
					separator = " , ";
					psb.append("( " + col1 + ", ");
					psb.append(joinType + ", ");
					psb.append(col2 + " ) ");
				} else {
					classLogger.info("Cannot process relationship of type: " + relationship.getRelationType());
				}
			}
			psb.append(") | ");
		}
		
		String sortPixel = getSortPixel(qs, qsToAlias, frameName);
		psb.append(sortPixel);

		if (qs.getLimit() > 0) {
			psb.append("Limit(").append(qs.getLimit()).append(") | ");
		}
		
		if(qs.getOffset() > 0) {
			psb.append("Offset(").append(qs.getOffset()).append(") | ");
		}

		// final import statement
		if (!merge) {
			psb.append("Import ( frame = [ " + frameName + " ] ) ;");

		}
		
		// if its from the frame, then remove reference to the frame
		String retString = psb.toString();
		if(!global) {
			retString = retString.replaceAll(this.getFrame().getName() + "__", "");
		}

		// return the pixel
		return retString;
	}

	/**
	 * get the sort pixel string -- pulled out because used in two different places
	 * @param qsToAlias 
	 */
	private String getSortPixel(SelectQueryStruct qs, Map<String, String> qsToAlias, String frameName) {
		List<IQuerySort> orderBys = qs.getOrderBy();
		String retString = "";
		boolean replaceFrame = false;
		
		if (orderBys == null || orderBys.isEmpty()) {
			return retString;
		}
		
		// need to recreate this if called from GeneratePixels function
		if(qsToAlias == null) {
			qsToAlias = new HashMap<String,String>();
			List<IQuerySelector> selectors = qs.getSelectors();

			// loop through the selectors and store their name and alias
			for (IQuerySelector sel : selectors) {
				String selAliasToAdd = sel.getAlias();
				// track list in case we need it
				qsToAlias.put(sel.getQueryStructName().toUpperCase(), selAliasToAdd);
			}
			replaceFrame = true;
		}
		
		StringBuilder b = new StringBuilder();
		StringBuilder b2 = new StringBuilder();
		int i = 0;
		for (IQuerySort orderBy : orderBys) {
			if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
				QueryColumnOrderBySelector columnSort = (QueryColumnOrderBySelector) orderBy;
				if (i > 0) {
					b.append(", ");
					b2.append(", ");
				}
				if (qsToAlias.containsKey(columnSort.getQueryStructName().toUpperCase())) {
					b.append(qsToAlias.get(columnSort.getQueryStructName().toUpperCase()));
				} else {
					b.append(columnSort.getQueryStructName());
				}
				b2.append(columnSort.getSortDirString());
				i++;
			}
		}
		retString = "Sort(columns=[" + b.toString() + "], sort=[" + b2.toString() + "]) | ";		
		
		// if this is being passed to the createviz reactor, need to remove frame name
		if(replaceFrame) {
			retString = retString.replaceAll(frameName + "__", "");
		}
		
		
		return retString;		
	}

	/**
	 * get the pixel to merge the db's together
	 * 
	 * @param qs
	 * @param prevAppId
	 *            -- to make sure its the correct join
	 * @param joinCombinedResults
	 *            -- the rows to join across
	 * @param frameName
	 *            -- to perform the join pixel
	 * @param qs
	 * 
	 * @return
	 */
	private String addMergePixel(SelectQueryStruct qs, LinkedHashSet<String> prevAppIds,
			List<Object[]> joinCombinedResult, String frameName) {
		// Merge ( joins = [ ( System , right.outer.join , EKTROPY_ITEMS_0722__System )
		// ] ) ;
		String appId = qs.getEngineId();
		String mergeCol = "";
		String mergeString = "Merge ( joins = [(";
		for (Object[] joinRow : joinCombinedResult) {
			// figure out which qs needs to merge, whether
			// its first, second, what the column is, etc.
			if (joinRow[1].equals(appId) && prevAppIds.contains(joinRow[2].toString())) {
				mergeCol = joinRow[5].toString();
			} else if (prevAppIds.contains(joinRow[1].toString()) && joinRow[2].equals(appId)) {
				mergeCol = joinRow[7].toString();
			}
		}
		mergeString += mergeCol + " , inner.join , " + mergeCol + " ) ]  , frame = [" + frameName + "] );";
		return mergeString;
	}

	/**
	 * get the pre-data import pixel
	 * 
	 * @param frameName
	 * 
	 * @return
	 */
	private String getStartPixel(String frameName, String panelId) {
		String addPanelText = panelId;
		if(panelId.equals("0")) {
			addPanelText = "panel = [ 0 ] , sheet = [ \"0\" ]";
		}
		
		String startPixel = "AddPanel ( " + addPanelText + " ) ;";
		startPixel += "Panel ( " + panelId + " ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ;";
		startPixel += "Panel ( " + panelId + " ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , "
				+ "\"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" :"
				+ " \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , "
				+ "\"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ; Panel ( " + panelId + " ) | RetrievePanelEvents ( ) ;";

		return startPixel;
	}

	/**
	 * Drop the columns that were not "picked"
	 * 
	 * @param colsToDrop
	 *            -- columns that were not picked by the query
	 * @param groupedCols
	 *            -- groupedCols to double check that they werent picked elsewhere
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
	 * 
	 * @param aggregateCols
	 *            -- rows that were aggregates in the R return
	 * @param groupedCols
	 *            -- the columns that we are grouping the aggregates on
	 * @return
	 */
	private String addGroupingsAndHavings(List<Object[]> aggregateCols, LinkedHashSet<String> groupedCols,
			List<Object[]> combinedHavingRows, String frameName, boolean global) {
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
		mergeString += "] , frame = [ " + frameName + " ] ) ; ";

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
		psb.append(buildImportPixelFromQs(qs, null, frameName, true, global));
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
	 * Get input database ids
	 * 
	 * @return
	 */
	private List<String> getDatabaseIds() {
		List<String> engineFilters = new Vector<>();
		GenRowStruct engineGrs = this.store.getNoun(this.keysToGet[1]);
		for (int i = 0; i < engineGrs.size(); i++) {
			engineFilters.add(engineGrs.get(i).toString());
		}

		return engineFilters;
	}

	/**
	 * Get the selectors' aliases as a list
	 * 
	 * @param qs
	 *            selectors
	 * @return
	 */
	private List<String> getSelectorAliases(List<IQuerySelector> selectors) {
		List<String> aliases = new Vector<>();
		for (IQuerySelector sel : selectors) {
			aliases.add(sel.getAlias());
		}
		return aliases;
	}
	
	private boolean getGlobal() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		if (grs == null || grs.isEmpty()) {
			return true;
		}
		return Boolean.parseBoolean(grs.get(0).toString());
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return "0";
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Utility
	 */

	/**
	 * Utilize a static cache so we do not query local master to get the type of an
	 * database every time
	 * 
	 * @param databaseId
	 * @return
	 */
	private static String getAppTypeFromId(String databaseId) {
		String type = databaseIdToTypeStore.get(databaseId);
		if (type != null) {
			return type;
		}

		// store the result so we don't need to query all the time
		type = MasterDatabaseUtility.getDatabaseTypeForId(databaseId);
		databaseIdToTypeStore.put(databaseId, type);
		if (databaseIdToTypeStore.size() > 200) {
			synchronized (databaseIdToTypeStore) {
				if (databaseIdToTypeStore.size() > 100) {
					// it should be ordered from first to last
					Iterator<String> it = databaseIdToTypeStore.keySet().iterator();
					int counter = 0;
					while (it.hasNext() && counter < 100) {
						databaseIdToTypeStore.remove(it.next());
					}
				}
			}
		}
		return type;
	}

}