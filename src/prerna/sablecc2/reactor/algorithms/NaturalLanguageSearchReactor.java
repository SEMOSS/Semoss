package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.h2.util.StringUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
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
	 * Generates pixel to dynamically create insight based on Natural Language search
	 */
	
	protected static final String CLASS_NAME = NaturalLanguageSearchReactor.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public NaturalLanguageSearchReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.APP.getKey() };
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
		String[] packages = new String[] { "data.table", "udpipe" };
		this.rJavaTranslator.checkPackages(packages);

		// Check to make sure that needed files exist before searching
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File algo1 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "nli_db.R");
		File algo2 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "db_pixel.R");
		File algo3 = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR + "english-ud-2.0-170801.udpipe");
		if(!algo1.exists() || !algo2.exists() || !algo3.exists()) {
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
		sb.append( ("setwd(\"" + baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts\");").replace("\\", "/") );
		sb.append("source(\"nli_db.r\");");
		sb.append("source(\"db_pixel.r\");");
		sb.append("library(udpipe);");
		this.rJavaTranslator.runR(sb.toString());
		logger.info(stepCounter + ". Done");
		stepCounter++;

		// Collect all the apps that we will iterate through
		logger.info(stepCounter + ". Collecting apps to iterate through");
		if(hasFilters) {
			// need to validate that the user has access to these ids
			if (AbstractSecurityUtils.securityEnabled()) {
				List<String> userIds = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
				// make sure our ids are a complete subset of the user ids
				// user defined list must always be a subset of all the engine ids
				if(!userIds.containsAll(engineFilters)) {
					throw new IllegalArgumentException("Attempting to filter to app ids that user does not have access to or do not exist");
				}
			} else {
				List<String> allIds = MasterDatabaseUtility.getAllEngineIds();
				if(!allIds.containsAll(engineFilters)) {
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

		// get matrix of data from local master
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engineFilters);
		List<String[]> allRelations = MasterDatabaseUtility.getRelationships(engineFilters);
		
		logger.info(stepCounter + ". Generating search results");
		List<Object[]> retData = generateAndRunScript(query, allTableCols, allRelations);
		logger.info(stepCounter + ". Done");

		logger.info(stepCounter + ". Generating pixel return from results");
		List<Map<String, String>> returnPixels = generatePixels(retData, query);
		logger.info(stepCounter + ". Done");
		
		// if no results were found, return error message
		if (returnPixels.isEmpty()) {
			NounMetadata noun = new NounMetadata("Natural Language Search provided no results", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			return noun;
		}

		return new NounMetadata(returnPixels, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////	

	/*
	 * Generate the R script
	 */

	/**
	 * Generate the 2 data.tables based on the table structure and relationships and returns back the results
	 * from the algorithm
	 * @param query
	 * @param allTableCols
	 * @param relationships
	 * @return
	 */
	private List<Object[]> generateAndRunScript(String query, List<Object[]> allTableCols, List<String[]> relationships) {
		// Getting database info
		String rTempTable = "NaturalLangTable" + Utility.getRandomString(8);
		String rJoinTable = "JoinTable" + Utility.getRandomString(8);
		String result = "result" + Utility.getRandomString(8);
		StringBuilder rsb = new StringBuilder();

		String rAppIds = "c(";
		String rTableNames = "c(";
		String rColNames = "c(";
		
		int colCount = allTableCols.size();
		// create R vector of appid, tables, and columns
		for(int i = 0; i < colCount; i++) {
			Object[] entry = allTableCols.get(i);
			String appId = entry[0].toString();
			String table = entry[1].toString();
			String column = entry[2].toString();
			if(i == 0) {
				rAppIds += "'" + appId + "'" ;
				rTableNames += "'" + table + "'" ;
				rColNames += "'" + column + "'" ;
			} else {
				rAppIds += ",'" + appId + "'" ;
				rTableNames += ",'" + table + "'" ;
				rColNames += ",'" + column + "'" ;
			}
		}

		// create R vector of table columns and table rows
		String rAppIDs_join = "c(";
		String rTbl1 = "c(";
		String rTbl2 = "c(";
		String rJoinBy1 = "c(";
		String rJoinBy2 = "c(";

		int numRels = relationships.size();
		int firstRel = 0;
		for(int i = 0; i < numRels; i++) {
			String[] entry = relationships.get(i);
			String appId = entry[0];
			String rel = entry[3];

			String[] relSplit = rel.split("\\.");
			if(relSplit.length == 4) {
				String sourceTable = relSplit[0];
				String sourceColumn = relSplit[1];
				String targetTable = relSplit[2];
				String targetColumn = relSplit[3];
				
				//check by firstRel, not index of for loop
				//loop increments even if relSplit.length != 4
				//whereas firstRel only increases if something is added to frame
				if(firstRel == 0) {
					rAppIDs_join += "'" + appId + "'";
					rTbl1 += "'" + sourceTable + "'" ;
					rTbl2 += "'" + targetTable + "'" ;
					rJoinBy1 += "'" + sourceColumn + "'" ;
					rJoinBy2 += "'" + targetColumn + "'" ;
				} else {
					rAppIDs_join += ",'" +  appId + "'" ;
					rTbl1 += ",'" + sourceTable + "'" ;
					rTbl2 += ",'" + targetTable + "'" ;
					rJoinBy1 += ",'" + sourceColumn + "'" ;
					rJoinBy2 += ",'" + targetColumn + "'" ;
				}
				
				if(sourceColumn.endsWith("_FK")) {
					// if column ends with a _FK, then add it to NaturalLangTable also
					rAppIds += ",'" + appId + "'" ;
					rTableNames += ",'" + sourceTable + "'" ;
					rColNames += ",'" + sourceColumn + "'" ;
				}
				//no longer adding the first row to this data frame, increment..
				firstRel++;
			}
		}

		// close all the arrays created
		rAppIds += ")";
		rTableNames += ")";
		rColNames += ")";
		rAppIDs_join += ")";
		rTbl1 += ")";
		rTbl2 += ")";
		rJoinBy1 += ")";
		rJoinBy2 += ")";
		
		rsb.append(rTempTable + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + " , AppID = " + rAppIds + ", stringsAsFactors = FALSE);");
		if(numRels == 0) {
			rsb.append(result + " <- nliapp_mgr(\"" + query + "\"," + rTempTable + ");\n");
		} else {
			// Create data frame from above join vectors
			rsb.append(rJoinTable + " <- data.frame(tbl1 = " + rTbl1 + " , tbl2 = " + rTbl2 + " , joinby1 = " + rJoinBy1 + " , joinby2 = " + rJoinBy2 + " , AppID = " + rAppIDs_join + ", stringsAsFactors = FALSE);");
			rsb.append(result + " <- nliapp_mgr(\"" + query + "\"," + rTempTable + "," + rJoinTable + ");");
		}
		this.rJavaTranslator.runR(rsb.toString());

		// get back the data
		String[] headerOrdering = new String[]{"appid", "part", "item1", "item2", "item3", "item4", "item5", "item6", "item7" };
		List<Object[]> list = this.rJavaTranslator.getBulkDataRow(result, headerOrdering);
		return list;
	}

	/**
	 * Generate the maps with the query information
	 * @param retData
	 * @param queryInput
	 * @return
	 */
	private List<Map<String, String>> generatePixels(List<Object[]> retData, String queryInput) {
		// we do not know how many rows associate with the same QS
		// but we know the algorithm only returns one QS per engine
		// and the rows are ordered with regards to how the engine comes back
		List<SelectQueryStruct> qsList = new Vector<SelectQueryStruct>();
		// when this value doesn't match the previous, we know to add a new QS
		String currAppId = null;
		SelectQueryStruct curQs = null;
		int numRows = retData.size();
		for(int i = 0; i < numRows; i++) {
			Object[] row = retData.get(i);
			// if it is an error
			// continue through the loop
			String part = row[1].toString();
			if(part.equalsIgnoreCase("error")) {
				continue;
			}
			// this is the first one
			String rowAppId = row[0].toString();
			if(currAppId == null) {
				currAppId = rowAppId;
				curQs = new SelectQueryStruct();
				curQs.setEngineId(currAppId);
				qsList.add(curQs);
			} else if(!currAppId.equals(rowAppId)) {
				// okay
				// this row is now starting a new QS
				// we gotta init another one
				currAppId = rowAppId;
				curQs = new SelectQueryStruct();
				curQs.setEngineId(currAppId);
				qsList.add(curQs);
			}
			
			// check what type of row it is, then add to qs by case
			
			if(part.equalsIgnoreCase("select")) {
				String selectConcept = row[2].toString();
				String selectProperty = row[3].toString();
				boolean agg = !row[4].toString().isEmpty();
				
				IQuerySelector selector = null;
				// if it is a table
				// we do not know the correct primary key
				// so we exec a query to determine if we should use the current selectedProperty
				// or keep it as PRIM_KEY_PLACEHOLDER
				if(MasterDatabaseUtility.getTableForColumn(currAppId, selectProperty) == null) {
					selector = new QueryColumnSelector(selectConcept);
				} else {
					selector = new QueryColumnSelector(selectConcept + "__" + selectProperty);
				}
				if(agg) {
					QueryFunctionSelector fSelector = new QueryFunctionSelector();
					fSelector.setFunction(row[4].toString());
					fSelector.addInnerSelector(selector);
					// add the selector
					curQs.addSelector(fSelector);
				} else {
					curQs.addSelector(selector);
				}
				
			} else if(part.equalsIgnoreCase("from")) {
				if (!row[3].toString().isEmpty()) {
					String fromConcept = row[2].toString();
					String toConcept = row[4].toString();
					String joinType = "inner.join";
					curQs.addRelation(fromConcept, toConcept, joinType);
				}
				
			} else if(part.equalsIgnoreCase("where")) {
				String whereTable = row[2].toString();
				String whereCol = row[3].toString();
				String comparator = row[4].toString();
				// if where value 2 is empty
				// where value is a scalar
				// if where value 2 is not empty
				// what means where value is a table name
				// and where value 2 is a column name
				Object whereValue = row[5];
				Object whereValue2 = row[6];
				
				// if it is a table
				// we do not know the correct primary key
				// so we exec a query to determine if we should use the current selectedProperty
				// or keep it as PRIM_KEY_PLACEHOLDER
				IQuerySelector selector = null;
				if(MasterDatabaseUtility.getTableForColumn(currAppId, whereCol) == null) {
					selector = new QueryColumnSelector(whereTable);
				} else {
					selector = new QueryColumnSelector(whereTable + "__" + whereCol);
				}
				NounMetadata lhs = new NounMetadata(selector, PixelDataType.COLUMN);
				
				// let us address the portion when we have another column
				// so whereValue2 is empty and comparator is not between
				if(!whereValue2.toString().isEmpty() && !comparator.equals("between")) {
					// my lhs is another column
					NounMetadata rhs = new NounMetadata(new QueryColumnSelector(whereValue + "__" + whereValue2), PixelDataType.COLUMN);
					// add this filter
					SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
					curQs.addExplicitFilter(filter);
				} else {
					// we have to consider the comparators
					// so i can do the correct types
					if(comparator.contains(">") || comparator.contains("<")) {
						// it must numeric
						NounMetadata rhs = new NounMetadata(whereValue, PixelDataType.CONST_DECIMAL);

						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
						curQs.addExplicitFilter(filter);
					} else if(comparator.equals("between")) {
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
						// this must be an equals or not equals...
						
						PixelDataType type = PixelDataType.CONST_STRING;
						if(whereValue instanceof Number) {
							type = PixelDataType.CONST_DECIMAL;
						}
						
						NounMetadata rhs = new NounMetadata(whereValue, type);
						// add this filter
						SimpleQueryFilter filter = new SimpleQueryFilter(lhs, comparator, rhs);
						curQs.addExplicitFilter(filter);
					}
				}
				
			} 
			else if(part.equalsIgnoreCase("having")) {
//				
//				String havingCol = row[3].toString();
//				String havingAgg = row[4].toString();
//				String comparator = row[5].toString();
//				Object havingValue = row[6];
//				Object havingValue2 = row[7];
//				String havingValueAgg = row[8].toString();
//
//				// Check the Type of the operator and build filter
//				// NOTE -- It can only be compared to another number or string
//				if (!havingValueAgg.isEmpty()) {
//					// the aggregate is being compared to another aggregate
//
//					// this is what happens on the min and max -- does this functionality exist?
//
//					return null;
//				} else {
//					List<IQuerySelector> selectors = qs.getSelectors();
//					// transform aggregate to the semoss syntax
//					switch (havingAgg) {
//					case "avg":
//						havingAgg = "Average";
//						break;
//					case "sum":
//						havingAgg = "Sum";
//						break;
//					case "min":
//						havingAgg = "Min";
//						break;
//					case "max":
//						havingAgg = "Max";
//						break;
//					case "count":
//						havingAgg = "UniqueCount";
//						break;
//					}
//					for (IQuerySelector sel : selectors) {
//						// get the selector that has the correct aggregate from the having clause
//						if (sel.toString().contains(havingAgg) && sel.toString().contains(havingCol)) {
//							if (havingOperator.equals("=")) {
//								if (StringUtils.isNumber(havingValue) || StringUtils.isNumber(havingValue2)) {
//									havingType = PixelDataType.CONST_DECIMAL;
//								}
//								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
//										havingOperator, (new NounMetadata(havingValue, havingType))));
//							} else if (havingOperator.equals("<") || havingOperator.equals(">")) {
//								havingType = PixelDataType.CONST_DECIMAL;
//								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
//										havingOperator, (new NounMetadata(havingValue, havingType))));
//							} else if (havingOperator.equals("between")) {
//								havingType = PixelDataType.CONST_DECIMAL;
//								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
//										">", (new NounMetadata(havingValue, havingType))));
//								qs.addHavingFilter(new SimpleQueryFilter(new NounMetadata(sel, PixelDataType.COLUMN),
//										"<", (new NounMetadata(havingValue2, havingType))));
//							}
//						}
//					}
//				}
			} else if(part.equalsIgnoreCase("group")) {
				String groupConcept = row[2].toString();
				String groupProperty = row[3].toString();
				curQs.addGroupBy(groupConcept, groupProperty);
			} 
			
		}
		
		List<Map<String, String>> retMap = new Vector<Map<String, String>>();
		for(SelectQueryStruct qs : qsList) {
			Map<String, String> map = new HashMap<String, String>();
			
			String appId = qs.getEngineId();
			String appName = MasterDatabaseUtility.getEngineAliasForId(appId);
			map.put("app_id", appId);
			map.put("app_name", appName);
			String finalPixel = buildPixelFromQs(qs, appId);
			finalPixel += "Panel ( 0 ) | SetPanelLabel(\"" + appName + ": " + queryInput + "\");";
			map.put("pixel", finalPixel);
			map.put("layout", "Grid");
			retMap.add(map);
		}
		
		return retMap;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/**
	 * Build the pixel based on the query struct and app id
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
		psb.append("Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;");
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
				if(simpleF.getComparator().equals("=")) {
					psb.append(" == ");
				} else {
					psb.append(" ").append(simpleF.getComparator()).append(" ");
				}
				NounMetadata rhs = simpleF.getRComparison();
				PixelDataType rhsType = rhs.getNounType();
				if(rhsType == PixelDataType.COLUMN) {
					psb.append(rhs.getValue() + "");
				} else if(rhsType == PixelDataType.CONST_STRING) {
					Object val = rhs.getValue();
					if(val instanceof List) {
						List<String> vList = (List<String>) val;
						int size = vList.size();
						psb.append("[");
						for(int i = 0; i < size; i++) {
							if(i == 0) {
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
					Object val = rhs.getValue();
					if(val instanceof List) {
						List<String> vList = (List<String>) val;
						int size = vList.size();
						psb.append("[");
						for(int i = 0; i < size; i++) {
							if(i == 0) {
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

		// final import statement
		psb.append("Import ( frame = [ " + frameName + " ] ) ;");

		// now that we have the data, visualize in grid
		psb.append("Panel ( 0 ) | SetPanelView ( \"visualization\" );");
		psb.append(
				"Frame () | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ;");

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
	
	
}