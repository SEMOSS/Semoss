//package prerna.util.usertracking;
//
//import java.io.BufferedReader;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.ds.OwlTemporalEngineMeta;
//import prerna.engine.api.IEngine;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.impl.AbstractEngine;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.nameserver.utility.MasterDatabaseUtility;
//import prerna.om.Insight;
//import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
//import prerna.query.querystruct.HardSelectQueryStruct;
//import prerna.query.querystruct.SelectQueryStruct;
//import prerna.query.querystruct.selectors.IQuerySelector;
//import prerna.query.querystruct.selectors.QueryColumnSelector;
//import prerna.query.querystruct.selectors.QueryFunctionSelector;
//import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.sablecc2.om.task.options.TaskOptions;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//
//public class GoogleAnalytics implements IGoogleAnalytics {
//
//	/**
//	 * Constructor is protected so it can only be created by the builder
//	 */
//	protected GoogleAnalytics() {
//
//	}
//
//	public static HashMap<String, ArrayList<String>> logicalLookup = new HashMap<>();
//
//	@Override
//	public void track(String thisExpression, String thisType) {
//		GoogleAnalyticsThread ga = new GoogleAnalyticsThread(thisExpression, thisType);
//		// fire and release
//		ga.start();
//	}
//
//	@Override
//	public void track(String thisExpression, String thisType, String prevExpression, String prevType) {
//		GoogleAnalyticsThread ga = new GoogleAnalyticsThread(thisExpression, thisType, prevExpression, prevType);
//		// fire and release
//		ga.start();
//	}
//
//	@Override
//	public void track(String thisExpression, String thisType, String prevExpression, String prevType, String userId) {
//		GoogleAnalyticsThread ga = new GoogleAnalyticsThread(thisExpression, thisType, prevExpression, prevType,
//				userId);
//		// fire and release
//		ga.start();
//	}
//
//	@Override
//	public void trackAnalyticsPixel(Insight in, String routine) {
//		String expression = "{\"analytics\":{\"analyticalRoutineName\":\"" + routine + "\"}}";
//		in.trackPixels("analytics", expression);
//	}
//
//	@Override
//	public void trackDataImport(Insight in, SelectQueryStruct qs) {
//		final String exprStart = "{\"dataquery\":[";
//		final String exprEnd = "]}";
//
//		HashMap<String, String[]> joinHash = new HashMap();
//		String engineName = qs.getEngineId();
//		if (qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
//			// person has entered their own query
//			String query = ((HardSelectQueryStruct) qs).getQuery();
//			String expression = exprStart + "{\"dbName\":\"" + MasterDatabaseUtility.getEngineAliasForId(engineName)
//					+ "\",\"dbId\":\"" + engineName
//					+ "\", \"tableName\":\"null\",\"columnName\":\"\",	\"joinType\": \"\",\"joinColumn\": \"\", \"query\":\""
//					+ query + "\"}" + exprEnd;
//			in.trackPixels("dataquery:", expression);
//		} else if (qs.getQsType() == QUERY_STRUCT_TYPE.CSV_FILE) {
//			String expression = exprStart
//					+ "{\"dbName\":\"CSV_FILE\",\"dbId\":\"CSV_FILE\", \"tableName\":\"null\",\"columnName\":\"\",	\"joinType\": \"\",\"joinColumn\": \"\", \"query\":\"\"}" + exprEnd;
//			in.trackPixels("dataquery:", expression);
//		} else if (qs.getQsType() == QUERY_STRUCT_TYPE.EXCEL_FILE){
//			String expression = exprStart + "{\"dbName\":\"EXCEL_FILE\",\"dbId\":\"EXCEL_FILE\",\"tableName\":\"null\",\"columnName\":\"\",	\"joinType\": \"\",\"joinColumn\": \"\", \"query\":\"\"}" + exprEnd;
//			
//			in.trackPixels("dataquery:", expression);
//		} else {
//			// get join relationships and put in a map to easily retrieve later by column
//			Map<String, Map<String, List>> relations = qs.getRelations();
//			for (String relationKey : relations.keySet()) {
//				Map<String, List> joinTable = relations.get(relationKey);
//				for (String joinKey : joinTable.keySet()) {
//					List cols = (List) joinTable.get(joinKey);
//					for (int i = 0; i < cols.size(); i++) {
//						String[] data = { joinKey, relationKey };
//						joinHash.put(cols.get(i) + "", data);
//					}
//				}
//			}
//
//			// person is using pixel so there is a query struct w/ selectors
//			List<IQuerySelector> selectors = qs.getSelectors();
//			int size = selectors.size();
//			int counter = 0;
//			StringBuilder exprBuilder = new StringBuilder();
//			for (int i = 0; i < size; i++) {
//				// loop through the selectors
//				IQuerySelector s = selectors.get(i);
//				if (s.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
//					if (counter > 0) {
//						exprBuilder.append(",");
//					}
//					QueryColumnSelector selector = (QueryColumnSelector) s;
//					String tableName = selector.getTable();
//					String columnName = selector.getColumn();
//					// retrieve join data from the joinHash
//					String[] joinData;
//					if (columnName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
//						joinData = joinHash.get(tableName);
//					} else {
//						joinData = joinHash.get(columnName);
//					}
//					String joinType = "";
//					String joinColumn = "";
//					if (joinData != null) {
//						joinType = joinData[0];
//						joinColumn = joinData[1];
//					}
//					// build the json expression
//					exprBuilder.append("{\"dbName\":\"").append(MasterDatabaseUtility.getEngineAliasForId(engineName)).append("\",\"dbId\":\"").append(engineName).append("\",\"tableName\":\"")
//					.append(tableName).append("\",\"columnName\":\"").append(columnName)
//					.append("\",\"joinType\":\"").append(joinType).append("\",\"joinColumn\":\"").append(joinColumn).append("\",\"query\":\"\"}");
//					// increase counter
//					counter++;
//				}
//			}
//			in.trackPixels("dataquery", exprStart + exprBuilder.toString() + exprEnd);
//		}
//	}
//
//	@Override
//	public void trackInsightExecution(Insight in, String type, String engineName, String rdbmsId, String insightName) {
//		String curExpression = "{\"" + type + "\":{\"dbName\":\"" + engineName + "\",\"insightID\":\"" + rdbmsId + "\",\"insightName\":\"" + insightName + "\"}}";
//		in.trackPixels(type, curExpression);
//	}
//
//	@Override
//	public void trackExcelUpload(String tableName, String fileName,
//			List<Map<String, Map<String, String[]>>> headerDataTypes) {
//
//		fileName = fileName.substring(0, fileName.length() - 24);
//		final String exprStart = "{\"upload\":{\"" + fileName + "\":[";
//		final String exprEnd = "]}}";
//		// String userID = request.getSession().getId();
//
//		// if (userID != null && userID.equals("-1")) {
//		// userID = null;
//		// }
//		StringBuilder exprBuilder = new StringBuilder();
//		Map<String, Map<String, String[]>> map = headerDataTypes.get(0);
//		for (Entry<String, Map<String, String[]>> entry : map.entrySet()) {
//			String[] gaHeaders = map.get(entry.getKey()).get("headers");
//			int counter = 0;
//			for (int j = 0; j < gaHeaders.length; j++) {
//				if (counter > 0) {
//					exprBuilder.append(",");
//				}
//				exprBuilder.append("{\"dbName\":\"").append(tableName).append("\",\"columnName\":\"").append(gaHeaders[j]).append("\"}");
//				counter++;
//			}
//
//			GoogleAnalyticsThread ga = new GoogleAnalyticsThread(exprStart + exprBuilder.toString() + exprEnd,
//					"upload");
//			// fire and release...
//			ga.start();
//		}
//
//	}
//
//	@Override
//	public void trackCsvUpload(String files, String dbName, List<Map<String, String[]>> headerDataTypes) {
//		String fileName = files.substring(files.lastIndexOf("\\") + 1, files.lastIndexOf("."));
//		fileName = fileName.substring(0, fileName.length() - 24);
//		final String exprStart = "{\"upload\":{\"" + fileName + "\":[";
//		final String exprEnd = "]}}";
//
//		StringBuilder exprBuilder = new StringBuilder();
//
//		for (int i = 0; i < headerDataTypes.size(); i++) {
//			String[] gaHeaders = headerDataTypes.get(i).get("headers");
//			int counter = 0;
//			for (int j = 0; j < gaHeaders.length; j++) {
//				if (counter > 0) {
//					exprBuilder.append(",");
//				}
//				exprBuilder.append("{\"dbName\":\"").append(dbName).append("\",\"columnName\":\"").append(gaHeaders[j]).append("\"}");
//				counter++;
//			}
//			GoogleAnalyticsThread ga = new GoogleAnalyticsThread(exprStart + exprBuilder.toString() + exprEnd, "upload");
//			// fire and release...
//			ga.start();
//		}
//	}
//
//	@Override
//	public void trackDragAndDrop(Insight in, List<String> headers, String FileName) {
//		final String exprStart = "{\"draganddrop\":{\"" + FileName + "\":[{";
//		final String exprEnd = "}]}}";
//		StringBuilder exprBuilder = new StringBuilder();
//
//		int count = 1;
//		for (int i = 0; i < headers.size(); i++) {
//			exprBuilder.append("\"columnName").append(count).append("\":\"").append(headers.get(i)).append("\"");
//			if (i != (headers.size() - 1)) {
//				exprBuilder.append(",");
//			}
//			count++;
//		}
//		in.trackPixels("draganddrop", exprStart + exprBuilder.toString() + exprEnd);
//	}
//
//	@Override
//	public void trackViz(TaskOptions taskOptions, Insight in, SelectQueryStruct qs) {
//		List kickOffColumns = new ArrayList<>();
//		try {
//			if (taskOptions == null || taskOptions.isEmpty()) {
//				return;
//			}
//			ITableDataFrame frame = qs.getFrame();
//			if(frame == null) {
//				frame = (ITableDataFrame) in.getDataMaker();
//			}
//			if (frame == null) {
//				return;
//			}
//			OwlTemporalEngineMeta meta = frame.getMetaData();
//			try{
//				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
//			} catch(Exception e){
//				return;
//			}
//			// keep the alias to bind to the correct meta
//			Map<String, String> aliasHash = new HashMap<String, String>();
//
//			// has to be defined after qs is converted to physical
//			List<IQuerySelector> selectors = qs.getSelectors();
//
//			// loop through QS
//			// figure out which selector column is part of the
//			for (int i = 0; i < selectors.size(); i++) {
//				IQuerySelector selector = selectors.get(i);
//				String alias = selector.getAlias();
//				String name = "";
//				if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
//					// TODO: this is assuming only 1 math inside due to FE
//					// limitation
//					name = ((QueryFunctionSelector) selector).getInnerSelector().get(0).getQueryStructName() + "";
//				} else {
//					name = selector.getQueryStructName();
//				}
//				aliasHash.put(alias, name);
//			}
//	
//			for (String panelId : taskOptions.getPanelIds()) {
//				if (panelId.equalsIgnoreCase("rule")){
//					return;
//				}
//				final String exprStart = "{\"Viz\":{";
//				final String exprEnd = "]}}";
//				StringBuilder exprBuilder = new StringBuilder();
//				String vizType = taskOptions.getLayout(panelId);
//
//				// alignment points to a map of string to vector
//				Map<String, Object> alignmentMap = taskOptions.getAlignmentMap(panelId);
//				if(alignmentMap == null) {
//					continue;
//				}
//				boolean first = true;
//				for (String uiCompName : alignmentMap.keySet()) {
//					// ui name can be label, value, x, y, etc.
//					List<String> columnsInUICompName = (List<String>) alignmentMap.get(uiCompName);
//					// now we want to generate a map for each input in this uiCompName
//					for (String columnAlias : columnsInUICompName) {
//						String uniqueMetaName = aliasHash.get(columnAlias);
//						List<String[]> dbInfo = meta.getDatabaseInformation(uniqueMetaName);
//						if (!first) {
//							exprBuilder.append(",");
//						} else {
//							first = false;
//						}
//						exprBuilder.append("[");
//						int size = dbInfo.size();
//						boolean processedFirst = false;
//						for (int i = 0; i < size; i++) {
//							String[] engineQs = dbInfo.get(i);
//							if (engineQs.length != 2) {
//								continue;
//							}
//							String db = engineQs[0];
//							String conceptProp = engineQs[1];
//							String table = conceptProp;
//							String column = SelectQueryStruct.PRIM_KEY_PLACEHOLDER;
//							if (conceptProp.contains("__")) {
//								String[] conceptPropSplit = conceptProp.split("__");
//								table = conceptPropSplit[0];
//								column = conceptPropSplit[1];
//							}
//							
//							// get data type
//							String dataType = meta.getHeaderTypeAsString(uniqueMetaName);
//							// get unique values
//							long uniqueValues = 0;
//							if(db != null) {
//								IEngine engine = Utility.getEngine(db);
//								if(engine != null) {
//									RDFFileSesameEngine owlEngine = ((AbstractEngine) engine).getBaseDataEngine();
//
//									// get unique values for string columns, if it doesnt exist
//									// or isnt a string then it is defaulted to zero
//									String queryCol = column;
//									// prim key placeholder cant be queried in
//									// the owl
//									// so we convert it back to the display name
//									// of the concept
//									if (column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
//										queryCol = table;
//									}
//									String uniqueValQuery = "SELECT DISTINCT ?concept ?unique WHERE "
//											+ "{ BIND(<http://semoss.org/ontologies/Concept/" + queryCol + "/" + table
//											+ "> AS ?concept)"
//											+ "{?concept <http://semoss.org/ontologies/Relation/Contains/UNIQUE> ?unique}}";
//									IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(owlEngine,
//											uniqueValQuery);
//									while (it.hasNext()) {
//										Object[] row = it.next().getValues();
//										uniqueValues = Long.parseLong(row[1].toString());
//									}
//								}
//							}
//
//							if (processedFirst) {
//								exprBuilder.append(",");
//							} else {
//								processedFirst = true;
//							}
//							exprBuilder.append("{\"dbName\":\"").append(db)
//									.append("\",\"tableName\": \"").append(table)
//									.append("\",\"columnName\": \"").append(column)
//									.append("\",\"columnType\":\"").append(dataType)
//									.append("\",\"component\": \"").append(uiCompName)
//									.append("\",\"uniqueValues\": ").append(uniqueValues);
//
//							// if lookup map is empty, initialize it
//							String uniqueName = db + "_" + table + "_" + column;
//							if (logicalLookup.isEmpty()) {
//								// go get the csv and populate it
//								initializeLogicalLookup();
//							}
//							ArrayList<String> logicalNamesList = logicalLookup.get(uniqueName);
//							// TODO: if the list is empty then we will run
//							// semantic blending
//							// in the GA thread to store those values for next
//							// time
//
//							// send empty value to keep the json structure
//							// consistent
//							if (logicalNamesList == null) {
//								logicalNamesList = new ArrayList<String>();
//								// kickOffColumns.add(columnAlias);
//								logicalNamesList.add("");
//								logicalNamesList.add("");
//								logicalNamesList.add("");
//								logicalNamesList.add("");
//								logicalNamesList.add("");
//							}
//							// always send exactly 5 elements
//							exprBuilder.append(",\"reference1\": \"").append(db).append("$").append(table).append("$")
//									.append(column);
//							for (int j = 0; j < 5; j++) {
//								if (j < logicalNamesList.size()) {
//									exprBuilder.append("\",\"reference").append(j + 2).append("\": \"")
//											.append(logicalNamesList.get(j));
//								} else {
//									exprBuilder.append("\",\"reference").append(j + 2).append("\": \"").append("");
//								}
//							}
//							exprBuilder.append("\"}");
//						}
//						exprBuilder.append("]");
//					}
//				}
//				// we are done for this panel track it
//				// and then go to the next one
//				String curExpression = exprStart + "\"" + vizType  + "\""+ ":[" + exprBuilder.toString() + exprEnd;
//				in.trackPixels("viz", curExpression);
//			}
//
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//
//	public void initializeLogicalLookup() {
//		String csvDir = DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations\\historicalData\\logicalNames.csv";
//		BufferedReader in;
//		try {
//			in = new BufferedReader(new FileReader(csvDir));
//		
//		String line;
//		// read each line in the csv and load into lookup map,
//		// only done on startup. map is updated directly moving forward
//		while ((line = in.readLine()) != null) {
//			String columns[] = line.split(",");
//			if (columns.length < 2) {
//				continue;
//			}
//			String key = columns[0];
//			ArrayList<String> list = logicalLookup.get(key);
//
//			// split and add each to list
//			List listArray;
//			if (list == null) {
//				list = new ArrayList<>();
//				String value = columns[1];
//				// elements is logical names film;test;other;
//				String[] elements = null;
//				if (value != null) {
//					elements = value.split(";");
//				}
//				for (int i = 0; i < elements.length; i++) {
//					list.add(elements[i]);
//				}
//				logicalLookup.put(key, list);
//			}
//		}
//		in.close();
//		} catch (FileNotFoundException e) {
//			// classLogger.error(Constants.STACKTRACE, e);
//		} catch (IOException e) {
//			// classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//
//	@Override
//	public void addNewLogicalNames(Map<String, Object> newLogicals, String[] columns, ITableDataFrame frame){
//		OwlTemporalEngineMeta meta = frame.getMetaData();
//		String csvDir = DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations\\historicalData\\logicalNames.csv";
//		String currentColumn = "";
//		int recommendationCount = 0;
//		// iterate columns map and extract logical names
//		// to put into csv and logicalLookup
//		for (int i = 0; i < columns.length; i++) {
//			String colName = columns[i];
//			String uniqueCol = meta.getUniqueNameFromAlias(colName);
//			List<String[]> dbInfo = meta.getDatabaseInformation(uniqueCol);
//			// generate a uniqueName used as key to lookup logicals in the hashmap
//			String uniqueName = "";
//			for (int j = 0; j < dbInfo.size(); j++) {
//				String[] engineQs = dbInfo.get(j);
//				if (engineQs.length != 2) {
//					continue;
//				}
//				String db = engineQs[0];
//				String conceptProp = engineQs[1];
//				String tableName = conceptProp;
//				String column = SelectQueryStruct.PRIM_KEY_PLACEHOLDER;
//				if (conceptProp.contains("__")) {
//					String[] conceptPropSplit = conceptProp.split("__");
//					tableName = conceptPropSplit[0];
//					column = conceptPropSplit[1];
//				}
//				uniqueName = db + "_" + tableName + "_" + column;
//			}
//			String dataType = meta.getHeaderTypeAsString(uniqueCol);
//			// add the new semantic names to the csv and logicalLookup map
//			if (newLogicals != null && !(newLogicals.isEmpty())) {
//				ArrayList data = (ArrayList<Object>) newLogicals.get("data");
//				// string builder for csv file
//				StringBuilder csvLogicals = new StringBuilder();
//				// list for the hash map
//				ArrayList<String> list = new ArrayList<String>();
//				// Iterate newlogicals data list and combine all logical names with a semi.
//				
//				// For each column add logicals to list, if its a new column dont 
//				// update count, come back to that element to add to next column list.
//				while (recommendationCount < data.size()) {
//					Object[] objects = (Object[]) data.get(recommendationCount);
//					if ((objects[0] + "").equals(currentColumn) || currentColumn.equals("")) {
//						String name = objects[1] + "";
//						// TODO: we only track 5 max, what about ties?
//						if (list.size() < 6) {
//							csvLogicals.append(name.replace(",", " ")).append(";");
//							list.add(name);
//						}
//						currentColumn = objects[0] + "";
//						recommendationCount++;
//					} else {
//						currentColumn = objects[0] + "";
//						// end of elements for that column so break loop
//						break;
//					}
//				}
//				// make sure the lookup was initialized first
//				if (logicalLookup.isEmpty()) {
//					initializeLogicalLookup();
//				}
//				// TODO: if it already exists dont add it again for now. Update map to H2 which will be updated.
//				if (!(logicalLookup.containsKey(uniqueName)) && dataType.equals("STRING")) {
//					// add to the map
//					logicalLookup.put(uniqueName, list);
//					// also persist to csv - for each in list append
//					try {
//						FileWriter csv = new FileWriter(csvDir, true);
//						csv.append(uniqueName).append(",").append(csvLogicals.toString()).append("\n");
//						csv.close();
//					} catch (IOException e) {
//						classLogger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//		}
//	}
//	
//	@Override
//	public ArrayList<String> getLogicalNames(String uniqueName) {
//		return logicalLookup.get(uniqueName);
//	}
//
//	@Override
//	public void trackDescriptions(Insight in, String engineId, String engineAlias, HashMap<String,Map> descriptions) {
//		final String exprStart = "{\"datasemantic\":[{\"dbName\": \"" + engineAlias + "\", \"dbId\": \"" + engineId + "\", \"tables\": [{ ";
//		final String exprEnd = "}]}]}";
//		StringBuilder sb = new StringBuilder();
//		boolean firstTab = true;
//		for(String table : descriptions.keySet()){
//			if(firstTab){
//				firstTab = false;
//			}else{
//				sb.append(", ");
//			}
//			Map tableDetail = descriptions.get(table);
//			sb.append("\"" + table + "\" : [{");
//			boolean firstCol = true;
//			for(Object column : tableDetail.keySet()){
//				if(firstCol){
//					firstCol = false;
//				}else{
//					sb.append(", {");
//				}
//				String description = tableDetail.get(column) + "";
//				description = description.replaceAll("\"", "'");
//				
//				sb.append("\"columnName\": \"" + column + "\", \"description\": \"" + description + "\"}");
//			}
//			sb.append("]");
//		}
//		in.trackPixels("datasemantic", exprStart + sb.toString() + exprEnd);
//	}
//}
//
