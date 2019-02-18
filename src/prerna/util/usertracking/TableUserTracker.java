package prerna.util.usertracking;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractFileQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.AbstractQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.util.Utility;

public class TableUserTracker implements IUserTracker {

	/**
	 * Send the request for tracking
	 * @param type
	 * @param rows
	 */
	private void sendTrackRequest(String type, List<Object[]> rows) {
		TrackRequestThread t = new TrackRequestThread(type, rows);
		t.start();
	}
	
	@Override
	public void trackVizWidget(Insight in, TaskOptions taskOptions, SelectQueryStruct qs) {
		if (taskOptions == null || taskOptions.isEmpty()) {
			return;
		}
		ITableDataFrame frame = qs.getFrame();
		if(frame == null) {
			frame = (ITableDataFrame) in.getDataMaker();
		}
		if (frame == null) {
			return;
		}
		OwlTemporalEngineMeta meta = frame.getMetaData();
		try{
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
		} catch(Exception e){
			return;
		}
		
		// keep the alias to bind to the correct meta
		Map<String, String> aliasHash = new HashMap<String, String>();
		// has to be defined after qs is converted to physical
		List<IQuerySelector> selectors = qs.getSelectors();
		
		// loop through QS
		// figure out which selector column is part of the
		for (int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String name = "";
			if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				// TODO: this is assuming only 1 math inside due to FE limitation
				name = ((QueryFunctionSelector) selector).getInnerSelector().get(0).getQueryStructName() + "";
			} else {
				name = selector.getQueryStructName();
			}
			aliasHash.put(alias, name);
		}
		
		List<Object[]> rows = new Vector<Object[]>();
		
		String id = UUID.randomUUID().toString();
		String[] insightDetails = getInsightDetailsString(in);
		String sessionId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0] + "");
		String insightId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1] + "");
		String userId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2] + "");
		String time = insightDetails[3];
		
		for (String panelId : taskOptions.getPanelIds()) {
			String layout = taskOptions.getLayout(panelId);
			Map<String, Object> alignmentMap = taskOptions.getAlignmentMap(panelId);
			if(alignmentMap == null) {
				// ummm
				// there is a weird task option
				// could be a color by value thing
				// will just track this as data query
				trackQueryData(in, qs);
			} else {
				for (String uiCompName : alignmentMap.keySet()) {
					// ui name can be label, value, x, y, etc.
					if(!(alignmentMap.get(uiCompName) instanceof List)) {
						continue;
					}
					List<String> columnsInUICompName = (List<String>) alignmentMap.get(uiCompName);
					// now we want to generate a map for each input in this uiCompName
					for (String columnAlias : columnsInUICompName) {
						String uniqueMetaName = aliasHash.get(columnAlias);
						List<String[]> dbInfo = meta.getDatabaseInformation(uniqueMetaName);
						int size = dbInfo.size();
						for(int i = 0; i < size; i++) {
							String[] engineQs = dbInfo.get(i);
							if (engineQs.length != 2) {
								continue;
							}
							String engineId = engineQs[0];
							String engineName = MasterDatabaseUtility.getEngineAliasForId(engineId);
							if(engineName == null) {
								engineName = engineId;
							}
							String table = engineQs[1];
							String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;
							if(table.contains("__")) {
								String[] split = table.split("__");
								table = split[0];
								column = split[1];
							}
							String dataType = meta.getHeaderTypeAsString(uniqueMetaName);
							Long uniqueCount = getUniqueValueCount(engineId, table, column);
							
							Object[] row = new Object[15];
							row[0] = id;
							// engine id
							row[1] = engineId;
							// engine name
							row[2] = engineName;
							// table name
							row[3] = table;
							// column name
							row[4] = column;
							// datatype
							row[5] = dataType;
							// count
							row[6] = uniqueCount;
							// input type
							row[7] = "VISUALIZATION";
							// input subtype
							row[8] = layout;
							// input name
							row[9] = uiCompName;
							// input value
							row[10] = columnAlias;
							// session id
							row[11] = sessionId;
							// insight id
							row[12] = insightId;
							// user id
							row[13] = userId;
							// time
							row[14] = time;
							// add batch
							rows.add(row);
						}
					}
				}
			}
			
			// track
			sendTrackRequest("widget", rows);
		}
	}

	@Override
	public void trackAnalyticsWidget(Insight in, ITableDataFrame frame, String routineName, Map<String, List<String>> keyValue) {
		List<Object[]> rows = new Vector<Object[]>();
		
		OwlTemporalEngineMeta meta = null;
		if(frame != null) {
			meta = frame.getMetaData();
		}
		
		String id = UUID.randomUUID().toString();
		String[] insightDetails = getInsightDetailsString(in);
		String sessionId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0] + "");
		String insightId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1] + "");
		String userId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2] + "");
		String time = insightDetails[3];
		
		if(keyValue == null || keyValue.isEmpty()) {
			// routine has no inputs
			Object[] row = new Object[15];
			row[0] = id;
			// input type
			row[7] = "ANALYTICS";
			// input subtype
			row[8] = routineName;
			// session id
			row[11] = sessionId;
			// insight id
			row[12] = insightId;
			// user id
			row[13] = userId;
			// time
			row[14] = time;
			// add batch
			rows.add(row);
		} else {
			// we have some inputs!
			for(String inputName : keyValue.keySet()) {
				List<String> inputValues = keyValue.get(inputName);
				for(String inputValue : inputValues) {
					
					// see if we can go from the base inputs 
					// if it is a column
					List<String[]> dbInfo = null;
					String uniqueMetaName = null;
					if(meta != null) {
						uniqueMetaName = meta.getUniqueNameFromAlias(inputValue);
						if(uniqueMetaName != null) {
							dbInfo = meta.getDatabaseInformation(uniqueMetaName);
						}
					}
					
					if(dbInfo == null) {
						// not a column

						// routine has no inputs
						Object[] row = new Object[15];
						row[0] = id;
						// input type
						row[7] = "ANALYTICS";
						// input subtype
						row[8] = routineName;
						// input name
						row[9] = inputName;
						// input value
						row[10] = inputValue;
						// session id
						row[11] = sessionId;
						// insight id
						row[12] = insightId;
						// user id
						row[13] = userId;
						// time
						row[14] = time;
						// add batch
						rows.add(row);
					} else {
						// we have a column
						int size = dbInfo.size();
						for(int i = 0; i < size; i++) {
							String[] engineQs = dbInfo.get(i);
							if (engineQs.length != 2) {
								// couldn't figure out the column
								// just add as if normal
								Object[] row = new Object[15];
								row[0] = id;
								// input type
								row[7] = "ANALYTICS";
								// input subtype
								row[8] = routineName;
								// input name
								row[9] = inputName;
								// input value
								row[10] = inputValue;
								// session id
								row[11] = sessionId;
								// insight id
								row[12] = insightId;
								// user id
								row[13] = userId;
								// time
								row[14] = time;
								// add batch
								rows.add(row);
								continue;
							}
							String engineId = engineQs[0];
							String engineName = MasterDatabaseUtility.getEngineAliasForId(engineId);
							if(engineName == null) {
								engineName = engineId;
							}
							String table = engineQs[1];
							String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;
							if(table.contains("__")) {
								String[] split = table.split("__");
								table = split[0];
								column = split[1];
							}
							String dataType = meta.getHeaderTypeAsString(uniqueMetaName);
							Long uniqueCount = getUniqueValueCount(engineId, table, column);
							
							Object[] row = new Object[15];
							row[0] = id;
							// engine id
							row[1] = engineId;
							// engine name
							row[2] = engineName;
							// table name
							row[3] = table;
							// column name
							row[4] = column;
							// datatype
							row[5] = dataType;
							// count
							row[6] = uniqueCount;
							// input type
							row[7] = "ANALYTICS";
							// input subtype
							row[8] = routineName;
							// input name
							row[9] = inputName;
							// input value
							row[10] = inputValue;
							// session id
							row[11] = sessionId;
							// insight id
							row[12] = insightId;
							// user id
							row[13] = userId;
							// time
							row[14] = time;
							// add batch
							rows.add(row);
						}
					}
				}
			}
		}
		
		sendTrackRequest("widget", rows);
	}
		
	@Override
	public void trackDataImport(Insight in, SelectQueryStruct qs) {
		trackSelectQueryStructData(in, qs, "DATA_IMPORT");
	}
	
	@Override
	public void trackQueryData(Insight in, SelectQueryStruct qs) {
		trackSelectQueryStructData(in, qs, "DATA_QUERY");
	}

	private void trackSelectQueryStructData(Insight in, SelectQueryStruct qs, String widgetType) {
		List<Object[]> rows = new Vector<Object[]>();
		
		String id = UUID.randomUUID().toString();
		String[] insightDetails = getInsightDetailsString(in);
		String sessionId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0] + "");
		String insightId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1] + "");
		String userId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2] + "");
		String time = insightDetails[3];
		
		// we are currently only tracking the selector information
		List<IQuerySelector> selectors = qs.getSelectors();
		
		QUERY_STRUCT_TYPE type = qs.getQsType();
		if(type == QUERY_STRUCT_TYPE.ENGINE) {
			IEngine engine = qs.retrieveQueryStructEngine();
			String engineId = engine.getEngineId();
			String engineName = engine.getEngineName();
			
			for(IQuerySelector selector : selectors) {
				String name = selector.getQueryStructName();
				String dataType = selector.getDataType();
				String table = name;
				String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;
				Long uniqueCount = null;

				if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
					// TODO: this is assuming only 1 math inside due to FE limitation
					name = ((QueryFunctionSelector) selector).getInnerSelector().get(0).getQueryStructName() + "";
					uniqueCount = getUniqueValueCount(engineId, name, column);
				} else {
					uniqueCount = getUniqueValueCount(engineId, table, column);
				}
				
				if(name.contains("__")) {
					String[] split = name.split("__");
					table = split[0];
					column = split[1];
				}
				
				if(dataType == null) {
					if(!column.equals(AbstractQuerySelector.PRIM_KEY_PLACEHOLDER)) {
						dataType = MasterDatabaseUtility.getBasicDataType(engineId, column, table);
					} else {
						dataType = MasterDatabaseUtility.getBasicDataType(engineId, table, null);
					}
				}
				
				
				Object[] row = new Object[15];
				row[0] = id;
				// engine id
				row[1] = engineId;
				// engine name
				row[2] = engineName;
				// table name
				row[3] = table;
				// column name
				row[4] = column;
				// datatype
				row[5] = dataType;
				// count
				row[6] = uniqueCount;
				// input type
				row[7] = widgetType;
				// input subtype
				row[8] = "ENGINE";
				// input name
				row[9] = "SELECTOR";
				// input value
				row[10] = selector.getQueryStructName();
				// session id
				row[11] = sessionId;
				// insight id
				row[12] = insightId;
				// user id
				row[13] = userId;
				// time
				row[14] = time;
				// add batch
				rows.add(row);
			}
			
		} else if(type == QUERY_STRUCT_TYPE.FRAME) {
			ITableDataFrame frame = qs.getFrame();
			OwlTemporalEngineMeta meta = frame.getMetaData();
			try{
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
				// needs to be redefined
				selectors = qs.getSelectors();
			} catch(Exception e){
				return;
			}
			
			// keep the alias to bind to the correct meta
			Map<String, String> aliasHash = new HashMap<String, String>();
			
			// loop through QS
			// figure out which selector column is part of the
			for (int i = 0; i < selectors.size(); i++) {
				IQuerySelector selector = selectors.get(i);
				String alias = selector.getAlias();
				String name = "";
				if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
					// TODO: this is assuming only 1 math inside due to FE limitation
					name = ((QueryFunctionSelector) selector).getInnerSelector().get(0).getQueryStructName() + "";
				} else {
					name = selector.getQueryStructName();
				}
				aliasHash.put(alias, name);
			}
			
			for(IQuerySelector selector : selectors) {
				String uniqueMetaName = aliasHash.get(selector.getAlias());
				List<String[]> dbInfo = meta.getDatabaseInformation(uniqueMetaName);
				int size = dbInfo.size();
				if(size == 0) {
					// this is probably some complex derived column
					Object[] row = new Object[15];
					row[0] = id;
					// input type
					row[7] = widgetType;
					// input subtype
					row[8] = "FRAME";
					// input name
					row[9] = "SELECTOR";
					// input value
					row[10] = selector.getQueryStructName();
					// session id
					row[11] = sessionId;
					// insight id
					row[12] = insightId;
					// user id
					row[13] = userId;
					// time
					row[14] = time;
					// add batch
					rows.add(row);
				} else {
					for(int i = 0; i < size; i++) {
						String[] engineQs = dbInfo.get(i);
						if (engineQs.length != 2) {
							// this is probably some kind of derived column that was added
							Object[] row = new Object[15];
							row[0] = id;
							// input type
							row[7] = widgetType;
							// input subtype
							row[8] = "FRAME";
							// input name
							row[9] = "SELECTOR";
							// input value
							row[10] = selector.getQueryStructName();
							// session id
							row[11] = sessionId;
							// insight id
							row[12] = insightId;
							// user id
							row[13] = userId;
							// time
							row[14] = time;
							// add batch
							rows.add(row);
							continue;
						}
						String engineId = engineQs[0];
						String engineName = MasterDatabaseUtility.getEngineAliasForId(engineId);
						if(engineName == null) {
							engineName = engineId;
						}
						String table = engineQs[1];
						String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;
						if(table.contains("__")) {
							String[] split = table.split("__");
							table = split[0];
							column = split[1];
						}
						String dataType = meta.getHeaderTypeAsString(uniqueMetaName);
						Long uniqueCount = getUniqueValueCount(engineId, table, column);
						
						Object[] row = new Object[15];
						row[0] = id;
						// engine id
						row[1] = engineId;
						// engine name
						row[2] = engineName;
						// table name
						row[3] = table;
						// column name
						row[4] = column;
						// datatype
						row[5] = dataType;
						// count
						row[6] = uniqueCount;
						// input type
						row[7] = widgetType;
						// input subtype
						row[8] = "FRAME";
						// input name
						row[9] = "SELECTOR";
						// input value
						row[10] = selector.getQueryStructName();
						// session id
						row[11] = sessionId;
						// insight id
						row[12] = insightId;
						// user id
						row[13] = userId;
						// time
						row[14] = time;
						// add batch
						rows.add(row);
					}
				}
			}
			
		} else if(type == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			IEngine engine = qs.retrieveQueryStructEngine();
			String engineId = engine.getEngineId();
			String engineName = engine.getEngineName();
			
			Object[] row = new Object[15];
			row[0] = id;
			// engine id
			row[1] = engineId;
			// engine name
			row[2] = engineName;
			// input type
			row[7] = widgetType;
			// input subtype
			row[8] = "RAW_ENGINE_QUERY";
			// input name
			row[9] = "QUERY";
			// input value
			row[10] = ((HardSelectQueryStruct) qs).getQuery();
			if(row[10].toString().length() > 255) {
				row[10] = row[10].toString().substring(0, 252) + "...";
			}
			// session id
			row[11] = sessionId;
			// insight id
			row[12] = insightId;
			// user id
			row[13] = userId;
			// time
			row[14] = time;
			// add batch
			rows.add(row);
			
		} else if(type == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY){
			Object[] row = new Object[15];
			row[0] = id;
			// input type
			row[7] = widgetType;
			// input subtype
			row[8] = "RAW_FRAME_QUERY";
			// input name
			row[9] = "QUERY";
			// input value
			row[10] = ((HardSelectQueryStruct) qs).getQuery();
			if(row[10].toString().length() > 255) {
				row[10] = row[10].toString().substring(0, 252) + "...";
			}
			// session id
			row[11] = sessionId;
			// insight id
			row[12] = insightId;
			// user id
			row[13] = userId;
			// time
			row[14] = time;
			// add batch
			rows.add(row);
			
		} else if(type == QUERY_STRUCT_TYPE.CSV_FILE || type == QUERY_STRUCT_TYPE.EXCEL_FILE) {
			AbstractFileQueryStruct fileQs = (AbstractFileQueryStruct) qs;
			Map<String, String> types = fileQs.getColumnTypes();
			String filePath = fileQs.getFilePath();
			String fileName = FilenameUtils.getBaseName(filePath);
			String extension = FilenameUtils.getExtension(filePath);
			// remove the ugly stuff we add to make this unique
			if(fileName.contains("_____UNIQUE")) {
				fileName = fileName.substring(0, fileName.indexOf("_____UNIQUE"));
			}
			for(IQuerySelector selector : selectors) {
				QueryColumnSelector cSelector = (QueryColumnSelector) selector;
				String column = cSelector.getColumn();
				String dataType = types.get(column);
				
				Object[] row = new Object[15];
				row[0] = id;
				// engine id
				row[1] = fileName + "." + extension;
				// engine name
				row[2] = fileName;
				// table name
				row[3] = fileName;
				// column name
				row[4] = column;
				// datatype
				row[5] = dataType;
				// input type
				row[7] = widgetType;
				// input subtype
				row[8] = "FILE";
				// input name
				row[9] = "SELECTOR";
				// input value
				row[10] = selector.getQueryStructName();
				// session id
				row[11] = sessionId;
				// insight id
				row[12] = insightId;
				// user id
				row[13] = userId;
				// time
				row[14] = time;
				// add batch
				rows.add(row);
			}
			
		} else if(type == QUERY_STRUCT_TYPE.LAMBDA) {
			// i guess this is what we want?
			for(IQuerySelector selector : selectors) {
				Object[] row = new Object[15];
				row[0] = id;
				// input type
				row[7] = widgetType;
				// input subtype
				row[8] = "LAMBDA";
				// input name
				row[9] = "SELECTOR";
				// input value
				row[10] = selector.getQueryStructName();
				// session id
				row[11] = sessionId;
				// insight id
				row[12] = insightId;
				// user id
				row[13] = userId;
				// time
				row[14] = time;
				// add batch
				rows.add(row);
			}
		}
		
		sendTrackRequest("widget", rows);
	}
	
	@Override
	public void trackInsightExecution(Insight in) {
		List<Object[]> rows = new Vector<Object[]>();
		String[] insightDetails = getInsightDetailsString(in);
		Object[] row = new Object[7];
		row[0] = RdbmsQueryBuilder.escapeForSQLStatement(in.getEngineId());
		row[1] = RdbmsQueryBuilder.escapeForSQLStatement(in.getEngineName());
		row[2] = RdbmsQueryBuilder.escapeForSQLStatement(in.getRdbmsId());
		row[3] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0]);
		row[4] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1]);
		row[5] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2]);
		row[6] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[3]);
		rows.add(row);
		sendTrackRequest("insight", rows);
	}
	
	@Override
	public void trackPixelExecution(Insight in, String pixel, boolean meta) {
		List<Object[]> rows = new Vector<Object[]>();
		String[] insightDetails = getInsightDetailsString(in);
		Object[] row = new Object[6];
		row[0] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0]);
		row[1] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1]);
		row[2] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2]);
		row[3] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[3]);
		row[4] = RdbmsQueryBuilder.escapeForSQLStatement(pixel);
		row[5] = meta;
		rows.add(row);
		sendTrackRequest("pixel", rows);
	}
	
	@Override
	public void trackUserWidgetMods(List<Object[]> rows) {
		sendTrackRequest("widget", rows);
	}
	
	/**
	 * Return a string[] with
	 * session id
	 * insight id
	 * user id
	 * timestamp
	 * @param in
	 * @return
	 */
	private String[] getInsightDetailsString(Insight in) {
		String[] row = new String[4];
		VarStore vStore = in.getVarStore();
		row[0] = vStore.get(JobReactor.SESSION_KEY).getValue().toString();
		row[1] = in.getInsightId();
		User user = in.getUser();
		if(user != null && !user.getLogins().isEmpty()) {
			String userId = user.getAccessToken(user.getLogins().get(0)).getId();
			row[2] = userId;
		}
		row[3] = java.sql.Timestamp.valueOf(LocalDateTime.now()).toString();
		return row;
	}
	
	private Long getUniqueValueCount(String engineId, String table, String column) {
		if (StringUtils.countMatches(engineId, "-") != 4) {
			// Not really an engineID (drag and drop...)
			return null;
		}
		IEngine engine = Utility.getEngine(engineId);
		if(engine != null) {
			RDFFileSesameEngine owlEngine = ((AbstractEngine) engine).getBaseDataEngine();
			
			// are we dealing with a concept or a property
			String physicalUri = null;
			if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				// table
				physicalUri = engine.getConceptPhysicalUriFromConceptualUri(table);
			} else {
				// property
				physicalUri = engine.getPropertyPhysicalUriFromConceptualUri(column, table);
			}
			
			String uniqueValQuery = "SELECT DISTINCT ?concept ?unique WHERE "
					+ "{ BIND(<" + physicalUri + "> AS ?concept)"
					+ "{?concept <http://semoss.org/ontologies/Relation/Contains/UNIQUE> ?unique}}";
			IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(owlEngine, uniqueValQuery);
			while (it.hasNext()) {
				Object[] row = it.next().getValues();
				return Long.parseLong(row[1].toString());
			}
		}
		
		return null;
	}

	@Override
	public boolean isActive() {
		return true;
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
//	private static final String ENGINE_ID = "425b298c-6e5c-481b-b1bc-cc3fe7075185";
//	private RDBMSNativeEngine engine;
//	
//	/*
//	 * Headers and Types are not changing so lets only create these objects once
//	 */
//
//	private static final String[] WIDGET_TRACKING_INSERT_PS = new String[]{
//			"WIDGET_TRACKING",
//			"ID",
//			"ENGINE_ID",
//			"ENGINE_NAME",
//			"TABLE_NAME",
//			"COLUMN_NAME",
//			"DATA_TYPE",
//			"UNIQUE_COUNT",
//			"INPUT_TYPE",
//			"INPUT_SUBTYPE",
//			"INPUT_NAME",
//			"INPUT_VALUE",
//			"SESSION_ID",
//			"INSIGHT_ID",
//			"USER_ID",
//			"TIME"};
//	
//	private static final String PIXEL_EXECUTION_TABLE_NAME = "PIXEL_EXECUTION";
//	private static final String[] PIXEL_EXECUTION_HEADERS = new String[]{"SESSION_ID", "INSIGHT_ID", "USER_ID", "TIME", "PIXEL", "META"};
//	private static final String[] PIXEL_EXECUTION_TYPES = new String[]{"VARCHAR(255)", "VARCHAR(50)", "VARCHAR(100)", "TIMESTAMP", "CLOB", "BOOLEAN"};
//
//	private static final String INSIGHT_EXECUTION_TABLE_NAME = "INSIGHT_EXECUTION";
//	private static final String[] INSIGHT_EXECUTION_HEADERS = new String[]{"ENGINE_ID", "ENGINE_NAME", "RDBMS_ID", "SESSION_ID", "INSIGHT_ID", "USER_ID", "TIME"};
//	private static final String[] INSIGHT_EXECUTION_TYPES = new String[]{"VARCHAR(50)", "VARCHAR(255)", "VARCHAR(50)", "VARCHAR(255)", "VARCHAR(50)", "VARCHAR(100)", "TIMESTAMP"};
//	
//	/**
//	 * Constructor is protected so it can only be created by the builder
//	 */
//	protected RdbmsUserTracker() {
//		this.engine = (RDBMSNativeEngine) Utility.getEngine(ENGINE_ID);
//	}
//	
//	@Override
//	public void trackVizWidget(Insight in, TaskOptions taskOptions, SelectQueryStruct qs) {
//		if (taskOptions == null || taskOptions.isEmpty()) {
//			return;
//		}
//		ITableDataFrame frame = qs.getFrame();
//		if(frame == null) {
//			frame = (ITableDataFrame) in.getDataMaker();
//		}
//		if (frame == null) {
//			return;
//		}
//		OwlTemporalEngineMeta meta = frame.getMetaData();
//		try{
//			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
//		} catch(Exception e){
//			return;
//		}
//		
//		// keep the alias to bind to the correct meta
//		Map<String, String> aliasHash = new HashMap<String, String>();
//		// has to be defined after qs is converted to physical
//		List<IQuerySelector> selectors = qs.getSelectors();
//		
//		// loop through QS
//		// figure out which selector column is part of the
//		for (int i = 0; i < selectors.size(); i++) {
//			IQuerySelector selector = selectors.get(i);
//			String alias = selector.getAlias();
//			String name = "";
//			if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
//				// TODO: this is assuming only 1 math inside due to FE limitation
//				name = ((QueryFunctionSelector) selector).getInnerSelector().get(0).getQueryStructName() + "";
//			} else {
//				name = selector.getQueryStructName();
//			}
//			aliasHash.put(alias, name);
//		}
//		
//		PreparedStatement ps = this.engine.bulkInsertPreparedStatement(WIDGET_TRACKING_INSERT_PS);
//		
//		String id = UUID.randomUUID().toString();
//		Object[] insightDetails = getInsightDetailsObject(in);
//		String sessionId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0] + "");
//		String insightId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1] + "");
//		String userId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2] + "");
//		java.sql.Timestamp time = (java.sql.Timestamp) insightDetails[3];
//		
//		for (String panelId : taskOptions.getPanelIds()) {
//			String layout = taskOptions.getLayout(panelId);
//			Map<String, Object> alignmentMap = taskOptions.getAlignmentMap(panelId);
//			for (String uiCompName : alignmentMap.keySet()) {
//				// ui name can be label, value, x, y, etc.
//				List<String> columnsInUICompName = (List<String>) alignmentMap.get(uiCompName);
//				// now we want to generate a map for each input in this uiCompName
//				for (String columnAlias : columnsInUICompName) {
//					String uniqueMetaName = aliasHash.get(columnAlias);
//					List<String[]> dbInfo = meta.getDatabaseInformation(uniqueMetaName);
//					int size = dbInfo.size();
//					for(int i = 0; i < size; i++) {
//						String[] engineQs = dbInfo.get(i);
//						if (engineQs.length != 2) {
//							continue;
//						}
//						String engineId = engineQs[0];
//						String engineName = MasterDatabaseUtility.getEngineAliasForId(engineId);
//						String table = engineQs[1];
//						String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;
//						if(table.contains("__")) {
//							String[] split = table.split("__");
//							table = split[0];
//							column = split[1];
//						}
//						String dataType = meta.getHeaderTypeAsString(uniqueMetaName);
//						long uniqueCount = getUniqueValueCount(engineId, table, column);
//						
//						// add to ps
//						// id
//						try {
//							ps.setString(1, id);
//							// engine id
//							ps.setString(2, engineId);
//							// engine name
//							ps.setString(3, engineName);
//							// table name
//							ps.setString(4, table);
//							// column name
//							ps.setString(5, column);
//							// datatype
//							ps.setString(6, dataType);
//							// count
//							ps.setLong(7, uniqueCount);
//							// input type
//							ps.setString(8, "VISUALIZATION");
//							// input subtype
//							ps.setString(9, layout);
//							// input name
//							ps.setString(10, uiCompName);
//							// input value
//							ps.setString(11, columnAlias);
//							// session id
//							ps.setString(12, sessionId);
//							// insight id
//							ps.setString(13, insightId);
//							// user id
//							ps.setString(14, userId);
//							// time
//							ps.setTimestamp(15, time);
//							// add batch
//							ps.addBatch();
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//					}
//				}
//			}
//		}
//		
//		try {
//			// execute
//			ps.executeBatch();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	@Override
//	public void trackDataImport(Insight in, SelectQueryStruct qs) {
//		String id = UUID.randomUUID().toString();
//		Object[] insightDetails = getInsightDetailsObject(in);
//		String sessionId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0] + "");
//		String insightId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1] + "");
//		String userId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2] + "");
//		java.sql.Timestamp time = (java.sql.Timestamp) insightDetails[3];
//		
//		// we are currently only tracking the selector information
//		List<IQuerySelector> selectors = qs.getSelectors();
//		
//		PreparedStatement ps = this.engine.bulkInsertPreparedStatement(WIDGET_TRACKING_INSERT_PS);
//		
//		QUERY_STRUCT_TYPE type = qs.getQsType();
//		if(type == QUERY_STRUCT_TYPE.ENGINE) {
//			IEngine engine = qs.retrieveQueryStructEngine();
//			String engineId = engine.getEngineId();
//			String engineName = engine.getEngineName();
//			
//			for(IQuerySelector selector : selectors) {
//				String name = selector.getQueryStructName();
//				String dataType = selector.getDataType();
//				String table = name;
//				String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;
//
//				if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
//					// TODO: this is assuming only 1 math inside due to FE limitation
//					name = ((QueryFunctionSelector) selector).getInnerSelector().get(0).getQueryStructName() + "";
//				}
//				
//				if(name.contains("__")) {
//					String[] split = name.split("__");
//					table = split[0];
//					column = split[1];
//				}
//				
//				if(dataType == null) {
//					if(!column.equals(AbstractQuerySelector.PRIM_KEY_PLACEHOLDER)) {
//						dataType = MasterDatabaseUtility.getBasicDataType(engineId, column, table);
//					} else {
//						dataType = MasterDatabaseUtility.getBasicDataType(engineId, table, null);
//					}
//				}
//				
//				long uniqueCount = getUniqueValueCount(engineId, table, column);
//				
//				// add to ps
//				try {
//					ps.setString(1, id);
//					// engine id
//					ps.setString(2, engineId);
//					// engine name
//					ps.setString(3, engineName);
//					// table name
//					ps.setString(4, table);
//					// column name
//					ps.setString(5, column);
//					// datatype
//					ps.setString(6, dataType);
//					// count
//					ps.setLong(7, uniqueCount);
//					// input type
//					ps.setString(8, "DATA_IMPORT");
//					// input subtype
//					ps.setString(9, "ENGINE");
//					// input name
//					ps.setString(10, "SELECTOR");
//					// input value
//					ps.setString(11, selector.getQueryStructName());
//					// session id
//					ps.setString(12, sessionId);
//					// insight id
//					ps.setString(13, insightId);
//					// user id
//					ps.setString(14, userId);
//					// time
//					ps.setTimestamp(15, time);
//					// add batch
//					ps.addBatch();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		} else if(type == QUERY_STRUCT_TYPE.FRAME) {
//			
//		} else if(type == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY){
//			
//		} else if(type == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY){
//			
//		} else if(type == QUERY_STRUCT_TYPE.CSV_FILE) {
//			
//		} else if(type == QUERY_STRUCT_TYPE.EXCEL_FILE) {
//			
//		} else if(type == QUERY_STRUCT_TYPE.LAMBDA) {
//			
//		}
//		
//		
//		try {
//			// execute
//			ps.executeBatch();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	@Override
//	public void trackAnalyticsWidget(Insight in, ITableDataFrame frame, String routineName, Map<String, List<String>> keyValue) {
//		PreparedStatement ps = this.engine.bulkInsertPreparedStatement(WIDGET_TRACKING_INSERT_PS);
//		
//		OwlTemporalEngineMeta meta = null;
//		if(frame != null) {
//			meta = frame.getMetaData();
//		}
//		
//		String id = UUID.randomUUID().toString();
//		Object[] insightDetails = getInsightDetailsObject(in);
//		String sessionId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0] + "");
//		String insightId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1] + "");
//		String userId = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2] + "");
//		java.sql.Timestamp time = (java.sql.Timestamp) insightDetails[3];
//		
//		if(keyValue == null || keyValue.isEmpty()) {
//			// routine has no inputs
//			try {
//				ps.setString(1, id);
//				// engine id
//				ps.setNull(2, java.sql.Types.VARCHAR);
//				// engine name
//				ps.setNull(3, java.sql.Types.VARCHAR);
//				// table name
//				ps.setNull(4, java.sql.Types.VARCHAR);
//				// column name
//				ps.setNull(5, java.sql.Types.VARCHAR);
//				// datatype
//				ps.setNull(6, java.sql.Types.VARCHAR);
//				// count
//				ps.setNull(7, java.sql.Types.VARCHAR);
//				// input type
//				ps.setString(8, "ANALYTICS");
//				// input subtype
//				ps.setString(9, routineName);
//				// input name
//				ps.setNull(10, java.sql.Types.VARCHAR);
//				// input value
//				ps.setNull(11, java.sql.Types.VARCHAR);
//				// session id
//				ps.setString(12, sessionId);
//				// insight id
//				ps.setString(13, insightId);
//				// user id
//				ps.setString(14, userId);
//				// time
//				ps.setTimestamp(15, time);
//				// add batch
//				ps.addBatch();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		} else {
//			// we have some inputs!
//			for(String inputName : keyValue.keySet()) {
//				List<String> inputValues = keyValue.get(inputName);
//				for(String inputValue : inputValues) {
//					
//					// see if we can go from the base inputs 
//					// if it is a column
//					List<String[]> dbInfo = null;
//					String uniqueMetaName = null;
//					if(meta != null) {
//						uniqueMetaName = meta.getUniqueNameFromAlias(inputValue);
//						if(uniqueMetaName != null) {
//							dbInfo = meta.getDatabaseInformation(uniqueMetaName);
//						}
//					}
//					
//					if(dbInfo == null) {
//						// not a column
//						// add to ps
//						// id
//						try {
//							ps.setString(1, id);
//							// engine id
//							ps.setNull(2, java.sql.Types.VARCHAR);
//							// engine name
//							ps.setNull(3, java.sql.Types.VARCHAR);
//							// table name
//							ps.setNull(4, java.sql.Types.VARCHAR);
//							// column name
//							ps.setNull(5, java.sql.Types.VARCHAR);
//							// datatype
//							ps.setNull(6, java.sql.Types.VARCHAR);
//							// count
//							ps.setNull(7, java.sql.Types.VARCHAR);
//							// input type
//							ps.setString(8, "ANALYTICS");
//							// input subtype
//							ps.setString(9, routineName);
//							// input name
//							ps.setString(10, inputName);
//							// input value
//							ps.setString(11, inputValue);
//							// session id
//							ps.setString(12, sessionId);
//							// insight id
//							ps.setString(13, insightId);
//							// user id
//							ps.setString(14, userId);
//							// time
//							ps.setTimestamp(15, time);
//							// add batch
//							ps.addBatch();
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//					} else {
//						// we have a column
//						int size = dbInfo.size();
//						for(int i = 0; i < size; i++) {
//							String[] engineQs = dbInfo.get(i);
//							if (engineQs.length != 2) {
//								continue;
//							}
//							String engineId = engineQs[0];
//							String engineName = MasterDatabaseUtility.getEngineAliasForId(engineId);
//							String table = engineQs[1];
//							String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;
//							if(table.contains("__")) {
//								String[] split = table.split("__");
//								table = split[0];
//								column = split[1];
//							}
//							String dataType = meta.getHeaderTypeAsString(uniqueMetaName);
//							long uniqueCount = getUniqueValueCount(engineId, table, column);
//							
//							// add to ps
//							// id
//							try {
//								ps.setString(1, id);
//								// engine id
//								ps.setString(2, engineId);
//								// engine name
//								ps.setString(3, engineName);
//								// table name
//								ps.setString(4, table);
//								// column name
//								ps.setString(5, column);
//								// datatype
//								ps.setString(6, dataType);
//								// count
//								ps.setLong(7, uniqueCount);
//								// input type
//								ps.setString(8, "ANALYTICS");
//								// input subtype
//								ps.setString(9, routineName);
//								// input name
//								ps.setString(10, inputName);
//								// input value
//								ps.setString(11, inputValue);
//								// session id
//								ps.setString(12, sessionId);
//								// insight id
//								ps.setString(13, insightId);
//								// user id
//								ps.setString(14, userId);
//								// time
//								ps.setTimestamp(15, time);
//								// add batch
//								ps.addBatch();
//							} catch (SQLException e) {
//								e.printStackTrace();
//							}
//						}
//					}
//				}
//			}
//		}
//		
//		try {
//			// execute
//			ps.executeBatch();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	@Override
//	public void trackInsightExecution(Insight in) {
//		String[] insightDetails = getInsightDetailsString(in);
//		Object[] row = new Object[7];
//		row[0] = RdbmsQueryBuilder.escapeForSQLStatement(in.getEngineId());
//		row[1] = RdbmsQueryBuilder.escapeForSQLStatement(in.getEngineName());
//		row[2] = RdbmsQueryBuilder.escapeForSQLStatement(in.getRdbmsId());
//		row[3] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0]);
//		row[4] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1]);
//		row[5] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2]);
//		row[6] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[3]);
//		String insert = RdbmsQueryBuilder.makeInsert(INSIGHT_EXECUTION_TABLE_NAME, INSIGHT_EXECUTION_HEADERS, INSIGHT_EXECUTION_TYPES, row);
//		this.engine.insertData(insert);
//	}
//
//	@Override
//	public void trackPixelExecution(Insight in, String pixel, boolean meta) {
//		String[] insightDetails = getInsightDetailsString(in);
//		Object[] row = new Object[6];
//		row[0] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[0]);
//		row[1] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[1]);
//		row[2] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[2]);
//		row[3] = RdbmsQueryBuilder.escapeForSQLStatement(insightDetails[3]);
//		row[4] = RdbmsQueryBuilder.escapeForSQLStatement(pixel);
//		row[5] = meta;
//		String insert = RdbmsQueryBuilder.makeInsert(PIXEL_EXECUTION_TABLE_NAME, PIXEL_EXECUTION_HEADERS, PIXEL_EXECUTION_TYPES, row);
//		this.engine.insertData(insert);
//	}
//	
//	/**
//	 * Return a string[] with
//	 * session id
//	 * insight id
//	 * user id
//	 * timestamp
//	 * @param in
//	 * @return
//	 */
//	private Object[] getInsightDetailsObject(Insight in) {
//		Object[] row = new Object[4];
//		VarStore vStore = in.getVarStore();
//		row[0] = vStore.get(JobReactor.SESSION_KEY).getValue().toString();
//		row[1] = vStore.get(JobReactor.INSIGHT_KEY).getValue().toString();
//		User user = in.getUser();
//		if(user != null) {
//			String userId = user.getAccessToken(user.getLogins().get(0)).getId();
//			row[2] = userId;
//		}
//		row[3] = java.sql.Timestamp.valueOf(LocalDateTime.now());
//		return row;
//	}
	
	
}
