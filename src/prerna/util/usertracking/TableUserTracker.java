package prerna.util.usertracking;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.query.querystruct.AbstractFileQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.AbstractQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class TableUserTracker implements IUserTracker {

	private static final Logger classLogger = LogManager.getLogger(TableUserTracker.class);

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
		Map<String, String> aliasHash = new HashMap<>();
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
		
		List<Object[]> rows = new Vector<>();
		
		String id = UUID.randomUUID().toString();
		String[] insightDetails = getInsightDetailsString(in);
		String sessionId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[0] + "");
		String insightId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[1] + "");
		String userId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[2] + "");
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
							String engineName = MasterDatabaseUtility.getDatabaseAliasForId(engineId);
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
		List<Object[]> rows = new Vector<>();
		
		OwlTemporalEngineMeta meta = null;
		if(frame != null) {
			meta = frame.getMetaData();
		}
		
		String id = UUID.randomUUID().toString();
		String[] insightDetails = getInsightDetailsString(in);
		String sessionId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[0] + "");
		String insightId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[1] + "");
		String userId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[2] + "");
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
							String engineName = MasterDatabaseUtility.getDatabaseAliasForId(engineId);
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

							if (meta == null) {
								throw new NullPointerException("Frame metadata is null");
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
		List<Object[]> rows = new Vector<>();
		
		String id = UUID.randomUUID().toString();
		String[] insightDetails = getInsightDetailsString(in);
		String sessionId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[0] + "");
		String insightId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[1] + "");
		String userId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[2] + "");
		String time = insightDetails[3];
		
		// we are currently only tracking the selector information
		List<IQuerySelector> selectors = qs.getSelectors();
		
		QUERY_STRUCT_TYPE type = qs.getQsType();
		if(type == QUERY_STRUCT_TYPE.ENGINE) {
			IDatabaseEngine engine = qs.retrieveQueryStructEngine();
			String engineId = engine.getEngineId();
			String engineName = engine.getEngineName();
			
			for(IQuerySelector selector : selectors) {
				String name = selector.getQueryStructName();
				String dataType = selector.getDataType();
				String table = name;
				String column = AbstractQuerySelector.PRIM_KEY_PLACEHOLDER;

				if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
					// TODO: this is assuming only 1 math inside due to FE limitation
					name = ((QueryFunctionSelector) selector).getInnerSelector().get(0).getQueryStructName() + "";
					table = name;
				}
				
				if(name.contains("__")) {
					String[] split = name.split("__");
					if(split.length == 2) {
						table = split[0];
						column = split[1];
					}
				}
				
				if(dataType == null) {
					if(!column.equals(AbstractQuerySelector.PRIM_KEY_PLACEHOLDER)) {
						dataType = MasterDatabaseUtility.getBasicDataType(engineId, column, table);
					} else {
						dataType = MasterDatabaseUtility.getBasicDataType(engineId, table, null);
					}
				}
				
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
			Map<String, String> aliasHash = new HashMap<>();
			
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
				List<String[]> dbInfo = new Vector<>();
				if(selector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.CONSTANT) {
					dbInfo = meta.getDatabaseInformation(uniqueMetaName);
				}
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
						String engineName = MasterDatabaseUtility.getDatabaseAliasForId(engineId);
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
			IDatabaseEngine engine = qs.retrieveQueryStructEngine();
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
		List<Object[]> rows = new Vector<>();
		String[] insightDetails = getInsightDetailsString(in);
		Object[] row = new Object[7];
		row[0] = AbstractSqlQueryUtil.escapeForSQLStatement(in.getProjectId());
		row[1] = AbstractSqlQueryUtil.escapeForSQLStatement(in.getProjectName());
		row[2] = AbstractSqlQueryUtil.escapeForSQLStatement(in.getRdbmsId());
		row[3] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[0]);
		row[4] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[1]);
		row[5] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[2]);
		row[6] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[3]);
		rows.add(row);
		sendTrackRequest("insight", rows);
	}
	
	@Override
	public void trackPixelExecution(Insight in, String pixel, boolean meta) {
		List<Object[]> rows = new Vector<>();
		String[] insightDetails = getInsightDetailsString(in);
		Object[] row = new Object[6];
		row[0] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[0]);
		row[1] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[1]);
		row[2] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[2]);
		row[3] = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[3]);
		row[4] = AbstractSqlQueryUtil.escapeForSQLStatement(pixel);
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
		row[0] = ThreadStore.getSessionId();
		row[1] = in.getInsightId();
		User user = in.getUser();
		if(user != null) {
			if(!user.getLogins().isEmpty()) {
				String userId = user.getAccessToken(user.getLogins().get(0)).getId();
				row[2] = userId;
			} else if(user.isAnonymous()) {
				row[2] = user.getAnonymousId();
			}
		}
		row[3] = java.sql.Timestamp.valueOf(LocalDateTime.now()).toString();
		return row;
	}
	
	private Long getUniqueValueCount(String engineId, String table, String column) {
		if (StringUtils.countMatches(engineId, "-") != 4) {
			// Not really an engineID (drag and drop...)
			return null;
		}
		IDatabaseEngine engine = Utility.getDatabase(engineId);
		if(engine != null) {
			// are we dealing with a concept or a property
			String physicalUri = engine.getPhysicalUriFromPixelSelector(table + "__" + column);
			if(physicalUri != null) {
				try {
					String uniqueValQuery = "SELECT DISTINCT ?concept ?unique WHERE "
							+ "{ BIND(<" + physicalUri + "> AS ?concept)"
							+ "{?concept <http://semoss.org/ontologies/Relation/Contains/UNIQUE> ?unique}}";
					IRawSelectWrapper it = engine.getOWLEngineFactory().getReadOWL().query(uniqueValQuery);
					while (it.hasNext()) {
						Object[] row = it.next().getValues();
						return Long.parseLong(row[1].toString());
					}
				} catch(Exception e) {
					// will just print the error message but return null as the count
					classLogger.error("StackTrace: ", e);
				}
			}
		}
		
		return null;
	}

	@Override
	public void trackError(Insight in, String pixel, String reactorName, String parentReactorName, boolean meta, Exception ex) {
		List<Object[]> rows = new Vector<>();
		
		String id = UUID.randomUUID().toString();
		String[] insightDetails = getInsightDetailsString(in);
		String sessionId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[0] + "");
		String insightId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[1] + "");
		String userId = AbstractSqlQueryUtil.escapeForSQLStatement(insightDetails[2] + "");
		String time = insightDetails[3];
		
		Object[] row = new Object[14];
		row[0] = id;
		// error type
		row[1] = ex.getClass().getName();
		// error message
		row[2] = ex.getMessage();
		// stack track
		row[3] = ExceptionUtils.getStackTrace(ex);
		// activity type
		row[4] = reactorName;
		// subtype
		row[5] = parentReactorName;
		// trigger
		row[6] = null;
		// saved insight
		row[7] = in.getRdbmsId() != null;
		// pixel
		row[8] = pixel;
		// session id
		row[9] = sessionId;
		// insight id
		row[10] = insightId;
		// user id
		row[11] = userId;
		// time
		row[12] = time;
		// meta
		row[13] = meta;

		// add to list
		rows.add(row);
		sendTrackRequest("error", rows);
	}
	
	@Override
	public boolean isActive() {
		return true;
	}
	
}
