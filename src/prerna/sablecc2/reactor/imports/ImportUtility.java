package prerna.sablecc2.reactor.imports;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.util.CsvFileIterator;
import prerna.ds.util.ExcelFileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AlgorithmQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.QueryStruct2.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class ImportUtility {

	private ImportUtility() {
		
	}
	
	/**
	 * Generates the Iterator from the QS
	 * @param qs
	 * @return
	 */
	public static Iterator<IHeadersDataRow> generateIterator(QueryStruct2 qs, ITableDataFrame frame) {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// engine w/ qs
		if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.ENGINE) {
			IEngine engine = Utility.getEngine(qs.getEngineName());
			return WrapperManager.getInstance().getRawWrapper(engine, qs);
		} 
		// engine with hard coded query
		else if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			IEngine engine = Utility.getEngine(qs.getEngineName());
			return WrapperManager.getInstance().getRawWrapper(engine, ((HardQueryStruct) qs).getQuery());
		} 
		// frame with qs
		else if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.FRAME){
			return frame.query(qs);
		} 
		// frame with hard coded query
		else if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY){
			return frame.query( ((HardQueryStruct) qs).getQuery());
		}
		// csv file (really, any delimited file
		else if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.CSV_FILE) {
			CsvQueryStruct csvQs = (CsvQueryStruct) qs;
			CsvFileIterator it = new CsvFileIterator(csvQs);
			return it;
		} 
		// excel file
		else if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			ExcelQueryStruct xlQS = (ExcelQueryStruct) qs;
			ExcelFileIterator it = new ExcelFileIterator(xlQS);
			return it;
		} 
		// throw error saying i have no idea what to do with you
		else {
			throw new IllegalArgumentException("Cannot currently import from this type = " + qs.getQsType() + " in ImportUtility yet...");
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Generating the correct metadata for flat table frames
	 * This is used for both H2 (any rdbms really) and R
	 */
	
	public static void parseQueryStructToFlatTable(ITableDataFrame dataframe, QueryStruct2 qs, String frameTableName, Iterator<IHeadersDataRow> it) {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// engine
		if(qsType == QUERY_STRUCT_TYPE.ENGINE) {
			parseEngineQsToFlatTable(dataframe, qs, frameTableName);
		}
		// engine with raw query
		else if(qsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			parseRawQsToFlatTable(dataframe, qs, frameTableName, (IRawSelectWrapper) it, qs.getEngineName());
		}
		// frame
		else if(qsType == QUERY_STRUCT_TYPE.FRAME) {
			parseFrameQsToFlatTable(dataframe, qs, frameTableName);
		} 
		// frame with raw query
		else if(qsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			//TODO: make all the frame iterators be IRawSelectWrappers!!!
			//TODO: make all the frame iterators be IRawSelectWrappers!!!
			//TODO: make all the frame iterators be IRawSelectWrappers!!!
			parseRawQsToFlatTable(dataframe, qs, frameTableName, (IRawSelectWrapper) it, "RAW_FRAME_QUERY");
		}
		// csv file
		else if(qsType == QUERY_STRUCT_TYPE.CSV_FILE) {
			parseCsvFileQsToFlatTable(dataframe, (CsvQueryStruct) qs, frameTableName);
		} 
		// excel file
		else if(qsType == QUERY_STRUCT_TYPE.EXCEL_FILE) {
			parseExcelFileQsToFlatTable(dataframe, (ExcelQueryStruct) qs, frameTableName);
		} 
		// from algorithm routine
		else if(qsType == QUERY_STRUCT_TYPE.ALGORITHM) {
			parseAlgorithmQsToFlatTable(dataframe, (AlgorithmQueryStruct) qs, frameTableName);
		}
		// throw error
		else {
			throw new IllegalArgumentException("Haven't implemented this in ImportUtility yet...");
		}
	}
	
	/**
	 * Set the metadata information in the frame based on the QS information as a flat table
	 * @param dataframe
	 * @param qs
	 * @param frameTableName
	 */
	private static void parseEngineQsToFlatTable(ITableDataFrame dataframe, QueryStruct2 qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineName();
		
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			boolean isDerived = selector.isDerived();
			String dataType = selector.getDataType();
			
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, engineName, qsName);
			metaData.setDerivedToProperty(uniqueHeader, isDerived);
			metaData.setAliasToProperty(uniqueHeader, alias);

			if(dataType == null) {
				// this only happens when we have a column selector
				// and we need to query the local master
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					String type = MasterDatabaseUtility.getBasicDataType(engineName, table, null);
					metaData.setDataTypeToProperty(uniqueHeader, type);
				} else {
					String type = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
					metaData.setDataTypeToProperty(uniqueHeader, type);
				}
			} else {
				metaData.setDataTypeToProperty(uniqueHeader, dataType);
			}
		}
	}
	
	private static void parseRawQsToFlatTable(ITableDataFrame dataframe, QueryStruct2 qs, String frameTableName, IRawSelectWrapper it, String source) {
		String[] types = it.getTypes();
		String[] columns = it.getDisplayVariables();
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = columns.length;
		for(int i = 0; i < numSelectors; i++) {
			String alias = columns[i];
			String dataType = Utility.getCleanDataType(types[i]);
			
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, source, "CUSTOM_ENGINE_QUERY");
			metaData.setDerivedToProperty(uniqueHeader, true);
			metaData.setAliasToProperty(uniqueHeader, alias);
			metaData.setDataTypeToProperty(uniqueHeader, dataType);
		}
	}
	
	/**
	 * Set the metadata information in the frame based on the QS information as a flat table
	 * @param dataframe
	 * @param qs
	 * @param frameTableName
	 */
	private static void parseFrameQsToFlatTable(ITableDataFrame dataframe, QueryStruct2 qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String source = "FRAME_QUERY";
		
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			String dataType = selector.getDataType();
			
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, source, qsName);
			metaData.setAliasToProperty(uniqueHeader, alias);
			metaData.setDataTypeToProperty(uniqueHeader, dataType);
			
			// all values from a frame are considered derived
			metaData.setDerivedToProperty(uniqueHeader, true);
		}
	}
	
	private static void parseCsvFileQsToFlatTable(ITableDataFrame dataframe, CsvQueryStruct qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String csvFileName = FilenameUtils.getName(qs.getCsvFilePath());
		Map<String, String> dataTypes = qs.getColumnTypes();

		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			boolean isDerived = selector.isDerived();
			String dataType = selector.getDataType();
			
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, csvFileName, qsName);
			metaData.setDerivedToProperty(uniqueHeader, isDerived);
			metaData.setAliasToProperty(uniqueHeader, alias);

			if(dataType == null) {
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				// this only happens when we have a column selector
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					metaData.setQueryStructNameToProperty(uniqueHeader, csvFileName, table);
					String type = dataTypes.get(table);
					metaData.setDataTypeToProperty(uniqueHeader, type);
				} else {
					metaData.setQueryStructNameToProperty(uniqueHeader, csvFileName, table + "__" + column);
					String type = dataTypes.get(column);
					metaData.setDataTypeToProperty(uniqueHeader, type);
				}
			} else {
				metaData.setDataTypeToProperty(uniqueHeader, dataType);
			}
		}
	}
	
	private static void parseAlgorithmQsToFlatTable(ITableDataFrame dataframe, AlgorithmQueryStruct qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String source = "ALGORITHM";
		Map<String, String> dataTypes = qs.getColumnTypes();

		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			boolean isDerived = selector.isDerived();
			
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, source, qsName);
			metaData.setDerivedToProperty(uniqueHeader, isDerived);
			metaData.setAliasToProperty(uniqueHeader, alias);
			String type = dataTypes.get(selector.getQueryStructName());
			metaData.setDataTypeToProperty(uniqueHeader, type);
		}
	}
	
	private static void parseExcelFileQsToFlatTable(ITableDataFrame dataframe, ExcelQueryStruct qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String excelFileName = FilenameUtils.getName(qs.getExcelFilePath());
		Map<String, String> dataTypes = qs.getColumnTypes();

		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			boolean isDerived = selector.isDerived();
			String dataType = selector.getDataType();
			
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, excelFileName, qsName);
			metaData.setDerivedToProperty(uniqueHeader, isDerived);
			metaData.setAliasToProperty(uniqueHeader, alias);

			if(dataType == null) {
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				// this only happens when we have a column selector
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					metaData.setQueryStructNameToProperty(uniqueHeader, excelFileName, table);
					String type = dataTypes.get(table);
					metaData.setDataTypeToProperty(uniqueHeader, type);
				} else {
					metaData.setQueryStructNameToProperty(uniqueHeader, excelFileName, table + "__" + column);
					String type = dataTypes.get(column);
					metaData.setDataTypeToProperty(uniqueHeader, type);
				}
			} else {
				metaData.setDataTypeToProperty(uniqueHeader, dataType);
			}
		}
	}
	
	/**
	 * When we are generating information programmatically without a standard data source
	 * We can create a new meta using the column names, types, and the table name
	 * 
	 * Example use: Pivot/Unpivot in R which drastically modifies the table
	 * 
	 * @param dataframe
	 * @param columns
	 * @param types
	 * @param frameTableName
	 */
	public static void parseColumnsAndTypesToFlatTable(ITableDataFrame dataframe, String[] columns, String[] types, String frameTableName) {
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);
		
		int numCols = columns.length;
		for(int i = 0; i < numCols; i++) {
			String columnName = columns[i];
			String type = types[i];
			
			String uniqueHeader = frameTableName + "__" + columnName;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, columnName);
			metaData.setDataTypeToProperty(uniqueHeader, type);
		}
	}
	
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Generating the correct metadata for graph frames
	 * This is used for TinkerPop frame
	 */

	
	
	/**
	 * Set the metadata information in the frame based on the QS information as a full graph
	 * @param dataframe
	 * @param qs
	 * @param edgeHash 
	 * @param frameTableName
	 */
	public static void parseQueryStructAsGraph(ITableDataFrame dataframe, QueryStruct2 qs, Map<String, Set<String>> edgeHash) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineName();
		
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();

		Map<String, String> aliasMap = new HashMap<String, String>();
		
		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			boolean isDerived = selector.isDerived();
			String dataType = selector.getDataType();
			
			metaData.addVertex(alias);
			metaData.setQueryStructNameToVertex(alias, engineName, qsName);
			metaData.setDerivedToVertex(alias, isDerived);
			metaData.setAliasToVertex(alias, alias);

			// this is so we can properly match for relationships
			aliasMap.put(qsName, alias);
			
			if(dataType == null) {
				// this only happens when we have a column selector
				// and we need to query the local master
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					String type = MasterDatabaseUtility.getBasicDataType(engineName, table, null);
					metaData.setDataTypeToVertex(alias, type);
				} else {
					String type = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
					metaData.setDataTypeToVertex(alias, type);
				}
			} else {
				metaData.setDataTypeToVertex(alias, dataType);
			}
		}
		
		// now add the relationships
		Map<String, Map<String, List>> relationships = qs.getRelations();
		for(String upVertex : relationships.keySet()) {
			Map<String, List> joinTypeMap = relationships.get(upVertex);
			for(String joinType : joinTypeMap.keySet()) {
				List<String> downstreamVertices = joinTypeMap.get(joinType);
				for(String downVertex : downstreamVertices) {
					String up = upVertex;
					if(aliasMap.containsKey(upVertex)) {
						up = aliasMap.get(upVertex);
					}
					String down = downVertex;
					if(aliasMap.containsKey(downVertex)) {
						down = aliasMap.get(downVertex);
					}
					metaData.addRelationship(up, down, joinType);
				}
			}
		}
		
		// also need to account for concept -> properties
		// we already have this in the edge hash
		// which has everything based on aliases
		for(String upVertex : edgeHash.keySet()) {
			Set<String> downstreamVertices = edgeHash.get(upVertex);
			for(String downVertex : downstreamVertices) {
				String up = upVertex;
				if(aliasMap.containsKey(upVertex)) {
					up = aliasMap.get(upVertex);
				}
				String down = downVertex;
				if(aliasMap.containsKey(downVertex)) {
					down = aliasMap.get(downVertex);
				}
				// just set the join type to be inner
				metaData.addRelationship(up, down, "inner.join");
			}
		}
	}
	
	/**
	 * Get the edge hash corresponding to the relationships defined within the qs
	 * @param qs
	 * @return
	 */
	public static Map<String, Set<String>> getEdgeHash(QueryStruct2 qs) {
		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		
		// go through all the selectors
		// these are what will be returned
		// and what we need to figure out how to connect
		List<IQuerySelector> selectors = qs.getSelectors();
		int numSelectors = selectors.size();
		
		Map<String, String> aliasMapping = flushOutAliases(selectors);
		
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			//TODO: doing base case first cause Davy said so
			//TODO: doing base case first cause Davy said so
			//TODO: doing base case first cause Davy said so
			//TODO: doing base case first cause Davy said so
			if(selector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
				continue;
			}
			QueryColumnSelector cSelector = (QueryColumnSelector) selector;
			String table = cSelector.getTable();
			String column = cSelector.getColumn();
			
			if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
				if(!edgeHash.containsKey(table)) {
					// we already know the alias, easy to do
					edgeHash.put(aliasMapping.get(table), new HashSet<String>());
				}			
			} else {
				Set<String> downConnections = null;
				String parentAlias = aliasMapping.get(table);
				if(parentAlias == null) {
					// we have a property
					// but we are not loading its parent
					// set the property as if it is a parent with no children
					edgeHash.put(aliasMapping.get(table + "__" + column), new HashSet<String>());
				} else {
					if(edgeHash.containsKey(parentAlias)) {
						downConnections = edgeHash.get(parentAlias);
					} else {
						downConnections = new HashSet<String>();
					}
					downConnections.add(aliasMapping.get(table + "__" + column));
					edgeHash.put(parentAlias, downConnections);
				}
			}
		}
		
		// add all relationships
		Map<String, Map<String, List>> relationships = qs.getRelations();
		for(String upVertex : relationships.keySet()) {
			String upVertexAlias = aliasMapping.get(upVertex);
			// store the list of downstream nodes for this "upVertex"
			Set<String> downConnections = null;
			if(edgeHash.containsKey(upVertexAlias)) {
				downConnections = edgeHash.get(upVertexAlias);
			} else {
				downConnections = new HashSet<String>();
				// add into the edgeHash
				edgeHash.put(upVertexAlias, downConnections);
			}
			Map<String, List> joinTypeMap = relationships.get(upVertex);
			for(String joinType : joinTypeMap.keySet()) {
				List<String> downstreamVertices = joinTypeMap.get(joinType);
				for(String downVertex : downstreamVertices) {
					String downVertexAlias = aliasMapping.get(downVertex);
					downConnections.add(downVertexAlias);
				}
			}
		}
		
		// all properties are related to their parent
		
		return edgeHash;
	}
	
	private static Map<String, String> flushOutAliases(List<IQuerySelector> selectors) {
		Map<String, String> aliasMapping = new HashMap<String, String>();
		for(IQuerySelector selector : selectors) {
			String qsName = selector.getQueryStructName();
			String alias = selector.getAlias();
			aliasMapping.put(qsName, alias);
		}
		return aliasMapping;
	}
	
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Set the metadata information in the frame based on the QS information as a full graph
	 * @param dataframe
	 * @param qs
	 * @param frameTableName
	 */
	public static void parseQueryStructIntoMeta(ITableDataFrame dataframe, QueryStruct2 qs) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineName();
		
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		Set<String> addedQsNames = new HashSet<String>();
		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			addedQsNames.add(qsName);
			if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					metaData.addVertex(table);
					metaData.setQueryStructNameToVertex(table, engineName, qsName);
					String type = MasterDatabaseUtility.getBasicDataType(engineName, table, null);
					metaData.setDataTypeToVertex(table, type);
					metaData.setAliasToVertex(table, alias);
				} else {
					String uniqueName = qsName;
					metaData.addProperty(table, uniqueName);
					metaData.setQueryStructNameToProperty(uniqueName, engineName, qsName);
					String type = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
					metaData.setDataTypeToProperty(uniqueName, type);
					metaData.setAliasToProperty(uniqueName, alias);
				}
			} else {
				metaData.addVertex(qsName);
				metaData.setQueryStructNameToVertex(qsName, engineName, selector.getQueryStructName());
				metaData.setAliasToVertex(qsName, alias);
				metaData.setDataTypeToVertex(qsName, selector.getDataType());

				List<QueryColumnSelector> cList = selector.getAllQueryColumns();
				for(QueryColumnSelector cSelect : cList) {
					String table = cSelect.getTable();
					String column = cSelect.getColumn();
					String cQsName = cSelect.getQueryStructName();
					String cAlias = cSelect.getAlias();
					addedQsNames.add(cQsName);

					if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
						metaData.addVertex(table);
						metaData.setQueryStructNameToVertex(table, engineName, cQsName);
						String type = MasterDatabaseUtility.getBasicDataType(engineName, table, null);
						metaData.setDataTypeToVertex(table, type);
						metaData.setAliasToVertex(table, cAlias);
						metaData.setPrimKeyToVertex(table, true);
					} else {
						String uniqueName = cQsName;
						metaData.addProperty(table, uniqueName);
						metaData.setQueryStructNameToProperty(uniqueName, engineName, cQsName);
						String type = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
						metaData.setDataTypeToProperty(uniqueName, type);
						metaData.setAliasToProperty(uniqueName, cAlias);
						metaData.setPrimKeyToProperty(uniqueName, true);
					}
				}
			}
		}
		
		// now add the relationships
		Map<String, Map<String, List>> relationships = qs.getRelations();
		for(String upVertex : relationships.keySet()) {
			Map<String, List> joinTypeMap = relationships.get(upVertex);
			for(String joinType : joinTypeMap.keySet()) {
				List<String> downstreamVertices = joinTypeMap.get(joinType);
				for(String downVertex : downstreamVertices) {
					if(!addedQsNames.contains(upVertex)) {
						metaData.addVertex(upVertex);
						metaData.setPrimKeyToVertex(upVertex, true);
						addedQsNames.add(upVertex);
					}
					if(!addedQsNames.contains(downVertex)) {
						metaData.addVertex(downVertex);
						metaData.setPrimKeyToVertex(downVertex, true);
						addedQsNames.add(downVertex);
					}
					metaData.addRelationship(upVertex, downVertex, joinType);
				}
			}
		}
	}
	
	public static void parseHeadersAndTypeIntoMeta(ITableDataFrame dataframe, String[] headers, String[] types, String frameTableName) {
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = headers.length;
		for(int i = 0; i < numSelectors; i++) {
			String header = headers[i];
			String type = types[i];
			
			String uniqueHeader = frameTableName + "__" + header;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, "unknown", header);
			metaData.setDataTypeToProperty(uniqueHeader, type);
			metaData.setAliasToProperty(uniqueHeader, header);
		}
	}
	

	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////

	// get the data types based on the QueryStruct

	public static Map<String, IMetaData.DATA_TYPES> getTypesFromQs(QueryStruct2 qs) {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		if(qsType == QUERY_STRUCT_TYPE.ENGINE) {
			return getMetaDataFromEngineQs(qs);
		} else if(qsType == QUERY_STRUCT_TYPE.FRAME) {
			return getMetaDataFromFrameQs(qs);
		} else if(qsType == QUERY_STRUCT_TYPE.CSV_FILE) {
			return getMetaDataFromCsvQs((CsvQueryStruct)qs);
		} else if (qsType == QUERY_STRUCT_TYPE.EXCEL_FILE) {
			return getMetaDataFromExcelQs((ExcelQueryStruct)qs);
		} else if(qsType == QUERY_STRUCT_TYPE.ALGORITHM) {
			return getMetaDataFromAlgorithmQs((AlgorithmQueryStruct)qs);
		}
		else {
			throw new IllegalArgumentException("Haven't implemented this in ImportUtility yet...");
		}
	}
	
	private static Map<String, DATA_TYPES> getMetaDataFromAlgorithmQs(AlgorithmQueryStruct qs) {
		Map<String, IMetaData.DATA_TYPES> metaData = new HashMap<String, IMetaData.DATA_TYPES>();
		Map<String, String> dataTypes = qs.getColumnTypes();
		for(String hName : dataTypes.keySet()) {
			if(hName.contains("__")) {
				metaData.put(hName.split("__")[1], Utility.convertStringToDataType(dataTypes.get(hName)));
			} else {
				metaData.put(hName, Utility.convertStringToDataType(dataTypes.get(hName)));
			}
		}
		return metaData;
	}

	private static Map<String, IMetaData.DATA_TYPES> getMetaDataFromEngineQs(QueryStruct2 qs) {
		Map<String, IMetaData.DATA_TYPES> metaData = new HashMap<String, IMetaData.DATA_TYPES>();
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineName();
		// loop through all the selectors
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String dataType = selector.getDataType();
			if(dataType == null) {
				// this only happens when we have a column selector
				// and we need to query the local master
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					dataType = MasterDatabaseUtility.getBasicDataType(engineName, table, null);
				} else {
					dataType = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
				}
			} 
			IMetaData.DATA_TYPES dtEnum = IMetaData.convertToDataTypeEnum(dataType);
			metaData.put(alias, dtEnum);
		}
		return metaData;
	}

	private static Map<String, IMetaData.DATA_TYPES> getMetaDataFromFrameQs(QueryStruct2 qs) {
		Map <String, IMetaData.DATA_TYPES> metaData = new HashMap<String, IMetaData.DATA_TYPES>();
		OwlTemporalEngineMeta owlMeta = qs.getFrame().getMetaData();
		List<IQuerySelector> selectors = qs.getSelectors();
		// loop through all the selectors
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String dataType = selector.getDataType();
			if(dataType == null) {
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				// this only happens when we have a column selector
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					dataType = owlMeta.getHeaderTypeAsString(table, null);
				} else {
					dataType = owlMeta.getHeaderTypeAsString(table + "__" + column, table);
				}
			}
			IMetaData.DATA_TYPES dtEnum = Utility.convertStringToDataType(dataType);
			metaData.put(alias, dtEnum);
		}
		return metaData;
	}
	
	private static Map<String, IMetaData.DATA_TYPES> getMetaDataFromCsvQs(CsvQueryStruct qs) {
		Map<String, IMetaData.DATA_TYPES> metaData = new HashMap<String, IMetaData.DATA_TYPES>();
		List<IQuerySelector> selectors = qs.getSelectors();
		Map<String, String> dataTypes = qs.getColumnTypes();
		// loop through all the selectors
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String dataType = selector.getDataType();

			if(dataType == null) {
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				// this only happens when we have a column selector
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					dataType = dataTypes.get(table);
				} else {
					dataType = dataTypes.get(column);
				}
			} 
			IMetaData.DATA_TYPES dtEnum = Utility.convertStringToDataType(dataType);
			metaData.put(alias, dtEnum);
		}
		return metaData;
	}
	
	private static Map<String, IMetaData.DATA_TYPES> getMetaDataFromExcelQs(ExcelQueryStruct qs) {
		Map<String, IMetaData.DATA_TYPES> metaData = new HashMap<String, IMetaData.DATA_TYPES>();
		List<IQuerySelector> selectors = qs.getSelectors();
		Map<String, String> dataTypes = qs.getColumnTypes();

		// loop through all the selectors
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String dataType = selector.getDataType();
			if(dataType == null) {
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				// this only happens when we have a column selector
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					dataType = dataTypes.get(table);
				} else {
					dataType = dataTypes.get(column);
				}
			}
			IMetaData.DATA_TYPES dtEnum = Utility.convertStringToDataType(dataType);
			metaData.put(alias, dtEnum);
		}
		return metaData;
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	public static void parseQueryStructToFlatTableWithJoin(ITableDataFrame dataframe, QueryStruct2 qs, String tableName, Iterator<IHeadersDataRow> it, List<Join> joins) {
		QueryStruct2.QUERY_STRUCT_TYPE qsType = qs.getQsType();
		if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			parseRawQsToFlatTableWithJoin(dataframe, tableName, (IRawSelectWrapper) it, joins, qs.getEngineName());
		} else if(qsType == QueryStruct2.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			parseRawQsToFlatTableWithJoin(dataframe, tableName, (IRawSelectWrapper) it, joins, "RAW_FRAME_QUERY");
		} else {
			parseQsToFlatTableWithJoin(dataframe, qs, tableName, it, joins);
		}
	}

	private static void parseQsToFlatTableWithJoin(ITableDataFrame dataframe, QueryStruct2 qs, String tableName, Iterator<IHeadersDataRow> it, List<Join> joins) {
		// this will get a new QS with the base details copied for the original qs
		// but does not have any selector/relationship/filter options
		QueryStruct2 updatedQsForJoins = qs.getNewBaseQueryStruct();
		
		List<IQuerySelector> newSelectors = qs.getSelectors();
		int numNewSelectors = newSelectors.size();
		// note, this is already cleaned so it is not table__col and just col
		boolean foundColumnJoin = false;
		String[] modifiedNewHeaders = new String[numNewSelectors];
		NEW_SELECTOR_LOOP : for(int i = 0; i < numNewSelectors; i++) {
			IQuerySelector newSelector = newSelectors.get(i);
			String alias = newSelector.getAlias();
			
			String qsName = newSelector.getQueryStructName();
			for(Join j : joins) {
				String existingColName = j.getSelector();
				String newColName = j.getQualifier();
				
				// if this is true, it means that the unique name of this selector is part of a join
				// and we want to replace its name
				if(qsName.equals(newColName)) {
					foundColumnJoin = true;
					if(existingColName.contains("__")) {
						modifiedNewHeaders[i] = existingColName.split("__")[1];
					} else {
						modifiedNewHeaders[i] = existingColName;
					}
					continue NEW_SELECTOR_LOOP;
				}
			}
			
			// if we got to this point
			// we will just set the modifiedNewName as is
			modifiedNewHeaders[i] = alias;
			updatedQsForJoins.addSelector(newSelector);
		}
		
		if(!foundColumnJoin) {
			throw new IllegalArgumentException("Could not find the specified join column to merge with the existing frame");
		}
		
		// once we have the updated QS, just use the normal method to update
		parseQueryStructToFlatTable(dataframe, updatedQsForJoins, tableName, it);
	}

	private static void parseRawQsToFlatTableWithJoin(ITableDataFrame dataframe, String frameTableName, IRawSelectWrapper it, List<Join> joins, String source) {
		String[] types = it.getTypes();
		String[] columns = it.getDisplayVariables();
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		boolean foundColumnJoin = false;

		int numSelectors = columns.length;
		for(int i = 0; i < numSelectors; i++) {
			String alias = columns[i];
			for(Join j : joins) {
				String newColName = j.getQualifier();
				// if this is true, it means that this column is part of a join
				// and we do not want to add it as a header
				if(alias.equals(newColName)) {
					foundColumnJoin = true;
					continue;
				}
			}
			
			String dataType = Utility.getCleanDataType(types[i]);
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, source, "CUSTOM_ENGINE_QUERY");
			metaData.setDerivedToProperty(uniqueHeader, true);
			metaData.setAliasToProperty(uniqueHeader, alias);
			metaData.setDataTypeToProperty(uniqueHeader, dataType);
		}
		
		if(!foundColumnJoin) {
			//TODO: delete stuff that was added!!!
			throw new IllegalArgumentException("Could not find the specified join column to merge with the existing frame");
		}
	}
}
