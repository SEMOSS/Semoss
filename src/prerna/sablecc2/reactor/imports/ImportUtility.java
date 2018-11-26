package prerna.sablecc2.reactor.imports;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.LambdaQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class ImportUtility {

	private ImportUtility() {
		
	}
	
	/**
	 * Generates the Iterator from the QS
	 * @param qs
	 * @return
	 */
	public static IRawSelectWrapper generateIterator(SelectQueryStruct qs, ITableDataFrame frame) {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// engine w/ qs
		if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
			return WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), qs);
		} 
		// engine with hard coded query
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			return WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), ((HardSelectQueryStruct) qs).getQuery());
		}
		// frame with qs
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
			ITableDataFrame qsFrame = qs.getFrame();
			if(qsFrame != null) {
				return qsFrame.query(qs);
			} else {
				return frame.query(qs);
			}
		} 
		// frame with hard coded query
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY){
			ITableDataFrame qsFrame = qs.getFrame();
			if(qsFrame != null) {
				return qsFrame.query( ((HardSelectQueryStruct) qs).getQuery());
			} else {
				return frame.query( ((HardSelectQueryStruct) qs).getQuery());
			}
		}
		// csv file (really, any delimited file
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE) {
			CsvQueryStruct csvQs = (CsvQueryStruct) qs;
			CsvFileIterator it = new CsvFileIterator(csvQs);
			return it;
		} 
		// excel file
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			if(frame instanceof RDataTable) {
				ExcelQueryStruct xlQS = (ExcelQueryStruct) qs;
				return new ExcelSheetFileIterator(xlQS);
			}
			ExcelQueryStruct xlQS = (ExcelQueryStruct) qs;
			return ExcelWorkbookFileHelper.buildSheetIterator(xlQS);
//			ExcelFileIterator it = new ExcelFileIterator(xlQS);
//			return it;
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
	
	public static void parseQueryStructToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName, Iterator<IHeadersDataRow> it) {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// engine
		if(qsType == QUERY_STRUCT_TYPE.ENGINE) {
			parseEngineQsToFlatTable(dataframe, qs, frameTableName);
		}
		// engine with raw query
		else if(qsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			parseRawQsToFlatTable(dataframe, qs, frameTableName, (IRawSelectWrapper) it, qs.getEngineId());
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
		else if(qsType == QUERY_STRUCT_TYPE.LAMBDA) {
			parseLambdaQsToFlatTable(dataframe, (LambdaQueryStruct) qs, frameTableName);
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
	private static void parseEngineQsToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineId();
		
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
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
	
	private static void parseRawQsToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName, IRawSelectWrapper it, String source) {
		SemossDataType[] types = it.getTypes();
		String[] columns = it.getHeaders();
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		int numSelectors = columns.length;
		for(int i = 0; i < numSelectors; i++) {
			String alias = columns[i];
			String dataType = types[i].toString();
			
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
	private static void parseFrameQsToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName) {
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
			
			if(dataType == null) {
				// cannot assume the iterator that created this qs
				// is from the same frame as the frame we are adding this data to
				if(qs.getFrame() != null) {
					dataType = qs.getFrame().getMetaData().getHeaderTypeAsString(selector.getQueryStructName());
				}
				if(dataType == null) {
					// if still null
					// try the current frame
					dataType = metaData.getHeaderTypeAsString(selector.getQueryStructName());
				}
			}
			
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
		String csvFileName = FilenameUtils.getBaseName(qs.getFilePath());
		// remove the ugly stuff we add to make this unique
		if(csvFileName.contains("_____UNIQUE")) {
			csvFileName = csvFileName.substring(0, csvFileName.indexOf("_____UNIQUE"));
		}
		csvFileName = Utility.makeAlphaNumeric(csvFileName);
		Map<String, String> dataTypes = qs.getColumnTypes();
		Map<String, String> additionalTypes = qs.getAdditionalTypes();
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
			metaData.setQueryStructNameToProperty(uniqueHeader, csvFileName, qsName);
			metaData.setDerivedToProperty(uniqueHeader, isDerived);
			metaData.setAliasToProperty(uniqueHeader, alias);

			QueryColumnSelector cSelect = (QueryColumnSelector) selector;
			String table = cSelect.getTable();
			String column = cSelect.getColumn();
			
			// I am just doing a check for bad inputs
			// We want to standardize so it is always DND__CSV_COLUMN_NAME
			// but just in case someone doesn't do DND and just passed in CSV_COLUMN_NAME
			// the column will be defaulted to PRIM_KEY_PLACEHOLDER based on our construct
			if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				metaData.setQueryStructNameToProperty(uniqueHeader, csvFileName, table);
				String type = dataTypes.get(table);
				metaData.setDataTypeToProperty(uniqueHeader, type);
				
				if(additionalTypes.get(table) != null) {
					metaData.setAddtlDataTypeToProperty(uniqueHeader, additionalTypes.get(uniqueHeader));
				}
			} else {
				metaData.setQueryStructNameToProperty(uniqueHeader, csvFileName, table + "__" + column);
				String type = dataTypes.get(column);
				metaData.setDataTypeToProperty(uniqueHeader, type);
				
				if(additionalTypes.get(column) != null) {
					metaData.setAddtlDataTypeToProperty(uniqueHeader, additionalTypes.get(column));
				}
			}
		}
	}
	
	private static void parseLambdaQsToFlatTable(ITableDataFrame dataframe, LambdaQueryStruct qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String source = "LAMBDA";
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
		String excelFileName = FilenameUtils.getBaseName(qs.getFilePath());
		// remove the ugly stuff we add to make this unique
		if(excelFileName.contains("_____UNIQUE")) {
			excelFileName = excelFileName.substring(0, excelFileName.indexOf("_____UNIQUE"));
		}
		excelFileName = Utility.makeAlphaNumeric(excelFileName);
		Map<String, String> dataTypes = qs.getColumnTypes();
		Map<String, String> additionalTypes = qs.getAdditionalTypes();

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
			metaData.setQueryStructNameToProperty(uniqueHeader, excelFileName, qsName);
			metaData.setDerivedToProperty(uniqueHeader, isDerived);
			metaData.setAliasToProperty(uniqueHeader, alias);

			QueryColumnSelector cSelect = (QueryColumnSelector) selector;
			String table = cSelect.getTable();
			String column = cSelect.getColumn();
			// I am just doing a check for bad inputs
			// We want to standardize so it is always DND__EXCEL_COLUMN_NAME
			// but just in case someone doesn't do DND and just passed in EXCEL_COLUMN_NAME
			// the column will be defaulted to PRIM_KEY_PLACEHOLDER based on our construct
			if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				metaData.setQueryStructNameToProperty(uniqueHeader, excelFileName, table);
				String type = dataTypes.get(table);
				metaData.setDataTypeToProperty(uniqueHeader, type);
				
				if(additionalTypes.get(table) != null) {
					metaData.setAddtlDataTypeToProperty(uniqueHeader, additionalTypes.get(uniqueHeader));
				}
			} else {
				metaData.setQueryStructNameToProperty(uniqueHeader, excelFileName, table + "__" + column);
				String type = dataTypes.get(column);
				metaData.setDataTypeToProperty(uniqueHeader, type);
				
				if(additionalTypes.get(column) != null) {
					metaData.setAddtlDataTypeToProperty(uniqueHeader, additionalTypes.get(column));
				}
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
	public static void parserRTableColumnsAndTypesToFlatTable(ITableDataFrame dataframe, String[] columns, String[] types, String frameTableName) {
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);
		metaData.setQueryStructNameToVertex(frameTableName, "UNKNOWN_R", frameTableName);
		int numCols = columns.length;
		for(int i = 0; i < numCols; i++) {
			String columnName = columns[i];
			String type = types[i];
			
			String uniqueHeader = frameTableName + "__" + columnName;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, columnName);
			metaData.setDataTypeToProperty(uniqueHeader, type);
			metaData.setQueryStructNameToProperty(uniqueHeader, "UNKNOWN_R", uniqueHeader);
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
	public static void parsePyTableColumnsAndTypesToFlatTable(ITableDataFrame dataframe, String[] columns, String[] types, String frameTableName) {
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);
		metaData.setQueryStructNameToVertex(frameTableName, "UNKNOWN_PY", frameTableName);
		int numCols = columns.length;
		for(int i = 0; i < numCols; i++) {
			String columnName = columns[i];
			String type = types[i];
			
			String uniqueHeader = frameTableName + "__" + columnName;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, columnName);
			metaData.setDataTypeToProperty(uniqueHeader, type);
			metaData.setQueryStructNameToProperty(uniqueHeader, "UNKNOWN_PY", uniqueHeader);
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
	public static void parseQueryStructAsGraph(ITableDataFrame dataframe, SelectQueryStruct qs, Map<String, Set<String>> edgeHash) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineId();
		
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
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
		
		List<String> addedRels = new Vector<String>();
		// now add the relationships
		Set<String[]> relations = qs.getRelations();
		for(String[] rel : relations) {
			String up = rel[0];
			if(aliasMap.containsKey(up)) {
				up = aliasMap.get(up);
			}
			String joinType = rel[1];
			String down = rel[2];
			if(aliasMap.containsKey(down)) {
				down = aliasMap.get(down);
			}
			metaData.addRelationship(up, down, joinType);
			addedRels.add(up + down);
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
				// make sure we dont add more relationships than necessary
				// especially since we are hardcoding the join here for properties
				if(!addedRels.contains(up + down)) {
					metaData.addRelationship(up, down, "inner.join");
				}
			}
		}
	}
	
	/**
	 * Set the metadata information in the frame based on the QS information as a full graph
	 * @param dataframe
	 * @param qs
	 * @param edgeHash 
	 * @param frameTableName
	 */
	public static void parseFileQueryStructAsGraph(ITableDataFrame dataframe, SelectQueryStruct qs, Map<String, Set<String>> edgeHash) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineId();
		
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
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
		
		// also need to account for the file row id to each header
		for(String upVertex : edgeHash.keySet()) {
			// add this to the meta
			metaData.addVertex(upVertex);
			metaData.setQueryStructNameToVertex(upVertex, engineName, upVertex);
			metaData.setDerivedToVertex(upVertex, true);
			metaData.setAliasToVertex(upVertex, upVertex);
			metaData.setDataTypeToVertex(upVertex, "STRING");

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
				metaData.addRelationship(up, down, "inner.join");
			}
		}
	}
	
	/**
	 * Get the edge hash corresponding to the relationships defined within the qs
	 * @param qs
	 * @return
	 */
	public static Map<String, Set<String>> getEdgeHash(SelectQueryStruct qs) {
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
			
			if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
		Set<String[]> relations = qs.getRelations();
		for(String[] rel : relations) {
			String upVertex = rel[0];
			String downVertex = rel[2];
			
			String upVertexAlias = aliasMapping.get(upVertex);
			String downVertexAlias = aliasMapping.get(downVertex);

			Set<String> downConnections = null;
			if(edgeHash.containsKey(upVertexAlias)) {
				downConnections = edgeHash.get(upVertexAlias);
			} else {
				downConnections = new HashSet<String>();
				edgeHash.put(upVertexAlias, downConnections);
			}
			downConnections.add(downVertexAlias);
		}
//		Map<String, Map<String, List>> relationships = qs.getRelations();
//		for(String upVertex : relationships.keySet()) {
//			String upVertexAlias = aliasMapping.get(upVertex);
//			// store the list of downstream nodes for this "upVertex"
//			Set<String> downConnections = null;
//			if(edgeHash.containsKey(upVertexAlias)) {
//				downConnections = edgeHash.get(upVertexAlias);
//			} else {
//				downConnections = new HashSet<String>();
//				// add into the edgeHash
//				edgeHash.put(upVertexAlias, downConnections);
//			}
//			Map<String, List> joinTypeMap = relationships.get(upVertex);
//			for(String joinType : joinTypeMap.keySet()) {
//				List<String> downstreamVertices = joinTypeMap.get(joinType);
//				for(String downVertex : downstreamVertices) {
//					String downVertexAlias = aliasMapping.get(downVertex);
//					downConnections.add(downVertexAlias);
//				}
//			}
//		}
		
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
	public static void parseNativeQueryStructIntoMeta(ITableDataFrame dataframe, SelectQueryStruct qs) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineId();
		
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
				
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
				// define a vertex with the alias
				// since this is used for native
				// we do not know what the user has done
				// this could be a function
				// it could be math
				// it could be opaque
				// countless possibilities .. (jk, just the above)
				metaData.addVertex(alias);
				metaData.setQueryStructNameToVertex(alias, engineName, qsName);
				metaData.setAliasToVertex(alias, alias);
				metaData.setDataTypeToVertex(alias, selector.getDataType());

				// i will add the selector into the meta
				// and get it back later
				// FE will only use the alias to utilize this selector
				// so i will go ahead and generate the original selector
				metaData.setSelectorComplex(alias, true);
				metaData.setSelectorTypeToVertex(alias, selector.getSelectorType());
				metaData.setSelectorObject(alias, GsonUtility.getDefaultGson().toJson(selector));
			}
		}
		
		// now add the relationships
		Set<String[]> relations = qs.getRelations();
		for(String[] rel : relations) {
			String upVertex = rel[0];
			String joinType = rel[1];
			String downVertex = rel[2];
			
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
		
//		Map<String, Map<String, List>> relationships = qs.getRelations();
//		for(String upVertex : relationships.keySet()) {
//			Map<String, List> joinTypeMap = relationships.get(upVertex);
//			for(String joinType : joinTypeMap.keySet()) {
//				List<String> downstreamVertices = joinTypeMap.get(joinType);
//				for(String downVertex : downstreamVertices) {
//					if(!addedQsNames.contains(upVertex)) {
//						metaData.addVertex(upVertex);
//						metaData.setPrimKeyToVertex(upVertex, true);
//						addedQsNames.add(upVertex);
//					}
//					if(!addedQsNames.contains(downVertex)) {
//						metaData.addVertex(downVertex);
//						metaData.setPrimKeyToVertex(downVertex, true);
//						addedQsNames.add(downVertex);
//					}
//				}
//			}
//		}
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

	public static Map<String, SemossDataType> getTypesFromQs(SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		if(qsType == QUERY_STRUCT_TYPE.ENGINE) {
			return getMetaDataFromEngineQs(qs);
		} else if(qsType == QUERY_STRUCT_TYPE.FRAME) {
			return getMetaDataFromFrameQs(qs);
		} else if(qsType == QUERY_STRUCT_TYPE.CSV_FILE) {
			return getMetaDataFromCsvQs((CsvQueryStruct)qs);
		} else if (qsType == QUERY_STRUCT_TYPE.EXCEL_FILE) {
			return getMetaDataFromExcelQs((ExcelQueryStruct)qs);
		} else if(qsType == QUERY_STRUCT_TYPE.LAMBDA) {
			return getMetaDataFromLambdaQs((LambdaQueryStruct)qs);
		} else if(qsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			return getMetaDataFromQuery((IRawSelectWrapper)it);
		} else if(qsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			return getMetaDataFromQuery((IRawSelectWrapper)it);
		}
		else {
			throw new IllegalArgumentException("Haven't implemented this in ImportUtility yet...");
		}
	}
	
	private static Map<String, SemossDataType> getMetaDataFromQuery(IRawSelectWrapper wrapper) {
		Map<String, SemossDataType> metaData = new HashMap<String, SemossDataType>();

		String[] headers = wrapper.getHeaders();
		SemossDataType[] types = wrapper.getTypes();
		for(int i = 0; i < headers.length; i++) {
			metaData.put(headers[i], types[i]);
		}
		return metaData;
	}
	
	private static Map<String, SemossDataType> getMetaDataFromLambdaQs(LambdaQueryStruct qs) {
		Map<String, SemossDataType> metaData = new HashMap<String, SemossDataType>();
		Map<String, String> dataTypes = qs.getColumnTypes();
		for(String hName : dataTypes.keySet()) {
			if(hName.contains("__")) {
				metaData.put(hName.split("__")[1], SemossDataType.convertStringToDataType(dataTypes.get(hName)));
			} else {
				metaData.put(hName, SemossDataType.convertStringToDataType(dataTypes.get(hName)));
			}
		}
		return metaData;
	}

	private static Map<String, SemossDataType> getMetaDataFromEngineQs(SelectQueryStruct qs) {
		Map<String, SemossDataType> metaData = new HashMap<String, SemossDataType>();
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineName = qs.getEngineId();
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
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					dataType = MasterDatabaseUtility.getBasicDataType(engineName, table, null);
				} else {
					dataType = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
				}
			} 
			SemossDataType dtEnum = SemossDataType.convertStringToDataType(dataType);
			metaData.put(alias, dtEnum);
		}
		return metaData;
	}

	private static Map<String, SemossDataType> getMetaDataFromFrameQs(SelectQueryStruct qs) {
		Map <String, SemossDataType> metaData = new HashMap<String, SemossDataType>();
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
				String qsName = cSelect.getQueryStructName();
				String uniqueName = owlMeta.getUniqueNameFromAlias(qsName);
				if(uniqueName == null) {
					uniqueName = qsName;
				}
				dataType = owlMeta.getHeaderTypeAsString(uniqueName);
			}
			SemossDataType dtEnum = SemossDataType.convertStringToDataType(dataType);
			metaData.put(alias, dtEnum);
		}
		return metaData;
	}
	
	private static Map<String, SemossDataType> getMetaDataFromCsvQs(CsvQueryStruct qs) {
		Map<String, SemossDataType> metaData = new HashMap<String, SemossDataType>();
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
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					dataType = dataTypes.get(table);
				} else {
					dataType = dataTypes.get(column);
				}
			} 
			SemossDataType dtEnum = SemossDataType.convertStringToDataType(dataType);
			metaData.put(alias, dtEnum);
		}
		return metaData;
	}
	
	private static Map<String, SemossDataType> getMetaDataFromExcelQs(ExcelQueryStruct qs) {
		Map<String, SemossDataType> metaData = new HashMap<String, SemossDataType>();
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
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					dataType = dataTypes.get(table);
				} else {
					dataType = dataTypes.get(column);
				}
			}
			SemossDataType dtEnum = SemossDataType.convertStringToDataType(dataType);
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

	public static void parseQueryStructToFlatTableWithJoin(ITableDataFrame dataframe, SelectQueryStruct qs, String tableName, Iterator<IHeadersDataRow> it, List<Join> joins) {
		SelectQueryStruct.QUERY_STRUCT_TYPE qsType = qs.getQsType();
		if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			parseRawQsToFlatTableWithJoin(dataframe, tableName, (IRawSelectWrapper) it, joins, qs.getEngineId());
		} else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			parseRawQsToFlatTableWithJoin(dataframe, tableName, (IRawSelectWrapper) it, joins, "RAW_FRAME_QUERY");
		} else {
			parseQsToFlatTableWithJoin(dataframe, qs, tableName, it, joins);
		}
	}

	private static void parseQsToFlatTableWithJoin(ITableDataFrame dataframe, SelectQueryStruct qs, String tableName, Iterator<IHeadersDataRow> it, List<Join> joins) {
		// this will get a new QS with the base details copied for the original qs
		// but does not have any selector/relationship/filter options
		SelectQueryStruct updatedQsForJoins = qs.getNewBaseQueryStruct();
		
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
				if(qsName.equals(newColName) || alias.equals(newColName)) {
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
		SemossDataType[] types = it.getTypes();
		String[] columns = it.getHeaders();
		// define the frame table name as a primary key within the meta
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);

		// loop through all the selectors
		// insert them into the frame
		boolean foundColumnJoin = false;

		int numSelectors = columns.length;
		SELECTOR : for(int i = 0; i < numSelectors; i++) {
			String alias = columns[i];
			for(Join j : joins) {
				String newColName = j.getQualifier();
				// if this is true, it means that this column is part of a join
				// and we do not want to add it as a header
				if(alias.equals(newColName)) {
					foundColumnJoin = true;
					continue SELECTOR;
				}
			}
			
			String dataType = types[i].toString();
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
