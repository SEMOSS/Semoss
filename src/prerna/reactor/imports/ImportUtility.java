package prerna.reactor.imports;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.ds.util.flatfile.ParquetFileIterator;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.poi.main.HeadersException;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.LambdaQueryStruct;
import prerna.query.querystruct.ParquetQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.RawSesameSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class ImportUtility {

	private static final Logger classLogger = LogManager.getLogger(ImportUtility.class);

	private ImportUtility() {
		
	}
	
	/**
	 * Generates the Iterator from the QS
	 * @param qs
	 * @return
	 */
	public static IRawSelectWrapper generateIterator(SelectQueryStruct qs, ITableDataFrame frame) throws Exception {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// engine w/ qs
		if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.DIRECT_API_QUERY) {
			return WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), qs);
		} 
		// engine with hard coded query
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY 
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY) {
			return WrapperManager.getInstance().getRawWrapper(qs.retrieveQueryStructEngine(), ((HardSelectQueryStruct) qs).getQuery());
		}
		// frame with qs
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
			ITableDataFrame qsFrame = qs.getFrame();
			if(qsFrame != null) {
				qs.mergeImplicitFilters(qsFrame.getFrameFilters());
				return qsFrame.query(qs);
			} else {
				qs.mergeImplicitFilters(frame.getFrameFilters());
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
		// parquet file
		else if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.PARQUET_FILE) {
			ParquetQueryStruct parquetQs = (ParquetQueryStruct) qs;
			ParquetFileIterator it = new ParquetFileIterator(parquetQs);
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
	
	public static void parseQueryStructToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName, Iterator<IHeadersDataRow> it, boolean temporalSelectors) {
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// engine
		if(qsType == QUERY_STRUCT_TYPE.ENGINE || qsType == QUERY_STRUCT_TYPE.DIRECT_API_QUERY) {
			parseEngineQsToFlatTable(dataframe, qs, frameTableName, it);
		}
		// engine with raw query
		else if(qsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY 
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY) {
			if(it instanceof IRawSelectWrapper) {
				parseRawQsToFlatTable(dataframe, qs, frameTableName, (IRawSelectWrapper) it, qs.getEngineId());
			} else if(it instanceof BasicIteratorTask) {
				try {
					parseRawQsToFlatTable(dataframe, qs, frameTableName, ((BasicIteratorTask) it).getIterator(), qs.getEngineId());
				} catch (Exception e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			} else {
				throw new IllegalArgumentException("Unable to process iterator of type " + it.getClass().getSimpleName() + " to generate the frame metadata");
			}
		}
		// frame
		else if(qsType == QUERY_STRUCT_TYPE.FRAME) {
			parseFrameQsToFlatTable(dataframe, qs, frameTableName, temporalSelectors);
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
		// parquet file
		else if(qsType == QUERY_STRUCT_TYPE.PARQUET_FILE) {
			parseParquetFileQsToFlatTable(dataframe, (ParquetQueryStruct) qs, frameTableName);
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
	 * @param it 
	 */
	private static void parseEngineQsToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName, Iterator<IHeadersDataRow> it) {
		SemossDataType[] iteratorTypes = null;
		if(it instanceof IRawSelectWrapper) {
			iteratorTypes = ((IRawSelectWrapper) it).getTypes();
		}
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
			String itDataType = null;
			if(iteratorTypes != null && iteratorTypes[i] != null) {
				itDataType = iteratorTypes[i].toString();
			}
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, engineName, qsName);
			metaData.setDerivedToProperty(uniqueHeader, isDerived);
			metaData.setAliasToProperty(uniqueHeader, alias);

			// if we are doing a complex operation
			// use the data type from the iterator
			// if it is a column
			// use the data type from the metadata
			if(itDataType != null && selector.getSelectorType() != SELECTOR_TYPE.COLUMN) {
				metaData.setDataTypeToProperty(uniqueHeader, itDataType);
			} else {
				if(dataType == null) {
					// this only happens when we have a column selector
					// and we need to query the local master
					QueryColumnSelector cSelect = (QueryColumnSelector) selector;
					String table = cSelect.getTable();
					String column = cSelect.getColumn();
					if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
						String type = MasterDatabaseUtility.getBasicDataType(engineName, table, null);
						String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineName, table, null);
						metaData.setDataTypeToProperty(uniqueHeader, type);
						if (adtlType != null) {
							metaData.setAddtlDataTypeToProperty(uniqueHeader, adtlType);
						}
					} else {
						String type = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
						String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineName, column, table);
						metaData.setDataTypeToProperty(uniqueHeader, type);
						if (adtlType != null) {
							metaData.setAddtlDataTypeToProperty(uniqueHeader, adtlType);
						}
					}
				} else {
					// use the data type
					metaData.setDataTypeToProperty(uniqueHeader, dataType);
				}
			}
		}
	}
	
	private static void parseRawQsToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName, IRawSelectWrapper it, String source) {
		SemossDataType[] types = it.getTypes();
		String[] columns = it.getHeaders();
		columns = HeadersException.getInstance().getCleanHeaders(columns);
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
	private static void parseFrameQsToFlatTable(ITableDataFrame dataframe, SelectQueryStruct qs, String frameTableName, boolean temporalSelectors) {
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
			String adtlType = null;
			
			if(dataType == null) {
				// cannot assume the iterator that created this qs
				// is from the same frame as the frame we are adding this data to
				if(qs.getFrame() != null) {
					OwlTemporalEngineMeta meta = qs.getFrame().getMetaData();
					dataType = meta.getHeaderTypeAsString(selector.getQueryStructName());
					String uniqueName = meta.getUniqueNameFromAlias(selector.getQueryStructName());
					if(dataType == null) {
						dataType = meta.getHeaderTypeAsString(uniqueName);
					}
					
					adtlType = meta.getHeaderAdtlType(selector.getQueryStructName());
					if (adtlType == null && uniqueName != null) {
						adtlType = meta.getHeaderAdtlType(uniqueName);
					}
				}
				if(dataType == null) {
					// if still null
					// try the current frame
					dataType = metaData.getHeaderTypeAsString(selector.getQueryStructName());
				}
				if (adtlType == null) {
					adtlType = metaData.getHeaderAdtlType(selector.getQueryStructName());
				}
			}
			
			String uniqueHeader = frameTableName + "__" + alias;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setQueryStructNameToProperty(uniqueHeader, source, qsName);
			metaData.setAliasToProperty(uniqueHeader, alias);
			metaData.setDataTypeToProperty(uniqueHeader, dataType);
			if (adtlType != null) {
				metaData.setAddtlDataTypeToProperty(uniqueHeader, adtlType);
			}
			
			// all values from a frame are considered derived
			metaData.setDerivedToProperty(uniqueHeader, true);
			
			if(temporalSelectors) {
				metaData.setSelectorComplexToProperty(uniqueHeader, true);
				metaData.setSelectorTypeToProperty(uniqueHeader, selector.getSelectorType());
				metaData.setSelectorObjectToProperty(uniqueHeader, GsonUtility.getDefaultGson().toJson(selector));
			}
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
	
	private static void parseParquetFileQsToFlatTable(ITableDataFrame dataframe, ParquetQueryStruct qs, String frameTableName) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String parquetFileName = FilenameUtils.getBaseName(qs.getFilePath());
		// remove the ugly stuff we add to make this unique
		if(parquetFileName.contains("_____UNIQUE")) {
			parquetFileName = parquetFileName.substring(0, parquetFileName.indexOf("_____UNIQUE"));
		}
		parquetFileName = Utility.makeAlphaNumeric(parquetFileName);
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
			metaData.setQueryStructNameToProperty(uniqueHeader, parquetFileName, qsName);
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
				metaData.setQueryStructNameToProperty(uniqueHeader, parquetFileName, table);
				String type = dataTypes.get(table);
				metaData.setDataTypeToProperty(uniqueHeader, type);
				
				if(additionalTypes.get(table) != null) {
					metaData.setAddtlDataTypeToProperty(uniqueHeader, additionalTypes.get(uniqueHeader));
				}
			} else {
				metaData.setQueryStructNameToProperty(uniqueHeader, parquetFileName, table + "__" + column);
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
	 * @param metaData
	 * @param columns
	 * @param types
	 * @param frameTableName
	 */
	public static void parseTableColumnsAndTypesToFlatTable(OwlTemporalEngineMeta metaData, String[] columns, String[] types, String frameTableName) {
		// define the frame table name as a primary key within the meta
		parseTableColumnsAndTypesToFlatTable(metaData, columns, types, frameTableName, null, null, null);
	}
	
	/**
	 * When we are generating information programmatically without a standard data source
	 * We can create a new meta using the column names, types, and the table name
	 * 
	 * Example use: Pivot/Unpivot in R which drastically modifies the table
	 * @param metaData
	 * @param columns
	 * @param types
	 * @param frameTableName
	 * @param adtlDAtaTypes
	 * @param sourcesMap
	 * @param complexSelectors
	 */
	public static void parseTableColumnsAndTypesToFlatTable(OwlTemporalEngineMeta metaData, String[] columns, String[] types, String frameTableName, 
			Map<String, String> adtlDAtaTypes, Map<String, List<String>> sourcesMap, Map<String, String[]> complexSelectors) {
		// define the frame table name as a primary key within the meta
		metaData.addVertex(frameTableName);
		metaData.setPrimKeyToVertex(frameTableName, true);
		
		// there are other metadata fields that if we have, would be good to keep/maintain
		if(adtlDAtaTypes != null && adtlDAtaTypes.containsKey(frameTableName)) {
			metaData.setAddtlDataTypeToVertex(frameTableName, adtlDAtaTypes.get(frameTableName));
		}
		if(sourcesMap != null && sourcesMap.containsKey(frameTableName) && !sourcesMap.get(frameTableName).isEmpty()) {
			List<String> sources = sourcesMap.get(frameTableName);
			for(String s : sources) {
				String[] split = s.split(":::", 2);
				metaData.setQueryStructNameToVertex(frameTableName, split[0], split[1]);
			}
		} else {
			metaData.setQueryStructNameToVertex(frameTableName, "UNKNOWN", frameTableName);
		}
		
		int numCols = columns.length;
		for(int i = 0; i < numCols; i++) {
			String columnName = columns[i];
			String type = types[i];
			
			String uniqueHeader = frameTableName + "__" + columnName;
			metaData.addProperty(frameTableName, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, columnName);
			metaData.setDataTypeToProperty(uniqueHeader, type);
			
			// there are other metadata fields that if we have, would be good to keep/maintain
			if(adtlDAtaTypes != null && adtlDAtaTypes.containsKey(uniqueHeader)) {
				metaData.setAddtlDataTypeToVertex(uniqueHeader, adtlDAtaTypes.get(uniqueHeader));
			}
			if(sourcesMap != null && sourcesMap.containsKey(uniqueHeader) && !sourcesMap.get(uniqueHeader).isEmpty()) {
				List<String> sources = sourcesMap.get(uniqueHeader);
				for(String s : sources) {
					String[] split = s.split(":::", 2);
					metaData.setQueryStructNameToProperty(uniqueHeader, split[0], split[1]);
				}
			} else {
				metaData.setQueryStructNameToProperty(uniqueHeader, "UNKNOWN", frameTableName);
			}
		}
		
		// add the complex selectors as well
		if(complexSelectors != null) {
			for(String header : complexSelectors.keySet()) {
				String[] complexDetails = complexSelectors.get(header);
				if(header.contains("__")) {
					header = header.split("__")[1];
				}
				String alias = (String) complexDetails[0];
				String dataType = (String) complexDetails[1];
				String qsInfo = (String) complexDetails[2];
				String qType = (String) complexDetails[3];
				String qJson = (String) complexDetails[4];
				
				String uniqueHeader = frameTableName + "__" + alias;
				metaData.addProperty(frameTableName, uniqueHeader);
				metaData.setFullQueryStructNameToProperty(uniqueHeader, qsInfo);
				metaData.setAliasToProperty(uniqueHeader, alias);
				metaData.setDataTypeToProperty(uniqueHeader, dataType);
				metaData.setDerivedToProperty(uniqueHeader, true);
				metaData.setSelectorComplexToProperty(uniqueHeader, true);
				metaData.setSelectorTypeToProperty(uniqueHeader, qType);
				metaData.setSelectorObjectToProperty(uniqueHeader, qJson);
			}
		}
	}
	
	/**
	 * Merge additional data types into the metadata assuming the headers exist
	 * @param metaData
	 * @param adtlDataTypes
	 */
	public static void mergeFlatTableAdditionalDataTypes(OwlTemporalEngineMeta metaData, Map<String, String> adtlDataTypes) {
		if(adtlDataTypes == null || adtlDataTypes.isEmpty()) {
			return;
		}
		List<String> uNames = metaData.getUniqueNames();
		
		for(String name : uNames) {
			if(adtlDataTypes.containsKey(name)) {
				String adtlDataType = adtlDataTypes.get(name);
				if (name.contains("__")) {
					metaData.setAddtlDataTypeToProperty(name, adtlDataType);
				} else {
					metaData.setAddtlDataTypeToVertex(name, adtlDataType);
				}
			}
		}
	}
	
	/**
	 * Merge additional data types into the metadata assuming the headers exist
	 * @param metaData
	 * @param sourcesMap
	 */
	public static void mergeFlatTableSources(OwlTemporalEngineMeta metaData, Map<String, List<String>> sourcesMap) {
		if(sourcesMap == null || sourcesMap.isEmpty()) {
			return;
		}
		List<String> uNames = metaData.getUniqueNames();
		for(String name : uNames) {
			if(sourcesMap.containsKey(name)) {
				List<String> sources = sourcesMap.get(name);
				for(String s : sources) {
					String[] split = s.split(":::", 2);
					if(name.contains("__")) {
						metaData.setQueryStructNameToProperty(name, split[0], split[1]);
					} else {
						metaData.setQueryStructNameToVertex(name, split[0], split[1]);
					}
				}
			}
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
		parseQueryStructAsGraph(dataframe, qs, edgeHash, true);
	}
	
	public static void parseFlatEdgeHashAsGraph(TinkerFrame dataframe, SelectQueryStruct qs, Map<String, Set<String>> edgeHash) {
		parseQueryStructAsGraph(dataframe, qs, edgeHash, false);
	}
	
	private static void parseQueryStructAsGraph(ITableDataFrame dataframe, SelectQueryStruct qs, Map<String, Set<String>> edgeHash, boolean addRels) {
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
					String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineName, table, null);
					metaData.setDataTypeToVertex(alias, type);
					if (adtlType != null) {
						metaData.setAddtlDataTypeToVertex(alias, adtlType);
					}
				} else {
					String type = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
					String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineName, column, table);
					metaData.setDataTypeToVertex(alias, type);
					if (adtlType != null) {
						metaData.setAddtlDataTypeToVertex(alias, adtlType);
					}
				}
			} else {
				metaData.setDataTypeToVertex(alias, dataType);
			}
		}
		
		List<String> addedRels = new Vector<String>();
		if(addRels) {
			// now add the relationships
			Set<IRelation> relations = qs.getRelations();
			for (IRelation relationship : relations) {
				if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
					BasicRelationship rel = (BasicRelationship) relationship;

					String up = rel.getFromConcept();
					if(aliasMap.containsKey(up)) {
						up = aliasMap.get(up);
					}
					String joinType = rel.getJoinType();
					String down = rel.getToConcept();
					if(aliasMap.containsKey(down)) {
						down = aliasMap.get(down);
					}
					metaData.addRelationship(up, down, joinType);
					addedRels.add(up + down);
				} else {
					classLogger.info("Cannot process relationship of type: " + relationship.getRelationType());
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
					String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineName, table, null);
					metaData.setDataTypeToVertex(alias, type);
					if (adtlType != null) {
						metaData.setAddtlDataTypeToVertex(alias, adtlType);
					}
				} else {
					String type = MasterDatabaseUtility.getBasicDataType(engineName, column, table);
					String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineName, column, table);
					metaData.setDataTypeToVertex(alias, type);
					if (adtlType != null) {
						metaData.setAddtlDataTypeToVertex(alias, adtlType);
					}
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
	public static Map<String, Set<String>> getFlatEngineEdgeHash(SelectQueryStruct qs) {
		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		IDatabaseEngine engine = qs.getEngine();

		// non graph db
		List<IQuerySelector> selectors = qs.getSelectors();
		Map<String, String> aliasMapping = flushOutAliases(selectors);
		Map<String, String> parent = new HashMap<String, String>();
		List<String> ignoreSelector = new ArrayList<String>();
		// add all relationships
		Set<IRelation> relations = qs.getRelations();
		for (IRelation relationship : relations) {
			if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
				BasicRelationship rel = (BasicRelationship) relationship;

				String upVertex = rel.getFromConcept();
				String downVertex = rel.getToConcept();
				
				String[] results = getJoinInformation(engine, upVertex, downVertex);
				if(results != null) {
					parent.put(results[0], results[1]);
					parent.put(results[2], results[3]);

					String upQs = results[0] + "__" + results[1];
					String downQs = results[2] + "__" + results[3];
					
					String upVertexAlias = aliasMapping.get(upQs);
					String downVertexAlias = aliasMapping.get(downQs);
					
					if(upVertexAlias == null || downVertexAlias == null) {
						// this is a pass through
						continue;
					}
					
					Set<String> downConnections = null;
					if(edgeHash.containsKey(upVertexAlias)) {
						downConnections = edgeHash.get(upVertexAlias);
					} else {
						downConnections = new HashSet<String>();
						edgeHash.put(upVertexAlias, downConnections);
					}
					
					if(!edgeHash.containsKey(downVertexAlias)) {
						edgeHash.put(downVertexAlias, new HashSet<String>());
					}
					
					downConnections.add(downVertexAlias);
					
					// add to ignore
					ignoreSelector.add(upQs);
					ignoreSelector.add(downQs);
				}
			} else {
				classLogger.info("Cannot process relationship of type: " + relationship.getRelationType());
			}
		}
		
		// go through all the selectors
		// these are what will be returned
		// and what we need to figure out how to connect
		
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			// TODO: doing base case first cause Davy said so
			// TODO: doing base case first cause Davy said so
			// TODO: doing base case first cause Davy said so
			// TODO: doing base case first cause Davy said so
			if (selector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
				continue;
			}
			QueryColumnSelector cSelector = (QueryColumnSelector) selector;
			String table = cSelector.getTable();
			String column = cSelector.getColumn();
			String querySelector = table + "__" + column;
			if(ignoreSelector.contains(querySelector)) {
				continue;
			}
			
			if(!parent.containsKey(table)) {
				parent.put(table, aliasMapping.get(querySelector));
				// put alias yuck!!!
				aliasMapping.put(table, aliasMapping.get(querySelector));
			}
			
			if(parent.get(table).equals(aliasMapping.get(querySelector))) {
				// this is so we prevent a self loop
				continue;
			}
			
			String downstreamNode = aliasMapping.get(querySelector);
			String parentUpColumn = parent.get(table);
			
			// in case the defined joined requires columns
			// that are not part of the selectors
			if(aliasMapping.keySet().contains(parentUpColumn)) {
				Set<String> downConnections = new HashSet<String>();
				if (edgeHash.containsKey(parent.get(table))) {
					downConnections = edgeHash.get(parent.get(table));
				}
				downConnections.add(downstreamNode);
				edgeHash.put(parent.get(table), downConnections);
			} else {
				// if the parent column is not an actual return
				// we need to find what join it has
				// that is a return
				for (IRelation relationship : relations) {
					if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
						BasicRelationship rel = (BasicRelationship) relationship;
						if(rel.getToConcept().equals(table)) {
							parentUpColumn = rel.getFromConcept();
							break;
						}
					} else {
						classLogger.info("Cannot process relationship of type: " + relationship.getRelationType());
					}
				}
				
				Set<String> downConnections = new HashSet<String>();
				if (edgeHash.containsKey(parentUpColumn)) {
					downConnections = edgeHash.get(parentUpColumn);
				}
				downConnections.add(downstreamNode);
				edgeHash.put(parentUpColumn, downConnections);
			}
		}
		
		return edgeHash;
	}
	
	/**
	 * Get the edge hash corresponding to the relationships defined within the qs
	 * @param qs
	 * @return
	 */
	public static Map<String, Set<String>> getGraphEdgeHash(SelectQueryStruct qs) {
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
		Set<IRelation> relations = qs.getRelations();
		for (IRelation relationship : relations) {
			if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
				BasicRelationship rel = (BasicRelationship) relationship;

				String upVertex = rel.getFromConcept();
				String downVertex = rel.getToConcept();
				
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
			} else {
				classLogger.info("Cannot process relationship of type: " + relationship.getRelationType());
			}
		}
		
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
	
	private static String[] getJoinInformation(IDatabaseEngine engine, String fromString, String toString) {
		String fromTable = null;
		String fromCol = null;
		String toTable = null;
		String toCol = null;
		
		if(fromString.contains("__")) {
			String[] split = fromString.split("__");
			fromTable = split[0];
			fromCol = split[1];
		}
		
		if(toString.contains("__")) {
			String[] split = toString.split("__");
			toTable = split[0];
			toCol = split[1];
		}
		
		if(fromTable != null && fromCol != null && toTable != null && toCol != null) {
			return new String[]{fromTable, fromCol, toTable, toCol};
		}
		
		String fromURI = engine.getPhysicalUriFromPixelSelector(fromString);
		String toURI = engine.getPhysicalUriFromPixelSelector(toString);
		
		// need to figure out what the predicate is from the owl
		// also need to determine the direction of the relationship -- if it is forward or backward
		String query = "SELECT ?relationship WHERE {<" + fromURI + "> ?relationship <" + toURI + "> } ORDER BY DESC(?relationship)";
		TupleQueryResult res = (TupleQueryResult) engine.execOntoSelectQuery(query);
		String predURI = null;
		try {
			if(res.hasNext()){
				predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
			}
			else {
				query = "SELECT ?relationship WHERE {<" + toURI + "> ?relationship <" + fromURI + "> } ORDER BY DESC(?relationship)";
				res = (TupleQueryResult) engine.execOntoSelectQuery(query);
				if(res.hasNext()){
					predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
				}
			}
		} catch (QueryEvaluationException e) {
			// ignore for now
			return null;
		}
		if(predURI == null) {
			return null;
		}
		
		String[] predPieces = Utility.getInstanceName(predURI).split("[.]");
		if(predPieces.length == 4)
		{
			fromTable = predPieces[0];
			fromCol = predPieces[1];
			toTable = predPieces[2];
			toCol = predPieces[3];
		}
		else if(predPieces.length == 6) // this is coming in with the schema
		{
			// EHUB_CLM_SDS . EHUB_CLM_EVNT . CLM_EVNT_KEY . EHUB_CLM_SDS . EHUB_CLM_PROV_DMGRPHC . CLM_EVNT_KEY
			// [0]               [1]            [2]             [3]             [4]                    [5]
			fromTable = predPieces[0] + "." + predPieces[1];
			fromCol = predPieces[2];
			toTable = predPieces[3] + "." + predPieces[4];
			toCol = predPieces[5];
		}
		
		return new String[]{fromTable, fromCol, toTable, toCol};
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
	public static void parseNativeQueryStructIntoMeta(ITableDataFrame dataframe, SelectQueryStruct qs, boolean ignore, SemossDataType[] selectorDataTypes) {
		List<IQuerySelector> selectors = qs.getSelectors();
		String engineId = qs.getEngineId();
		
		OwlTemporalEngineMeta metaData = dataframe.getMetaData();
		Set<String> addedQsNames = new HashSet<String>();
		// loop through all the selectors
		// insert them into the frame
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String qsName = selector.getQueryStructName();
			SemossDataType sType = null;
			if(selectorDataTypes != null && selectorDataTypes.length > i) {
				sType = selectorDataTypes[i];
			}
			addedQsNames.add(qsName);
			if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				QueryColumnSelector cSelect = (QueryColumnSelector) selector;
				String table = cSelect.getTable();
				String column = cSelect.getColumn();
				
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					metaData.addVertex(table);
					metaData.setQueryStructNameToVertex(table, engineId, qsName);
					if(sType != null) {
						metaData.setDataTypeToVertex(table, sType.toString());
					} else {
						String type = MasterDatabaseUtility.getBasicDataType(engineId, table, null);
						String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineId, table, null);
						metaData.setDataTypeToVertex(table, type);
						if (adtlType != null) {
							metaData.setAddtlDataTypeToVertex(table, adtlType);
						}
					}
					metaData.setAliasToVertex(table, alias);
				} else {
					// need to account for RDBMS vs. RDF
					if(ignore) {
						// add the vertex since we never get a prim key placeholder
						metaData.addVertex(table);
						metaData.setPrimKeyToVertex(table, true);
					}
					String uniqueName = qsName;
					metaData.addProperty(table, uniqueName);
					metaData.setQueryStructNameToProperty(uniqueName, engineId, qsName);
					if(sType != null) {
						metaData.setDataTypeToProperty(uniqueName, sType.toString());
					} else {
						String type = MasterDatabaseUtility.getBasicDataType(engineId, column, table);
						String adtlType = MasterDatabaseUtility.getAdditionalDataType(engineId, column, table);
						metaData.setDataTypeToProperty(uniqueName, type);
						if (adtlType != null) {
							metaData.setAddtlDataTypeToProperty(uniqueName, adtlType);
						}
					}
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
				metaData.setQueryStructNameToVertex(alias, engineId, qsName);
				metaData.setAliasToVertex(alias, alias);
				if(sType != null) {
					metaData.setDataTypeToVertex(alias, sType.toString());
				} else {
					metaData.setDataTypeToVertex(alias, selector.getDataType());
				}

				// i will add the selector into the meta
				// and get it back later
				// FE will only use the alias to utilize this selector
				// so i will go ahead and generate the original selector
				metaData.setSelectorComplexToVertex(alias, true);
				metaData.setSelectorTypeToVertex(alias, selector.getSelectorType());
				metaData.setSelectorObjectToVertex(alias, GsonUtility.getDefaultGson().toJson(selector));
			}
		}
		
		// now add the relationships
		Set<IRelation> relations = qs.getRelations();
		for (IRelation relationship : relations) {
			if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
				BasicRelationship rel = (BasicRelationship) relationship;

				String upVertex = rel.getFromConcept();
				String joinType = rel.getJoinType();
				String downVertex = rel.getToConcept();
				
				String upKey = upVertex;
				String downKey = downVertex;
				
				if(!addedQsNames.contains(upVertex)) {
					if(upKey.contains("__")) {
						upKey = upKey.split("__")[0];
					}
					metaData.addVertex(upKey);
					metaData.setPrimKeyToVertex(upKey, true);
					addedQsNames.add(upVertex);
				}
				if(!addedQsNames.contains(downVertex)) {
					if(downKey.contains("__")) {
						downKey = downKey.split("__")[0];
					}
					metaData.addVertex(downKey);
					metaData.setPrimKeyToVertex(downKey, true);
					addedQsNames.add(downVertex);
				}
				
				metaData.addRelationship(upVertex, downVertex, joinType);
			} else {
				classLogger.info("Cannot process relationship of type: " + relationship.getRelationType());
			}
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
			return getMetaDataFromEngineQs(qs, it);
		} else if(qsType == QUERY_STRUCT_TYPE.FRAME) {
			return getMetaDataFromFrameQs(qs, it);
		} else if(qsType == QUERY_STRUCT_TYPE.CSV_FILE) {
			return getMetaDataFromCsvQs((CsvQueryStruct)qs);
		} else if (qsType == QUERY_STRUCT_TYPE.EXCEL_FILE) {
			return getMetaDataFromExcelQs((ExcelQueryStruct)qs);
		} else if(qsType == QUERY_STRUCT_TYPE.PARQUET_FILE) {
			return getMetaDataFromParquetQs((ParquetQueryStruct)qs);
		} else if(qsType == QUERY_STRUCT_TYPE.LAMBDA) {
			return getMetaDataFromLambdaQs((LambdaQueryStruct)qs);
		} else if(qsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY) {
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
		headers = HeadersException.getInstance().getCleanHeaders(headers);
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

	private static Map<String, SemossDataType> getMetaDataFromEngineQs(SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		// do not have a good way of grabbing the types from sesame
		// so pull from the engine itself
		if(it instanceof IRawSelectWrapper && !(it instanceof RawSesameSelectWrapper)) {
			return getMetaDataFromQuery((IRawSelectWrapper)it);
		}
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

	private static Map<String, SemossDataType> getMetaDataFromFrameQs(SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		if(it instanceof IRawSelectWrapper && !(it instanceof RawSesameSelectWrapper)) {
			return getMetaDataFromQuery((IRawSelectWrapper)it);
		}
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
	
	private static Map<String, SemossDataType> getMetaDataFromParquetQs(ParquetQueryStruct qs) {
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
		if(qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY
				|| qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY) {
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
		// need to account if this join has been already accounted for
		Set<Integer> indexIncluded = new HashSet<Integer>();
		int numJoins = joins.size();
		
		List<IQuerySelector> newSelectors = qs.getSelectors();
		int numNewSelectors = newSelectors.size();
		// note, this is already cleaned so it is not table__col and just col
		boolean foundColumnJoin = false;
		String[] modifiedNewHeaders = new String[numNewSelectors];
		NEW_SELECTOR_LOOP : for(int i = 0; i < numNewSelectors; i++) {
			IQuerySelector newSelector = newSelectors.get(i);
			String alias = newSelector.getAlias();
			
			String qsName = newSelector.getQueryStructName();
			JOIN_LOOP : for(int jIdx = 0; jIdx < numJoins; jIdx++) {
				if(indexIncluded.size() == numJoins) {
					// all joins have been accounted for
					// so we are done
					break JOIN_LOOP;
				}
				// if we accounted for this
				// continue
				if(indexIncluded.contains(jIdx)) { 
					continue;
				}
				Join j = joins.get(jIdx);
				
				String existingColName = j.getLColumn();
				String newColName = j.getRColumn();
				
				if(newColName.contains("__")) {
					newColName = newColName.split("__")[1];
				}
				// if this is true, it means that the unique name of this selector is part of a join
				// and we want to replace its name
				if(qsName.equals(newColName) || alias.equals(newColName)) {
					foundColumnJoin = true;
					if(existingColName.contains("__")) {
						modifiedNewHeaders[i] = existingColName.split("__")[1];
					} else {
						modifiedNewHeaders[i] = existingColName;
					}
					// so we do not do this index again
					indexIncluded.add(jIdx);
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
		parseQueryStructToFlatTable(dataframe, updatedQsForJoins, tableName, it, false);
	}

	private static void parseRawQsToFlatTableWithJoin(ITableDataFrame dataframe, String frameTableName, IRawSelectWrapper it, List<Join> joins, String source) {
		SemossDataType[] types = it.getTypes();
		String[] columns = it.getHeaders();
		columns = HeadersException.getInstance().getCleanHeaders(columns);

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
				String newColName = j.getRColumn();
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
