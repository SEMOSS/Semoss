package prerna.ds.r;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.ds.util.flatfile.ExcelFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaRserveTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RFrameBuilder {

	protected static final String CLASS_NAME = RFrameBuilder.class.getName();
	protected Logger logger = LogManager.getLogger(CLASS_NAME);
	
	// holds the name of the current data table
	protected String dataTableName = "datatable";
	
	// keep track of the indices that exist in the table for optimal speed in sorting
	protected Set<String> columnIndexSet = new HashSet<String>();

	// holds the connection object to execute r
	protected AbstractRJavaTranslator rJavaTranslator = null;
	
	public RFrameBuilder() {
		this.rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(null, logger);
		this.rJavaTranslator.startR(); 
	}

	public RFrameBuilder(String dataTableName) {
		this();
		this.dataTableName = dataTableName;
	}
	
	public RFrameBuilder(String dataTableName, RConnection retCon, String port) {
		this();
		this.dataTableName = dataTableName;
		this.rJavaTranslator.setConnection(retCon);
		this.rJavaTranslator.setPort(port);
	}

	protected String getTableName() {
		return this.dataTableName;
	}
	
	protected void setTableName(String dataTableName) {
		this.dataTableName = dataTableName;
	}
	
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	////////////////////////////////////////////////////////////////////
	///////////////////// Abstract Methods /////////////////////////////
	
	/**
	 * Method to run a r script and not need to process output
	 * @param r
	 */
	public void evalR(String r) {
		this.rJavaTranslator.executeEmptyR(r);
	}
	
	/**
	 * Wrap the R script in a try-eval in order to get the same error message that a user would see if using
	 * the R console
	 * @param rscript			The R script to execute
	 * @return					The R script wrapped in a try-eval statement
	 */
	public String addTryEvalToScript(String rscript) {
		return "try(eval(" + rscript + "), silent=FALSE)";
	}

	/**
	 * Creates a new data table from an iterator
	 * @param it					The iterator to flush into a r data table
	 * @param typesMap				The data type of each column
	 */
	public void createTableViaIterator(String tableName, Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap) {
		Map<String, String> additionalType = new HashMap<String,String>();
		
		/*
		 * We have an iterator that comes for 3 main sources
		 * 1) some kind of resultset (i.e. engine/endpoint) -> we flush this out to a csv file and load it
		 * 2) an iterator for a csv file
		 * 3) an iterator for a single sheet of an excel file (later we will figure out multi sheet excels...)
		 */
		if(it instanceof CsvFileIterator) {
			createTableViaCsvFile(tableName, (CsvFileIterator) it);
			additionalType = ((CsvFileIterator) it).getQs().getAdditionalTypes();
		} else if(it instanceof ExcelFileIterator) {
			createTableViaExcelFile(tableName, (ExcelFileIterator) it);
			additionalType = ((ExcelFileIterator) it).getQs().getAdditionalTypes();
		} else {
			// default behavior is to just write this to a csv file
			// get the fread() notation for that csv file
			// and read it back in
			String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".csv";
			File newFile = Utility.writeResultToFile(newFileLoc, it, typesMap, "\t");
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(tableName, newFile.getAbsolutePath(), "\\t");
			evalR(loadFileRScript);
			newFile.delete();
		}	
		
		// ... and make sure you update the types after!
		// the types map will have the finalized headers to their types (?) TODO check this, if not true, need to do this in csv/excel iterators
		// prior to updating the datatypes, need an inverted form of the typesMap
		Map<SemossDataType, List<String>> invTypesMap = new HashMap<SemossDataType, List<String>>();
		for (String header : typesMap.keySet()){
			SemossDataType dataType = typesMap.get(header);
			invTypesMap.computeIfAbsent(dataType, v -> new ArrayList<String>());
			invTypesMap.get(dataType).add(header);
		}

		// alter types
		alterColumnTypes(tableName, typesMap, additionalType);
		
		//add indices
		addColumnIndex(tableName, getColumnNames());
	}
	
	private void createTableViaCsvFile(String tableName, CsvFileIterator it) {
		CsvQueryStruct qs = it.getQs();
		String[] newCleanHeaders = it.getHelper().getAllCSVHeaders();
		{
			long start = System.currentTimeMillis();
			logger.info("Loading R table via CSV File");
			// get you the fread notation with the csv file within the iterator
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(tableName, it.getFileLocation(), qs.getDelimiter() + "");
			evalR(loadFileRScript);

			// fread will use the original headers, even if there are duplicates
			// we need to fix this -> grab the new ones and write it out
			evalR("setnames(" + tableName + ", " + RSyntaxHelper.createStringRColVec(newCleanHeaders) + ")");
			long end = System.currentTimeMillis();
			logger.info("Loading R done in " + (end-start) + "ms");
		}

		if (qs.getSelectors().size() < newCleanHeaders.length) {
			long start = System.currentTimeMillis();
			logger.info("Need to filter R table based on selected headers");
			RInterpreter interp = new RInterpreter();
			interp.setDataTableName(tableName);
			interp.setQueryStruct(qs);
			Map<String, String> strTypes = qs.getColumnTypes();
			Map<String, SemossDataType> enumTypes = new HashMap<String, SemossDataType>();
			for(String key : strTypes.keySet()) {
				enumTypes.put(key, SemossDataType.convertStringToDataType(strTypes.get(key)));
			}
			interp.setColDataTypes(enumTypes);
			String query = interp.composeQuery();
			evalR(tableName + "<-" + query);
			long end = System.currentTimeMillis();
			logger.info("Done filter R table based on selected headers in " + (end-start) + "ms");			
		}
		
		if(!qs.getExplicitFilters().isEmpty()) {
			long start = System.currentTimeMillis();
			logger.info("Need to filter R table based on QS");
			// we need to execute a script to modify the table to only contain the data based on the filters defined
			//create a new querystruct object that will have header names in the format used for RInterpreter
			SelectQueryStruct modifiedQs = new SelectQueryStruct();
			updateFileSelectors(modifiedQs, tableName, newCleanHeaders);

			//add filters to the new qs
			List<IQueryFilter> gFilters = qs.getExplicitFilters().getFilters();
			for (int i = 0; i < gFilters.size(); i++) {
				//TODO: example this update filter logic!
				IQueryFilter sFilter = gFilters.get(i);
				if(sFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
					SimpleQueryFilter updatedFilter = updateFilter(tableName, (SimpleQueryFilter) sFilter);
					modifiedQs.addExplicitFilter(updatedFilter);
				}
			}
			RInterpreter interp = new RInterpreter();
			interp.setDataTableName(tableName);
			interp.setQueryStruct(modifiedQs);
			Map<String, String> strTypes = qs.getColumnTypes();
			Map<String, SemossDataType> enumTypes = new HashMap<String, SemossDataType>();
			for(String key : strTypes.keySet()) {
				enumTypes.put(key, SemossDataType.convertStringToDataType(strTypes.get(key)));
			}
			interp.setColDataTypes(enumTypes);
			String query = interp.composeQuery();
			evalR(tableName + "<-" + query);
			long end = System.currentTimeMillis();
			logger.info("Done filter R table in " + (end-start) + "ms");
		}
	}

	private void createTableViaExcelFile(String tableName, ExcelFileIterator it) {
		ExcelQueryStruct qs = it.getQs();
		String sheetName = qs.getSheetName();
		String[] newCleanHeaders = it.getHelper().getHeaders(sheetName);
		{
			long start = System.currentTimeMillis();
			logger.info("Loading R table via Excel File");
			// get you the fread notation with the csv file within the iterator
			String loadFileRScript = RSyntaxHelper.getExcelReadSheetSyntax(tableName, it.getFileLocation(), sheetName);
			evalR(loadFileRScript);

			evalR("setnames(" + tableName + ", " + RSyntaxHelper.createStringRColVec(newCleanHeaders) + ")");
			long end = System.currentTimeMillis();
			logger.info("Loading R done in " + (end-start) + "ms");
		}
		
		if (qs.getSelectors().size() < newCleanHeaders.length) {
			long start = System.currentTimeMillis();
			logger.info("Need to filter R table based on selected headers");
			RInterpreter interp = new RInterpreter();
			interp.setDataTableName(tableName);
			interp.setQueryStruct(qs);
			Map<String, String> strTypes = qs.getColumnTypes();
			Map<String, SemossDataType> enumTypes = new HashMap<String, SemossDataType>();
			for(String key : strTypes.keySet()) {
				enumTypes.put(key, SemossDataType.convertStringToDataType(strTypes.get(key)));
			}
			interp.setColDataTypes(enumTypes);
			String query = interp.composeQuery();
			evalR(tableName + "<-" + query);
			long end = System.currentTimeMillis();
			logger.info("Done filter R table based on selected headers in " + (end-start) + "ms");			
		}
		
		if(!qs.getExplicitFilters().isEmpty()) {
			long start = System.currentTimeMillis();
			logger.info("Need to filter R table based on QS");
			// we need to execute a script to modify the table to only contain the data based on the filters defined
			//create a new querystruct object that will have header names in the format used for RInterpreter
			SelectQueryStruct modifiedQs = new SelectQueryStruct();
			updateFileSelectors(modifiedQs, tableName, newCleanHeaders);

			//add filters to the new qs
			List<IQueryFilter> gFilters = qs.getExplicitFilters().getFilters();
			for (int i = 0; i < gFilters.size(); i++) {
				//TODO: example this update filter logic!
				IQueryFilter sFilter = gFilters.get(i);
				if(sFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
					SimpleQueryFilter updatedFilter = updateFilter(tableName, (SimpleQueryFilter) sFilter);
					modifiedQs.addExplicitFilter(updatedFilter);
				}
			}
			RInterpreter interp = new RInterpreter();
			interp.setDataTableName(tableName);
			interp.setQueryStruct(modifiedQs);
			Map<String, String> strTypes = qs.getColumnTypes();
			Map<String, SemossDataType> enumTypes = new HashMap<String, SemossDataType>();
			for(String key : strTypes.keySet()) {
				enumTypes.put(key, SemossDataType.convertStringToDataType(strTypes.get(key)));
			}
			interp.setColDataTypes(enumTypes);
			String query = interp.composeQuery();
			evalR(tableName + "<-" + query);
			long end = System.currentTimeMillis();
			logger.info("Done filter R table in " + (end-start) + "ms");
		}
	}

	private SelectQueryStruct updateFileSelectors(SelectQueryStruct qs, String tableName, String[] colNames) {
		for (int i = 0; i < colNames.length; i++) {
			qs.addSelector(tableName, colNames[i]);
		}
		return qs;
	}
	
	private SimpleQueryFilter updateFilter(String tableName, SimpleQueryFilter filter) {
		SimpleQueryFilter newFilter = null;
		FILTER_TYPE fType = filter.getFilterType();
		if(fType == FILTER_TYPE.COL_TO_COL) {
			//change both left comparator and right comparator
			String lHeader = filter.getLComparison().getValue().toString();
			NounMetadata lNoun = new NounMetadata(tableName + "__" + lHeader, PixelDataType.COLUMN);
			String rHeader = filter.getRComparison().getValue().toString();
			NounMetadata rNoun = new NounMetadata(tableName + "__" + rHeader, PixelDataType.COLUMN);
			newFilter = new SimpleQueryFilter(lNoun, filter.getComparator() , rNoun);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			//change only the left comparator
			String lHeader = filter.getLComparison().getValue().toString();
			NounMetadata lNoun = new NounMetadata(tableName + "__" + lHeader, PixelDataType.COLUMN);
			newFilter = new SimpleQueryFilter(lNoun, filter.getComparator(), filter.getRComparison());
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			//change only the right comparator
			String rHeader = filter.getRComparison().getValue().toString();
			NounMetadata rNoun = new NounMetadata(tableName + "__" + rHeader, PixelDataType.COLUMN);
			newFilter = new SimpleQueryFilter(filter.getLComparison(), filter.getComparator() , rNoun);
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
		}
		return newFilter;
	}
	
	public void genRowId(String dataTableName, String rowIdName) {
		// syntax
		//id <- rownames(arAmgXk);
		//d <- cbind(id=id, arAmgXk)
		
		// generate the row names first
		String idName = Utility.getRandomString(6);
		String rStatement = idName + "<- rownames(" + dataTableName + ");";
		evalR(rStatement);
		
		// now bind it with the name
		String newName = Utility.getRandomString(6);
		rStatement = newName + " <- cbind(" + rowIdName + "=" + idName + ", " + dataTableName + ");";
		evalR(rStatement);
		
		// change the type of row 
		//evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToNumeric(newName, idName) ) );

		
		// now change the table to this new name
		rStatement = dataTableName + " <- " + newName + ";";
		evalR(rStatement);		
	}
	
	/**
	 * Alters a set of columns togheter
	 * Faster than running each conversion separately
	 * @param tableName
	 * @param typesMap
	 * @param javaDateFormatMap
	 */
	private void alterColumnTypes(String tableName, Map<String, SemossDataType> typesMap, Map<String, String> javaDateFormatMap) {
		// go through all the headers
		// and collect similar types
		// so we can execute with a single r script line
		// for performance improvements
		
		List<String> charColumns = new Vector<String>();
		List<String> intColumns = new Vector<String>();
		List<String> doubleColumns = new Vector<String>();
		Map<String, List<String>> datesMap = new HashMap<String, List<String>>();
		Map<String, List<String>> dateTimeMap = new HashMap<String, List<String>>();

		for(String header : typesMap.keySet()) {
			SemossDataType type = typesMap.get(header);
			if(type == SemossDataType.STRING) {
				charColumns.add(header);
			} else if(type == SemossDataType.INT) {
				intColumns.add(header);
			} else if(type == SemossDataType.DOUBLE) {
				doubleColumns.add(header);
			} else if( type == SemossDataType.DATE && javaDateFormatMap.containsKey(header)) {
				String format = javaDateFormatMap.get(header);
				if(datesMap.containsKey(format)) {
					// add to existing list
					datesMap.get(format).add(header);
				} else {
					List<String> headerList = new Vector<String>();
					headerList.add(header);
					datesMap.put(format, headerList);
				}
			} else if( type == SemossDataType.TIMESTAMP && javaDateFormatMap.containsKey(header)) {
				String format = javaDateFormatMap.get(header);
				if(dateTimeMap.containsKey(format)) {
					// add to existing list
					dateTimeMap.get(format).add(header);
				} else {
					List<String> headerList = new Vector<String>();
					headerList.add(header);
					dateTimeMap.put(format, headerList);
				}
			}
		}
		
		// now that we have everything
		// execute everything
		evalR( addTryEvalToScript ( RSyntaxHelper.alterColumnTypeToCharacter(tableName, charColumns) ) );
		evalR( addTryEvalToScript ( RSyntaxHelper.alterColumnTypeToNumeric(tableName, intColumns) ) );
		evalR( addTryEvalToScript ( RSyntaxHelper.alterColumnTypeToNumeric(tableName, doubleColumns) ) );
		// loop through normal dates
		for(String format : datesMap.keySet()) {
			String rFormat = RSyntaxHelper.translateJavaRDateTimeFormat(format);
			evalR( addTryEvalToScript ( RSyntaxHelper.alterColumnTypeToDate(tableName, rFormat, datesMap.get(format)) ) );
		}
		// loop through time stamps dates
		for(String format : dateTimeMap.keySet()) {
			String rFormat = RSyntaxHelper.translateJavaRDateTimeFormat(format);
			this.rJavaTranslator.runR( RSyntaxHelper.alterColumnTypeToDateTime(tableName, rFormat, dateTimeMap.get(format)) );
		}
	}
	
	protected void addColumnIndex(String tableName, String colName) {
		if (!columnIndexSet.contains(tableName + "+++" + colName)) {
			long start = System.currentTimeMillis();
			String rIndex = null;
			logger.info("CREATING INDEX ON R TABLE = " + tableName + " ON COLUMN = " + colName);
			try {
				rIndex = "CREATE INDEX ON " + tableName + "(" + colName + ")";
				this.rJavaTranslator.executeEmptyR("setindex(" + tableName + "," + colName + ");");
				List<String> confirmedIndices = Arrays.asList(this.rJavaTranslator.getStringArray("indices(" + tableName + ");"));
				if (confirmedIndices.contains(colName)) {
					columnIndexSet.add(tableName + "+++" + colName);		
				}
				long end = System.currentTimeMillis();
				logger.info("TIME FOR R INDEX CREATION = " + (end - start) + " ms");
			} catch (Exception e) {
				logger.info("ERROR WITH R INDEX !!! " + rIndex);
				e.printStackTrace();
			}
		}
	}
	
	protected void addColumnIndex(String tableName, String[] colNames) {
		HashSet<String> colNamesSet = new HashSet<>(Arrays.asList(colNames));
		colNamesSet.removeAll(columnIndexSet);
		
		if (colNamesSet.size() > 0 ){
			long start = System.currentTimeMillis();
			String rIndex = null;
			logger.info("CREATING INDEX ON R TABLE = " + tableName + " ON COLUMN(S) = " + StringUtils.join(colNamesSet,", "));
			try {
				rIndex = "CREATE INDEX ON " + tableName + "(" + StringUtils.join(colNamesSet,", ") + ")";
				this.rJavaTranslator.executeEmptyR(
						"invisible(lapply(c('" + StringUtils.join(colNamesSet,"','") + "')" + ", setindexv, x= " + tableName + "));");
				List<String> confirmedIndices = Arrays.asList(this.rJavaTranslator.getStringArray("indices(" + tableName + ");"));
				for (String c : colNamesSet) {
					if (confirmedIndices.contains(c)) {
						columnIndexSet.add(tableName + "+++" + c);
					}
				}
				long end = System.currentTimeMillis();
				logger.info("TIME FOR R INDEX CREATION = " + (end - start) + " ms");
			} catch (Exception e) {
				logger.info("ERROR WITH R INDEX !!! " + rIndex);
				e.printStackTrace();
			}
			
		}
	}
	
	/*
	 * Wrappers around existing emthods in rJavaTranslator
	 */
	
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		return this.rJavaTranslator.getDataRow(rScript, headerOrdering);
	}
	
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		return this.rJavaTranslator.getBulkDataRow(rScript, headerOrdering);
	}

	public boolean isEmpty() {
		return this.rJavaTranslator.isEmpty(this.dataTableName);
	}
	
	public boolean isEmpty(String frameName) {
		return this.rJavaTranslator.isEmpty(frameName);
	}

	public int getNumRows() {
		return getNumRows(this.dataTableName);
	}
	
	public int getNumRows(String varName) {
		return this.rJavaTranslator.getNumRows(varName);
	}
	
	public String[] getColumnNames() {
		return getColumnNames(this.dataTableName);
	}

	public String[] getColumnNames(String varName) {
		return this.rJavaTranslator.getColumns(varName);
	}

	public String[] getColumnTypes() {
		return getColumnTypes(this.dataTableName);
	}
	
	public String[] getColumnTypes(String varName) {
		return this.rJavaTranslator.getColumnTypes(varName);
	}

	protected RConnection getConnection() {
		if(this.rJavaTranslator instanceof RJavaRserveTranslator) {
			return ((RJavaRserveTranslator) this.rJavaTranslator).getConnection();
		}
		return null;
	}

	protected String getPort() {
		if(this.rJavaTranslator instanceof RJavaRserveTranslator) {
			return ((RJavaRserveTranslator) this.rJavaTranslator).getPort();
		}
		return null;
	}
	
	
//	/**
//	 * Modify columns to make sure they are numeric for math operations
//	 * @param typesMap
//	 */
//	private void alterColumnsToNumeric(String tableName, Map<SemossDataType, List<String>> typesMap) {	
//		if (typesMap.containsKey(SemossDataType.INT) || typesMap.containsKey(SemossDataType.DOUBLE)) {
//			List<String> columns = new ArrayList<String>();
//			if (typesMap.get(SemossDataType.INT) != null && typesMap.get(SemossDataType.INT).size() > 0) {
//				columns.addAll(typesMap.get(SemossDataType.INT));
//			}
//			if (typesMap.get(SemossDataType.DOUBLE) != null && typesMap.get(SemossDataType.DOUBLE).size() > 0) {
//				columns.addAll(typesMap.get(SemossDataType.DOUBLE));
//			}
//			evalR( addTryEvalToScript ( RSyntaxHelper.alterColumnTypeToNumeric(tableName, columns) ) );
//		}
//	}
//	
//
//	/**
//	 * 	Modify columns to make sure they are date or datetime types
//	 * @param tableName
//	 * @param typesMap
//	 * @param javaDateFormatMap
//	 */
//	private void alterColumnsToDate(String tableName, Map<SemossDataType, List<String>> typesMap, Map<String, String> javaDateFormatMap) {
//		if (typesMap.containsKey(SemossDataType.DATE)) {
//			Map<String, List<String>> rJavaDateFormatMap = new HashMap<String, List<String>>();
//			//translate java date format to R syntax and aggregate applicable columns
//			for (String header: typesMap.get(SemossDataType.DATE)){
//				String javaFormat = javaDateFormatMap.get(header);
//				String rFormat = RSyntaxHelper.translateJavaRDateTimeFormat(javaFormat);
//				rJavaDateFormatMap.computeIfAbsent(rFormat, v -> new ArrayList<String>());
//				rJavaDateFormatMap.get(rFormat).add(header);
//			}
//			//for each r format of date, convert column appropriately
//			for (String key : rJavaDateFormatMap.keySet()) {
//				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToDate(tableName, key, rJavaDateFormatMap.get(key)) ) );
//			}
//		} 
//		
//		if (typesMap.containsKey(SemossDataType.TIMESTAMP)) {
//			Map<String, List<String>> rJavaTSFormatMap = new HashMap<String, List<String>>();
//			//translate java timestamp format to R syntax and aggregate applicable columns
//			for (String header: typesMap.get(SemossDataType.TIMESTAMP)){
//				String javaFormat = javaDateFormatMap.get(header);
//				String rFormat = RSyntaxHelper.translateJavaRDateTimeFormat(javaFormat);
//				rJavaTSFormatMap.computeIfAbsent(rFormat, v -> new ArrayList<String>());
//				rJavaTSFormatMap.get(rFormat).add(header);
//			}
//			//for each r format of timestamp, convert column appropriately
//			for (String key : rJavaTSFormatMap.keySet()) {
//				this.rJavaTranslator.runR( RSyntaxHelper.alterColumnTypeToDateTime(tableName, key, rJavaTSFormatMap.get(key)) );
//			}
//		}
//	}
//	/**
//	 * Modify columns to make sure they are chars
//	 * @param tableName
//	 * @param typesMap
//	 */
//	private void alterColumnsToChars(String tableName, Map<SemossDataType, List<String>> typesMap) {
//		if (typesMap.containsKey(SemossDataType.STRING)) {
//			List<String> columns = typesMap.get(SemossDataType.STRING);
//			evalR( addTryEvalToScript ( RSyntaxHelper.alterColumnTypeToCharacter(tableName, columns) ) );
//		}
//	}
	
}
