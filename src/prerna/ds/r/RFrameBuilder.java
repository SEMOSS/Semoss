package prerna.ds.r;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.util.FileUtils;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.ds.util.flatfile.ParquetFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.IStringExportProcessor;
import prerna.om.Insight;
import prerna.poi.main.HeadersException;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.ParquetQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.rdf.engine.wrappers.RawRSelectWrapper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaRserveTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaUserRserveTranslator;
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
	protected boolean isInMem = true;
	
	public RFrameBuilder(AbstractRJavaTranslator rJavaTranslator) {
		this.rJavaTranslator = rJavaTranslator;
		this.rJavaTranslator.startR(); 
	}

	public RFrameBuilder(AbstractRJavaTranslator rJavaTranslator, String dataTableName) {
		this(rJavaTranslator);
		this.dataTableName = dataTableName;
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
	
	/**
	 * Getter for the translator
	 * @return
	 */
	public AbstractRJavaTranslator getRJavaTranslator() {
		return this.rJavaTranslator;
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
	 * Creates a new data table from an iterator
	 * @param it					The iterator to flush into a r data table
	 * @param typesMap				The data type of each column
	 */
	public void createTableViaIterator(String tableName, Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap) {
		Map<String, String> additionalType = new HashMap<String,String>();
		String fileType = "";
		/*
		 * We have an iterator that comes for 3 main sources
		 * 1) some kind of resultset (i.e. engine/endpoint) -> we flush this out to a csv file and load it
		 * 2) an iterator for a csv file
		 * 3) an iterator for a single sheet of an excel file (later we will figure out multi sheet excels...)
		 */
		
		boolean loaded = false;
		if(it instanceof RawRSelectWrapper) {
			// TODO: THIS DOESN'T WORK ON THE SERVER SINCE RSERVE MAYBE RUNNING ON DIFFERENT PORTS
			// ADD BACK ONCE HAVE THE PROPER CHECKS
//			RawRSelectWrapper rIterator = (RawRSelectWrapper) it;
//			SelectQueryStruct qs = rIterator.getOutput().getQs();
//			if(qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE) { 
//				// if we have a small limit
//				// write to new file
//				// in case the variable size is really large and the IO 
//				// still produces better performance
//				// TODO: determine optimal number for this...
//				if(qs == null || qs.getLimit() == -1 || qs.getLimit() > 10_000) {
//					RNativeEngine engine = (RNativeEngine) rIterator.getEngine();
//					engine.directLoad(this.rJavaTranslator, tableName, rIterator.getTempVariableName());
//					loaded = true;
//					if(qs != null && (qs.getLimit() > 0 || qs.getOffset() > 0)) {
//						int numRows = getNumRows(tableName);
//						evalR(RSyntaxHelper.determineLimitOffsetSyntax(tableName, numRows, qs.getLimit(), qs.getOffset()));
//					}
//				}
//			}
		} else if(it instanceof CsvFileIterator) {
			CsvQueryStruct csvQs = ((CsvFileIterator) it).getQs();
			if(csvQs.getLimit() == -1 || csvQs.getLimit() > 10_000) {
				createTableViaCsvFile(tableName, (CsvFileIterator) it);
				additionalType = ((CsvFileIterator) it).getQs().getAdditionalTypes();
				fileType = "csv";
				loaded = true;
			}
		} else if(it instanceof ExcelSheetFileIterator ) {
			ExcelQueryStruct qs = ((ExcelSheetFileIterator)it).getQs();
			String sheetName = qs.getSheetName();
			String filePath = qs.getFilePath();
			String sheetRange = qs.getSheetRange();
			// load sheet
			this.rJavaTranslator.runR(RSyntaxHelper.loadExcelSheet(filePath, tableName, sheetName, sheetRange));
			// clean headers
			String[] colNames = this.rJavaTranslator.getColumns(tableName);
			StringBuilder script = new StringBuilder();
			script.append(RSyntaxHelper.cleanFrameHeaders(tableName, colNames));
			// set new header names for frame
			Map<String, String> newHeaders = qs.getNewHeaderNames();
			for (String oldHeader : newHeaders.keySet()) {
				String newHeader = newHeaders.get(oldHeader);
				script.append(RSyntaxHelper.alterColumnName(tableName, oldHeader, newHeader));
			}
			this.rJavaTranslator.runR(script.toString());
			fileType = "excel";
			loaded = true;
		} else if(it instanceof ParquetFileIterator) {
			ParquetQueryStruct qs = ((ParquetFileIterator) it).getQs();
			String filePath = qs.getFilePath();
			// load parquet file
			this.rJavaTranslator.runR(RSyntaxHelper.loadParquetFile(filePath, tableName));
			
			// clean headers
			String[] colNames = this.rJavaTranslator.getColumns(tableName);
			StringBuilder script = new StringBuilder();
			
			// apply limit for import
			long limit = qs.getLimit();
			if (limit > -1) {
				String rowLimits = String.valueOf(limit);
				script.append(tableName + "<-" + tableName + "[1:" + rowLimits + ",];");
			}
			script.append(RSyntaxHelper.cleanFrameHeaders(tableName, colNames));
			
			// set new header names for frame
			Map<String, String> newHeaders = qs.getNewHeaderNames();
			for (String newHeader : newHeaders.keySet()) {
				String oldHeader = newHeaders.get(newHeader);
				script.append(RSyntaxHelper.alterColumnName(tableName, oldHeader, newHeader));
			}
			this.rJavaTranslator.runR(script.toString());
			fileType = "parquet";
			loaded = true;
		}
		
		if(!loaded) {
			// default behavior is to just write this to a csv file
			// get the fread() notation for that csv file
			// and read it back in

			String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";

			if(Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
				Insight in = this.getRJavaTranslator().getInsight();
				
				String insightFolder = this.getRJavaTranslator().getInsight().getInsightFolder();
			
				try {
					FileUtils.mkdirs(new File(insightFolder), true);
					if(in.getUser() != null) {
						in.getUser().getUserMountHelper().mountFolder(this.getRJavaTranslator().getInsight().getInsightFolder(),this.getRJavaTranslator().getInsight().getInsightFolder(), false);
					}
					newFileLoc = insightFolder + "/" + Utility.getRandomString(6) + ".tsv";
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			File newFile = Utility.writeResultToFile(newFileLoc, it, typesMap, "\t", new IStringExportProcessor() {
				// for fread - we need to replace all inner quotes with ""
				@Override
				public String processString(String input) {
					return input.replace("\"", "\"\"");
				}
			});
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(tableName, newFile.getAbsolutePath(), "\\t");
			evalR(loadFileRScript);
			//sudo do.delete();
			
//			// check that the variable exists
//			if(isEmpty(tableName)) {
//				throw new EmptyIteratorException("No data found to import");
//			}
			
			// update the headers to be cleaned
			if(it instanceof IRawSelectWrapper) {
				String[] headers = ((IRawSelectWrapper) it).getHeaders();
				String[] cleanHeaders = HeadersException.getInstance().getCleanHeaders(headers);
				String modHeaders = RSyntaxHelper.alterColumnNames(tableName, headers, cleanHeaders);
				evalR(modHeaders);
			} else if(it instanceof BasicIteratorTask) {
				List<Map<String, Object>> taskHeaders = ((BasicIteratorTask) it).getHeaderInfo();
				int numHeaders = taskHeaders.size();
				String[] headers = new String[numHeaders];
				for(int i = 0; i < numHeaders; i++) {
					Map<String, Object> headerInfo = taskHeaders.get(i);
					String alias = (String) headerInfo.get("alias");
					headers[i] = alias;
				}
				String[] cleanHeaders = HeadersException.getInstance().getCleanHeaders(headers);
				String modHeaders = RSyntaxHelper.alterColumnNames(tableName, headers, cleanHeaders);
				evalR(modHeaders);
			}
		}
		
		// alter types
		alterColumnTypes(tableName, typesMap, additionalType, fileType);
		
		//add indices
		addColumnIndex(tableName, typesMap.keySet().toArray(new String[typesMap.size()]));
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

	private SelectQueryStruct updateFileSelectors(SelectQueryStruct qs, String tableName, String[] colNames) {
		for (int i = 0; i < colNames.length; i++) {
			qs.addSelector(tableName, colNames[i]);
		}
		return qs;
	}
	
	private SimpleQueryFilter updateFilter(String tableName, SimpleQueryFilter filter) {
		SimpleQueryFilter newFilter = null;
		FILTER_TYPE fType = filter.getSimpleFilterType();
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
	private void alterColumnTypes(String tableName, Map<String, SemossDataType> typesMap, Map<String, String> javaDateFormatMap, String fileType) {
		// go through all the headers
		// and collect similar types
		// so we can execute with a single r script line
		// for performance improvements
		List<String> charColumns = new Vector<String>();
		List<String> intColumns = new Vector<String>();
		List<String> doubleColumns = new Vector<String>();
		List<String> booleanColumns = new Vector<String>();

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
			} else if(type == SemossDataType.BOOLEAN) {
				booleanColumns.add(header);
			} else if(type == SemossDataType.DATE) {
				String format = javaDateFormatMap.get(header);
				if(format == null) {
					format = "yyyy-MM-dd";
				}
				if(datesMap.containsKey(format)) {
					// add to existing list
					datesMap.get(format).add(header);
				} else {
					List<String> headerList = new Vector<String>();
					headerList.add(header);
					datesMap.put(format, headerList);
				}
			} else if( type == SemossDataType.TIMESTAMP) {
				String format = javaDateFormatMap.get(header);
				if(format == null) {
					format = "yyyy-MM-dd HH:mm:ss.SSS";
				}
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
		// execute type modifications
		if(!charColumns.isEmpty()) {
			evalR( RSyntaxHelper.alterColumnTypeToCharacter(tableName, charColumns) );
			evalR( RSyntaxHelper.replaceNAString(tableName, charColumns) );
		}
		if(!intColumns.isEmpty()) {
			evalR( RSyntaxHelper.alterColumnTypeToInteger(tableName, intColumns) );
		}
		if(!doubleColumns.isEmpty()) {
			evalR( RSyntaxHelper.alterColumnTypeToNumeric(tableName, doubleColumns) );
		}
		if(!booleanColumns.isEmpty()) {
			evalR( RSyntaxHelper.alterColumnTypeToBoolean(tableName, booleanColumns) );
		}
		
		// seeing dates are now loader as the proper data type and not as numbers...
		// if the original file type is excel, then need to assess if there are date/time cols that have been parsed to numbers first 
		// and handle those separately
//		if (fileType.equals("excel")) {
//			//handle date numbers
//			if (!datesMap.isEmpty()) {
//				List<String> dateHeaders = new ArrayList<String>();
//				datesMap.values().forEach(dateHeaders::addAll);
//				List<String> dateExcelR = RSyntaxHelper.alterColumnTypeToDate_Excel(tableName, dateHeaders);
//				this.rJavaTranslator.runR(dateExcelR.get(0));
//				//retrieve cols have been converted to Date type
//				if (this.rJavaTranslator.getInt("length(" + dateExcelR.get(1) + ")") > 0) {
//					excelDateNumHeaders.addAll(Arrays.asList(this.rJavaTranslator.getStringArray(dateExcelR.get(1))));
//				}
//				//clean up the handledcol var in R
//				this.rJavaTranslator.runR("rm(" + dateExcelR.get(1) + ";gc();");
//			}
//			//handle datetime numbers
//			if (!dateTimeMap.isEmpty()) {
//				List<String> dateTimeHeaders = new ArrayList<String>();
//				dateTimeMap.values().forEach(dateTimeHeaders::addAll);
//				//TODO track millisecond digits
//				List<String> dateTimeExcelR = RSyntaxHelper.alterColumnTypeToDateTime_Excel(tableName, dateTimeHeaders);
//				for(int i = 0; i < dateTimeExcelR.size(); i++) {
//					System.out.println(dateTimeExcelR.get(i));
//				}
//				this.rJavaTranslator.runR(dateTimeExcelR.get(0));
//				//retrieve cols have been converted to Date/Time type
//				if (this.rJavaTranslator.getInt("length(" + dateTimeExcelR.get(1) + ")") > 0) {
//					excelDTNumHeaders.addAll(Arrays.asList(this.rJavaTranslator.getStringArray(dateTimeExcelR.get(1))));
//				}
//				//clean up the handledcol var in R
//				this.rJavaTranslator.runR("rm(" + dateTimeExcelR.get(1) + ";gc();");
//			}
//		}
		
		// loop through normal dates
		for(String format : datesMap.keySet()) {
			List<String> dateHeaders = datesMap.get(format);
			if (!dateHeaders.isEmpty()){
				String rFormat = RSyntaxHelper.translateJavaRDateTimeFormat(format);
				this.rJavaTranslator.runR( RSyntaxHelper.alterColumnTypeToDate(tableName, rFormat, dateHeaders) ) ;
			}
		}
		// excel reading already loads as POSIXct types
		// so no need to modify again
		// TODO: need to handle strings that we are trying to parse as timestamps?
		if(!fileType.equals("excel")) {
			// loop through time stamps dates
			if(isEmpty(tableName)) {
				for(String format : dateTimeMap.keySet()) {
					List<String> dateTimeHeaders = dateTimeMap.get(format);
					if (!dateTimeHeaders.isEmpty()){
						this.rJavaTranslator.runR( RSyntaxHelper.alterEmptyTableColumnTypeToDateTime(tableName, dateTimeHeaders) );
					}
				}
			} else {
				for(String format : dateTimeMap.keySet()) {
					List<String> dateTimeHeaders = dateTimeMap.get(format);
					if (!dateTimeHeaders.isEmpty()){
						String rFormat = RSyntaxHelper.translateJavaRDateTimeFormat(format);
						this.rJavaTranslator.runR( RSyntaxHelper.alterColumnTypeToDateTime(tableName, rFormat, dateTimeHeaders) );
					}
				}
			}
		}
	}
	
	protected void addColumnIndex(String tableName, String colName) {
		if (!columnIndexSet.contains(tableName + "+++" + colName)) {
			long start = System.currentTimeMillis();
			String rIndex = null;
			logger.info("Generating index on R Data Table on column = " + colName);
			logger.debug("CREATING INDEX ON R TABLE = " + tableName + " ON COLUMN = " + colName);
			try {
				rIndex = "CREATE INDEX ON " + tableName + "(" + colName + ")";
				this.rJavaTranslator.executeEmptyR("setindex(" + tableName + "," + colName + ");");
				String[] confirmedIndicesArr = this.rJavaTranslator.getStringArray("indices(" + tableName + ");");
				if(confirmedIndicesArr != null) {
					List<String> confirmedIndices = Arrays.asList(confirmedIndicesArr);
					if (confirmedIndices.contains(colName)) {
						columnIndexSet.add(tableName + "+++" + colName);		
					}
					long end = System.currentTimeMillis();
					logger.debug("TIME FOR R INDEX CREATION = " + (end - start) + " ms");
					logger.info("Finished generating indices on R Data Table on column = " + colName);
				} else {
					logger.info("Encountered issue with generating indices on R Data Table on column = " + colName);
				}
			} catch (Exception e) {
				logger.debug("ERROR WITH R INDEX !!! " + rIndex);
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
			logger.info("Generating index on R Data Table on columns = " + Utility.cleanLogString(StringUtils.join(colNamesSet,", ")));
			logger.debug("CREATING INDEX ON R TABLE = " + tableName + " ON COLUMN(S) = " + StringUtils.join(colNamesSet,", "));
			try {
				rIndex = "CREATE INDEX ON " + tableName + "(" + StringUtils.join(colNamesSet,", ") + ")";
				this.rJavaTranslator.executeEmptyR("invisible(lapply(c('" + StringUtils.join(colNamesSet,"','") + "')" + ", setindexv, x= " + tableName + "));");
				
				// get the current indices
				List<String> confirmedIndices = null;
				String[] indices = this.rJavaTranslator.getStringArray("indices(" + tableName + ");");
				if(indices != null && indices.length > 0) {
					confirmedIndices = Arrays.asList(indices);
				} else {
					confirmedIndices = new Vector<String>();
				}
				
				// add if not a current index
				for (String c : colNamesSet) {
					if (confirmedIndices.contains(c)) {
						columnIndexSet.add(tableName + "+++" + c);
					}
				}
				
				long end = System.currentTimeMillis();
				logger.debug("TIME FOR R INDEX CREATION = " + (end - start) + " ms");
				logger.info("Finished generating indices on R Data Table on columns = " + Utility.cleanLogString(StringUtils.join(colNamesSet,", ")));
			} catch (Exception e) {
				logger.debug("ERROR WITH R INDEX !!! " + rIndex);
				e.printStackTrace();
			}
			
		}
	}
	
	public void removeAllColumnIndex() {
		this.columnIndexSet.clear();
	}
	
	public void dropTable() {
		evalR("rm(" + this.dataTableName + ")");
		evalR("gc()");
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
	
	public int getFrameSize(String varName) {
		return this.rJavaTranslator.getInt("nrow(" + varName + ") * ncol(" + varName + ");");
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
		} else if(this.rJavaTranslator instanceof RJavaUserRserveTranslator){
			return ((RJavaUserRserveTranslator) this.rJavaTranslator).getConnection();
		}
		return null;
	}

	protected String getPort() {
		if(this.rJavaTranslator instanceof RJavaRserveTranslator) {
			return ((RJavaRserveTranslator) this.rJavaTranslator).getPort();
		}
		return null;
	}
	
	protected void saveRda(String frameFileName, String frameName) {
		this.evalR("save(" + frameName + ", file=\"" + frameFileName.replace("\\", "/") + "\")");
		if (new File(frameFileName).length() == 0){
			throw new IllegalArgumentException("Attempting to save an empty R frame");
		}
	}
	
	protected void openRda(String frameFileName){
		this.evalR("load(\"" + frameFileName.replace("\\", "/") + "\")");
	}
	
	protected void saveFst(String frameFileName, String frameName) {
		this.evalR("library(\"fst\")");
		this.evalR("write_fst(" + frameName + ", \"" + frameFileName.replace("\\", "/") + "\")");
		if (new File(Utility.normalizePath(frameFileName)).length() == 0){
			throw new IllegalArgumentException("Attempting to save an empty R frame");
		}
	}
	
	protected void openFst(String frameFileName, String frameName){
		this.evalR("library(\"fst\")");
		// 2020-01-02 
		// newer version of fst library shouldn't require the additional "as.data.table" syntax
		// https://github.com/fstpackage/fst/milestone/23
		this.evalR(frameName + " <- as.data.table(read_fst(\"" + frameFileName.replace("\\", "/") + "\"))");
	}
	
}
