package prerna.ds.r;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.CsvFileIterator;
import prerna.ds.util.ExcelFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.interpreters.RInterpreter2;
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
		/*
		 * We have an iterator that comes for 3 main sources
		 * 1) some kind of resultset (i.e. engine/endpoint) -> we flush this out to a csv file and load it
		 * 2) an iterator for a csv file
		 * 3) an iterator for a single sheet of an excel file (later we will figure out multi sheet excels...)
		 */
		if(it instanceof CsvFileIterator) {
			createTableViaCsvFile(tableName, (CsvFileIterator) it);
		} else if(it instanceof ExcelFileIterator) {
			createTableViaExcelFile(tableName, (ExcelFileIterator) it);
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
		
		// modify columns such that they are chars where needed
		// this is because an int id may need to be convered to string
		alterColumnsToChars(tableName, typesMap);
		// modify columns such that they are numeric where needed
		alterColumnsToNumeric(tableName, typesMap);
		//modify columns such that they are date where needed
		alterColumnsToDate(tableName, typesMap);
	}
	
	private void createTableViaCsvFile(String tableName, CsvFileIterator it) {
		CsvQueryStruct qs = it.getQs();
		String[] newCleanHeaders = it.getHelper().getAllCSVHeaders();
		{
			long start = System.currentTimeMillis();
			logger.info("Loading R table via CSV File");
			// get you the fread notation with the csv file within the iterator
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(tableName, it.getFileLocation(), it.getDelimiter() + "");
			evalR(loadFileRScript);

			// fread will use the original headers, even if there are duplicates
			// we need to fix this -> grab the new ones and write it out

			evalR("setnames(" + tableName + ", " + RSyntaxHelper.createStringRColVec(newCleanHeaders) + ")");
			long end = System.currentTimeMillis();
			logger.info("Loading R done in " + (end-start) + "ms");
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
			RInterpreter2 interp = new RInterpreter2();
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
			System.out.println(it.getFileLocation());
			evalR(loadFileRScript);

			evalR("setnames(" + tableName + ", " + RSyntaxHelper.createStringRColVec(newCleanHeaders) + ")");
			long end = System.currentTimeMillis();
			logger.info("Loading R done in " + (end-start) + "ms");
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
			RInterpreter2 interp = new RInterpreter2();
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
	 * Modify columns to make sure they are numeric for math operations
	 * @param typesMap
	 */
	private void alterColumnsToNumeric(String tableName, Map<String, SemossDataType> typesMap) {
		for(String header : typesMap.keySet()) {
			SemossDataType type = typesMap.get(header);
			if(type == SemossDataType.INT || type == SemossDataType.DOUBLE) {
				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToNumeric(tableName, header) ) );
			}
		}
	}
	
	/**
	 * Modify columns to make sure they are in the date format
	 * @param typesMap
	 */
	private void alterColumnsToDate(String tableName, Map<String, SemossDataType> typesMap) {
		for(String header : typesMap.keySet()) {
			SemossDataType type = typesMap.get(header);
			if(type == SemossDataType.DATE) {
				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToDate(tableName, header) ) );
			} else if(type == SemossDataType.TIMESTAMP) {
				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToDateTime(tableName, header) ) );
			}
		}
	}
	
	/**
	 * Modify columns to make sure they are chars
	 * @param tableName
	 * @param typesMap
	 */
	private void alterColumnsToChars(String tableName, Map<String, SemossDataType> typesMap) {
		for(String header : typesMap.keySet()) {
			SemossDataType type = typesMap.get(header);
			if(type == SemossDataType.STRING) {
				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToCharacter(tableName, header) ) );
			}
		}
	}
	
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
	
}
