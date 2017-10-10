package prerna.ds.r;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.IMetaData;
import prerna.ds.util.CsvFileIterator;
import prerna.ds.util.ExcelFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RCsvFileWrapper;
import prerna.engine.impl.r.RExcelFileWrapper;
import prerna.query.interpreters.RInterpreter2;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.GenRowFilters;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.QueryFilter;
import prerna.sablecc2.om.QueryFilter.FILTER_TYPE;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRBuilder {

	protected static final String CLASS_NAME = AbstractRBuilder.class.getName();
	protected Logger logger = LogManager.getLogger(CLASS_NAME);
	
	// holds the connection for RDataFrame to the instance of R running
	protected String dataTableName = "datatable";

	public AbstractRBuilder() {
		
	}

	public AbstractRBuilder(String dataTableName) throws RserveException {
		super();
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
	
	////////////////////////////////////////////////////////////////////
	///////////////////// Abstract Methods /////////////////////////////
	
	/**
	 * Method to run a r script and not need to process output
	 * @param r
	 */
	protected abstract void evalR(String r);
	
	protected abstract Iterator<Object[]> iterator(String[] headerNames, int i, int j);
	
	protected abstract RConnection getConnection();

	protected abstract String getPort();
	
	protected abstract Double executeStat(String columnHeader, String string);
	
	protected abstract Object executeR(String rScript);
	
	protected abstract boolean isEmpty();
	
	protected abstract int getNumRows();
	
	protected abstract int getNumRows(String varName);
	
	protected abstract Object[] getDataRow(String rScript, String[] headerOrdering);
	
	protected abstract List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering);
	
	protected abstract Object[] getBulkSingleColumn(String rScript);
	
	protected abstract Object getScalarReturn(String rScript);

	protected abstract String[] getColumnNames();

	protected abstract String[] getColumnNames(String varName);

	protected abstract String[] getColumnTypes();
	
	protected abstract String[] getColumnTypes(String varName);
	
	protected abstract String[] getColumnType(String varName);
	
	protected abstract int getIntFromScript(String rScript);
	
	/**
	 * Wrap the R script in a try-eval in order to get the same error message that a user would see if using
	 * the R console
	 * @param rscript			The R script to execute
	 * @return					The R script wrapped in a try-eval statement
	 */
	protected String addTryEvalToScript(String rscript) {
		return "try(eval(" + rscript + "), silent=FALSE)";
	}

	/**
	 * Creates a new data table from an iterator
	 * @param it					The iterator to flush into a r data table
	 * @param typesMap				The data type of each column
	 */
	protected void createTableViaIterator(String tableName, Iterator<IHeadersDataRow> it, Map<String, IMetaData.DATA_TYPES> typesMap) {
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
			File newFile = Utility.writeResultToFile(newFileLoc, it, typesMap);
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(tableName, newFile.getAbsolutePath());
			evalR(loadFileRScript);
			newFile.delete();
		}
		
		// ... and make sure you update the types after!
		// the types map will have the finalized headers to their types (?) TODO check this, if not true, need to do this in csv/excel iterators
		// modify columns such that they are numeric where needed
		alterColumnsToNumeric(typesMap);
		//modify columns such that they are date where needed
		alterColumnsToDate(typesMap);
	}
	
	private void createTableViaCsvFile(String tableName, CsvFileIterator it) {
		CsvQueryStruct qs = it.getQs();
		String[] newCleanHeaders = it.getHelper().getAllCSVHeaders();
		{
			long start = System.currentTimeMillis();
			logger.info("Loading R table via CSV File");
			// get you the fread notation with the csv file within the iterator
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(tableName, it.getFileLocation());
			evalR(loadFileRScript);

			// fread will use the original headers, even if there are duplicates
			// we need to fix this -> grab the new ones and write it out

			evalR("setnames(" + tableName + ", " + RSyntaxHelper.createStringRColVec(newCleanHeaders) + ")");
			long end = System.currentTimeMillis();
			logger.info("Loading R done in " + (end-start) + "ms");
		}
		if(!qs.getFilters().isEmpty()) {
			long start = System.currentTimeMillis();
			logger.info("Need to filter R table based on QS");
			// we need to execute a script to modify the table to only contain the data based on the filters defined
			//create a new querystruct object that will have header names in the format used for RInterpreter
			QueryStruct2 modifiedQs = new QueryStruct2();
			updateFileSelectors(modifiedQs, tableName, newCleanHeaders);

			//add filters to the new qs
			GenRowFilters gFilters = qs.getFilters();
			for (int i = 0; i < gFilters.getFilters().size(); i++) {
				QueryFilter singleFilter = gFilters.getFilters().get(i);
				QueryFilter updatedFilter = updateFilter(tableName, singleFilter);
				modifiedQs.addFilter(updatedFilter);
			}
			RInterpreter2 interp = new RInterpreter2();
			interp.setDataTableName(tableName);
			interp.setQueryStruct(modifiedQs);
			Map<String, String> strTypes = qs.getColumnTypes();
			Map<String, IMetaData.DATA_TYPES> enumTypes = new HashMap<String, IMetaData.DATA_TYPES>();
			for(String key : strTypes.keySet()) {
				enumTypes.put(key, Utility.convertStringToDataType(strTypes.get(key)));
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
		if(!qs.getFilters().isEmpty()) {
			long start = System.currentTimeMillis();
			logger.info("Need to filter R table based on QS");
			// we need to execute a script to modify the table to only contain the data based on the filters defined
			//create a new querystruct object that will have header names in the format used for RInterpreter
			QueryStruct2 modifiedQs = new QueryStruct2();
			updateFileSelectors(modifiedQs, tableName, newCleanHeaders);

			//add filters to the new qs
			GenRowFilters gFilters = qs.getFilters();
			for (int i = 0; i < gFilters.getFilters().size(); i++) {
				QueryFilter singleFilter = gFilters.getFilters().get(i);
				QueryFilter updatedFilter = updateFilter(tableName, singleFilter);
				modifiedQs.addFilter(updatedFilter);
			}
			RInterpreter2 interp = new RInterpreter2();
			interp.setDataTableName(tableName);
			interp.setQueryStruct(modifiedQs);
			Map<String, String> strTypes = qs.getColumnTypes();
			Map<String, IMetaData.DATA_TYPES> enumTypes = new HashMap<String, IMetaData.DATA_TYPES>();
			for(String key : strTypes.keySet()) {
				enumTypes.put(key, Utility.convertStringToDataType(strTypes.get(key)));
			}
			interp.setColDataTypes(enumTypes);
			String query = interp.composeQuery();
			evalR(tableName + "<-" + query);
			long end = System.currentTimeMillis();
			logger.info("Done filter R table in " + (end-start) + "ms");
		}
	}

	private QueryStruct2 updateFileSelectors(QueryStruct2 qs, String tableName, String[] colNames) {
		for (int i = 0; i < colNames.length; i++) {
			qs.addSelector(tableName, colNames[i]);
		}
		return qs;
	}
	
	private QueryFilter updateFilter(String tableName, QueryFilter filter) {
		QueryFilter newFilter = null;
		FILTER_TYPE fType = QueryFilter.determineFilterType(filter);
		if(fType == FILTER_TYPE.COL_TO_COL) {
			//change both left comparator and right comparator
			String lHeader = filter.getLComparison().getValue().toString();
			NounMetadata lNoun = new NounMetadata(tableName + "__" + lHeader, PixelDataType.COLUMN);
			String rHeader = filter.getRComparison().getValue().toString();
			NounMetadata rNoun = new NounMetadata(tableName + "__" + rHeader, PixelDataType.COLUMN);
			newFilter = new QueryFilter(lNoun, filter.getComparator() , rNoun);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			//change only the left comparator
			String lHeader = filter.getLComparison().getValue().toString();
			NounMetadata lNoun = new NounMetadata(tableName + "__" + lHeader, PixelDataType.COLUMN);
			newFilter = new QueryFilter(lNoun, filter.getComparator(), filter.getRComparison());
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			//change only the right comparator
			String rHeader = filter.getRComparison().getValue().toString();
			NounMetadata rNoun = new NounMetadata(tableName + "__" + rHeader, PixelDataType.COLUMN);
			newFilter = new QueryFilter(filter.getLComparison(), filter.getComparator() , rNoun);
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
		}
		return newFilter;
	}
	
	/**
	 * Loads a file as the data table
	 * @param fileWrapper			RFileWrapper used to contain the required information for the load
	 */
	protected void createTableViaCsvFile(RCsvFileWrapper fileWrapper) {
		String loadFileRScript = RSyntaxHelper.getFReadSyntax(this.dataTableName, fileWrapper.getFilePath());
		evalR(loadFileRScript);
		
		// since we clean headers
		// we need to fix the headers from the csv file to match those which are good
		// thankfully the index is the same
		evalR("setnames(" + this.dataTableName + ", " + fileWrapper.getModHeadersRVec() + ")");
		
		// this will modify the csv to contain the specified columns and rows based on selectors and filters
		String filterScript = fileWrapper.getRScript();
		if(!filterScript.isEmpty()) {
			String modifyTableScript = this.dataTableName + "<- " + filterScript;
			evalR(modifyTableScript);
		}
		// now modify column types to ensure they are all good
		alterColumnsToNumeric(fileWrapper.getDataTypes());
		alterColumnsToDate(fileWrapper.getDataTypes());
	}
	
	protected void createTableViaExcelFile(RExcelFileWrapper fileWrapper) {
		String loadFileRScript = RSyntaxHelper.getExcelReadSheetSyntax(this.dataTableName, fileWrapper.getFilePath(), fileWrapper.getSheetName());
		evalR(loadFileRScript);
		
		// since we clean headers
		// we need to fix the headers from the csv file to match those which are good
		// thankfully the index is the same
		evalR("setnames(" + this.dataTableName + ", " + fileWrapper.getModHeadersRVec() + ")");
		
		// this will modify the csv to contain the specified columns and rows based on selectors and filters
		String filterScript = fileWrapper.getRScript();
		if(!filterScript.isEmpty()) {
			String modifyTableScript = this.dataTableName + "<- " + filterScript;
			evalR(modifyTableScript);
		}
		// now modify column types to ensure they are all good
		alterColumnsToNumeric(fileWrapper.getDataTypes());
		//modify columns such that they are date where needed
		alterColumnsToDate(fileWrapper.getDataTypes());
	}

	/**
	 * Modify columns to make sure they are numeric for math operations
	 * @param typesMap
	 */
	private void alterColumnsToNumeric(Map<String, IMetaData.DATA_TYPES> typesMap) {
		for(String header : typesMap.keySet()) {
			IMetaData.DATA_TYPES type = typesMap.get(header);
			if(type == IMetaData.DATA_TYPES.NUMBER) {
				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToNumeric(this.dataTableName, header) ) );
			}
		}
	}
	
	/**
	 * Modify columns to make sure they are in the date format
	 * @param typesMap
	 */
	private void alterColumnsToDate(Map<String, IMetaData.DATA_TYPES> typesMap) {
		for(String header : typesMap.keySet()) {
			IMetaData.DATA_TYPES type = typesMap.get(header);
			if(type == IMetaData.DATA_TYPES.DATE) {
				System.out.println(this.executeR(dataTableName + "$" + header));
				evalR( addTryEvalToScript( RSyntaxHelper.alterColumnTypeToDate(this.dataTableName, header) ) );
				System.out.println(this.executeR(dataTableName + "$" + header));
			}
		}
	}
}
