package prerna.ds.r;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerMetaData;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RCsvFileWrapper;
import prerna.engine.impl.r.RExcelFileWrapper;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.query.interpreters.RInterpreter2;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RDataTable extends AbstractTableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(RDataTable.class.getName());
	public static final String R_DATA_FRAME = "RDataFrame";
	
	private AbstractRBuilder builder;
	
	public RDataTable() {
		this(null);
	}
	
	public RDataTable(String rTableVarName) {
		String useJriStr = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI);
		boolean useJri = false;
		if(useJriStr != null) {
			useJri = Boolean.valueOf(useJriStr);
		}
		try {
			if(useJri) {
				this.builder = new RBuilderJRI(rTableVarName);
			} else {
				this.builder = new RBuilder(rTableVarName);
			}
			this.metaData = new TinkerMetaData();
		} catch (RserveException e) {
			e.printStackTrace();
			closeConnection();
			throw new IllegalStateException("Could not create valid connection to R. "
					+ "Please make sure R is installed properly and running on machine.");
		}
	}
	
	public RDataTable(String rTableVarName, RConnection retCon, String port) {
		try {
			this.builder = new RBuilder(rTableVarName, retCon, port);
			this.metaData = new TinkerMetaData();
		} catch (RserveException e) {
			e.printStackTrace();
			closeConnection();
			throw new IllegalStateException("Could not create valid connection to R. "
					+ "Please make sure R is installed properly and running on machine.");
		}
	}
	
	public RConnection getConnection() {
		return this.builder.getConnection();
	}
	
	public String getPort() {
		return this.builder.getPort();
	}
	
	public void closeConnection() {
		if(this.builder.getConnection() != null) {
			try {
				this.builder.getConnection().shutdown();
			} catch (RserveException e) {
				LOGGER.info("R Connection is already closed...");
			}
		}
	}
	
	@Override
	public IQueryInterpreter2 getInterpreter() {
		return new RInterpreter2();
	}
	
	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.RImportDataReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.RDuplicatesReactor");

		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
		reactorNames.put(PKQLEnum.OPEN_DATA, "prerna.sablecc.OpenDataReactor");
		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");
		reactorNames.put(PKQLEnum.NETWORK_CONNECT, "prerna.sablecc.ConnectReactor");
		reactorNames.put(PKQLEnum.NETWORK_DISCONNECT, "prerna.sablecc.DisConnectReactor");

		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.RVizReactor");

		reactorNames.put(PKQLEnum.SUM, "prerna.sablecc.expressions.r.RSumReactor");
		reactorNames.put(PKQLEnum.MAX, "prerna.sablecc.expressions.r.RMaxReactor");
		reactorNames.put(PKQLEnum.MIN, "prerna.sablecc.expressions.r.RMinReactor");
		reactorNames.put(PKQLEnum.AVERAGE, "prerna.sablecc.expressions.r.RAverageReactor");
		reactorNames.put(PKQLEnum.STANDARD_DEVIATION, "prerna.sablecc.expressions.r.RStandardDeviationReactor");
		reactorNames.put(PKQLEnum.MEDIAN, "prerna.sablecc.expressions.r.RMedianReactor");
		reactorNames.put(PKQLEnum.COUNT, "prerna.sablecc.expressions.r.RCountReactor");
		reactorNames.put(PKQLEnum.COUNT_DISTINCT, "prerna.sablecc.expressions.r.RUniqueCountReactor");

		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
		reactorNames.put(PKQLEnum.CSV_API, "prerna.sablecc.RCsvApiReactor");
		reactorNames.put(PKQLEnum.EXCEL_API, "prerna.sablecc.RExcelApiReactor");
		reactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");

		return reactorNames;
	}
	
	public String getFilterString() {
		//TODO:
		//TODO:
		return "";
	}
	
	public void createTableViaIterator(Iterator<IHeadersDataRow> it) {
		// we really need another way to get the data types....
		Map<String, IMetaData.DATA_TYPES> typesMap = this.metaData.getColumnTypes();
		this.builder.createTableViaIterator(it, typesMap);
	}
	
	public void createTableViaCsvFile(RCsvFileWrapper fileWrapper) {
		this.builder.createTableViaCsvFile(fileWrapper);
	}
	
	public void createTableViaExcelFile(RExcelFileWrapper fileWrapper) {
		this.builder.createTableViaExcelFile(fileWrapper);
	}
	
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		return this.builder.getDataRow(rScript, headerOrdering);
	}
	
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		return this.builder.getBulkDataRow(rScript, headerOrdering);
	}
	
	public Object getScalarValue(String rScript) {
		return this.builder.getScalarReturn(rScript);
	}
	
	public void executeRScript(String rScript) {
		this.builder.executeR(rScript);
	}
	
	public String[] getColumnNames() {
		return this.builder.getColumnNames();
	}
	
	public String[] getColumnTypes() {
		return this.builder.getColumnTypes();
	}
	
	public String[] getColumnNames(String varName) {
		return this.builder.getColumnNames(varName);
	}
	
	public String[] getColumnTypes(String varName) {
		return this.builder.getColumnTypes(varName);
	}
	
	@Override
	public Double getMax(String columnHeader) {
		return this.builder.executeStat(columnHeader, "max");
	}

	@Override
	public Double getMin(String columnHeader) {
		return this.builder.executeStat(columnHeader, "min");
	}

	@Override
	public Iterator<Object[]> iterator() {
		return this.builder.iterator(getColumnHeaders(), 0, 0);
	}
	
	@Override
	public Iterator<Object[]> iterator(Map<String, Object> options) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// need to build this out
		String[] headers = getColumnHeaders();
		if(options.containsKey(AbstractTableDataFrame.SELECTORS)) {
			List<String> headerList = (List<String>) options.get(AbstractTableDataFrame.SELECTORS);
			headers = headerList.toArray(new String[]{});
		}
		
		int limit = 0;
		if(options.containsKey(AbstractTableDataFrame.LIMIT)) {
			limit = (int) options.get(AbstractTableDataFrame.LIMIT);
		}
		
		int offset = 0;
		if(options.containsKey(AbstractTableDataFrame.OFFSET)) {
			offset = (int) options.get(AbstractTableDataFrame.OFFSET);
		}
		
		return this.builder.iterator(headers, limit, offset);
	}
	
	@Override
	public void removeColumn(String columnHeader) {
		this.builder.evalR(this.builder.getTableName() + "[," + columnHeader + ":=NULL]");
		this.metaData.dropVertex(columnHeader);

		// Remove the column from header names
		String[] newHeaders = new String[this.headerNames.length-1];
		int newHeaderIdx = 0;
		for(int i = 0; i < this.headerNames.length; i++){
			String name = this.headerNames[i];
			if(!name.equals(columnHeader)){
				newHeaders[newHeaderIdx] = name;
				newHeaderIdx ++;
			}
		}
		
		this.headerNames = newHeaders;
		this.updateDataId();
	}
	
	/**
	 * String columnHeader - the column on which to filter on filterValues - the
	 * values that will remain in the
	 */
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		if (filterValues != null && filterValues.size() > 0) {
			this.metaData.setFiltered(columnHeader, true);
			builder.setFilters(columnHeader, filterValues, "=");
		}
	}
	
	@Override
	public void filter(String columnHeader, Map<String, List<Object>> filterValues) {
		if(columnHeader == null || filterValues == null) return;

		DATA_TYPES type = this.metaData.getDataType(columnHeader);
		boolean isOrdinal = type != null && (type == DATA_TYPES.DATE || type == DATA_TYPES.NUMBER);


		String[] comparators = filterValues.keySet().toArray(new String[]{});
		for(int i = 0; i < comparators.length; i++) {
			String comparator = comparators[i];
			boolean override = i == 0;
			List<Object> filters = filterValues.get(comparator);

			comparator = comparator.trim();
			if(comparator.equals("=")) {

				if(override) builder.setFilters(columnHeader, filters, comparator);
				else builder.addFilters(columnHeader, filters, comparator);

			} else if(comparator.equals("!=")) { 

				if(override) builder.setFilters(columnHeader, filters, comparator);
				else builder.addFilters(columnHeader, filters, comparator);

			} else if(comparator.equals("<")) {

				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, comparator);
					else builder.addFilters(columnHeader, filters, comparator);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}

			} else if(comparator.equals(">")) {

				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, comparator);
					else builder.addFilters(columnHeader, filters, comparator);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}

			} else if(comparator.equals("<=")) {
				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, comparator);
					else builder.addFilters(columnHeader, filters, comparator);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}
			} else if(comparator.equals(">=")) {
				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, comparator);
					else builder.addFilters(columnHeader, filters, comparator);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}
			} else {
				// comparator not recognized...do equal by default? or do
				// nothing? or throw error?
			}
			this.metaData.setFiltered(columnHeader, true);
		}
	}

	@Override
	public void unfilter(String columnHeader) {
		this.metaData.setFiltered(columnHeader, false);
		builder.removeFilter(columnHeader);
	}

	@Override
	public void unfilter() {
		builder.clearFilters();
	}
	
	@Override
	public boolean isEmpty() {
		return this.builder.isEmpty();
	}
	
	public String getTableVarName() {
		return this.builder.getTableName();
	}
	
	public String getTableName() {
		return this.getTableVarName();
	}
	
	public void setTableVarName(String tableVarName) {
		this.builder.setTableName(tableVarName);
	}
	
	@Override
	public int getNumRows() {
		return this.builder.getNumRows();
	}
	
	public int getNumRows(String varName) {
		return this.builder.getNumRows(varName);
	}
	
	@Override
	public void addRow(Object[] rowCleanData) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addRow(Object[] cleanCells, String[] headers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, Map<String, Object> options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean iterateAll) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Object[] getUniqueColumn(String column) {
		StringBuilder rScript = new StringBuilder();
		rScript.append("unique(").append(this.getTableName()).append("[")
			.append(this.getFilterString()).append(",").append(column).append("])");
		
		return this.builder.getBulkSingleColumn(rScript.toString());
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getFilterModel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void save(String fileName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ITableDataFrame open(String fileName, String userId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDataMakerName() {
		return R_DATA_FRAME;
	}

	
	
	// ignore these methods for now
	// ignore these methods for now
	// ignore these methods for now
	// ignore these methods for now
	// ignore these methods for now

	@Override
	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToValMap) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// we have only had RDataTable since PKQL was introduced
		// lets not try to expand this to cover the old stuff
		// assuming only pkql is used
		long startTime = System.currentTimeMillis();
		LOGGER.info("Processing Component..................................");
		processPostTransformations(component, component.getPostTrans());
		long endTime = System.currentTimeMillis();
		LOGGER.info("Component Processed: " + (endTime - startTime) + " ms");		
	}
	
	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToValMap) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, Object[]> getFilterTransformationValues() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void addRelationship(Map<String, Object> cleanRow) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeRelationship(String[] columns, Object[] values) {
		// TODO Auto-generated method stub
		
	}
}
