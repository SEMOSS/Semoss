package prerna.ds.R;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerMetaData;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RFileWrapper;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;

public class RDataTable extends AbstractTableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(RDataTable.class.getName());
	public static final String R_DATA_FRAME = "RDataFrame";
	
	private RBuilder builder;
	
	public RDataTable() {
		try {
			this.builder = new RBuilder();
			this.metaData = new TinkerMetaData();
		} catch (RserveException e) {
			e.printStackTrace();
			throw new IllegalStateException("Could not create valid connection to R. "
					+ "Please make sure R is installed properly and running on machine.");
		}
	}
	
	public RConnection getConnection() {
		return this.builder.getConnection();
	}
	
	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.RImportDataReactor");
//		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");

		//TODO: need to go through and modify these things so they are not H2 specific
		
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.MATH_PARAM, "prerna.sablecc.MathParamReactor");
		reactorNames.put(PKQLEnum.CSV_TABLE, "prerna.sablecc.CsvTableReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
		reactorNames.put(PKQLEnum.COL_SPLIT, "prerna.sablecc.H2ColSplitReactor");
		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
		reactorNames.put(PKQLEnum.OPEN_DATA, "prerna.sablecc.OpenDataReactor");
		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");
		reactorNames.put(PKQLEnum.NETWORK_CONNECT, "prerna.sablecc.ConnectReactor");
		reactorNames.put(PKQLEnum.NETWORK_DISCONNECT, "prerna.sablecc.DisConnectReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.H2DataFrameDuplicatesReactor");

		reactorNames.put(PKQLEnum.SUM, "prerna.sablecc.expressions.r.RSumReactor");
		reactorNames.put(PKQLEnum.MAX, "prerna.sablecc.expressions.r.RMaxReactor");
		reactorNames.put(PKQLEnum.MIN, "prerna.sablecc.expressions.r.RMinReactor");
		reactorNames.put(PKQLEnum.AVERAGE, "prerna.sablecc.expressions.r.RAverageReactor");
		reactorNames.put(PKQLEnum.STANDARD_DEVIATION, "prerna.sablecc.expressions.r.RStandardDeviationReactor");
		reactorNames.put(PKQLEnum.MEDIAN, "prerna.sablecc.expressions.r.RMedianReactor");
		reactorNames.put(PKQLEnum.COUNT, "prerna.sablecc.expressions.r.RCountReactor");

		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
		reactorNames.put(PKQLEnum.CSV_API, "prerna.sablecc.RCsvApiReactor");
		reactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");
//		reactorNames.put(PKQLEnum.R_API, "prerna.sablecc.RApiReactor");

		return reactorNames;
	}
	
	public void createTableViaIterator(Iterator<IHeadersDataRow> it) {
		// we really need another way to get the data types....
		Map<String, IMetaData.DATA_TYPES> typesMap = this.metaData.getColumnTypes();
		this.builder.createTableViaIterator(it, typesMap);
	}
	
	public void createTableViaCsvFile(RFileWrapper fileWrapper) {
		this.builder.createTableViaCsvFile(fileWrapper);
	}
	
	public REXP executeRScript(String rScript) {
		return this.builder.executeR(rScript);
	}
	
	public String getROutput(String rScript) {
		return this.builder.getROutput(rScript);
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
		return this.builder.iterator(this.headerNames);
	}
	
	@Override
	public Iterator<Object[]> iterator(Map<String, Object> options) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// need to build this out
		String[] headers = this.headerNames;
		if(options.containsKey(AbstractTableDataFrame.SELECTORS)) {
			List<String> headerList = (List<String>) options.get(AbstractTableDataFrame.SELECTORS);
			headers = headerList.toArray(new String[]{});
		}
		return this.builder.iterator(headers);
	}
	
	@Override
	public boolean isEmpty() {
		return this.builder.isEmpty();
	}
	
	public String getTableVarName() {
		return this.builder.getTableName();
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

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unfilter(String columnHeader) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unfilter() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeColumn(String columnHeader) {
		// TODO Auto-generated method stub
		
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
	public void filter(String columnHeader, Map<String, List<Object>> filterValues) {
		// TODO Auto-generated method stub
		
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
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
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
	public void removeRelationship(Map<String, Object> cleanRow) {
		// TODO Auto-generated method stub
		
	}
	
}
