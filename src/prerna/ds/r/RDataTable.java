package prerna.ds.r;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.RawRSelectWrapper;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;

public class RDataTable extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "RDataTable";
	
	private RFrameBuilder builder;

	public RDataTable() {
		AbstractRJavaTranslator rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(new Insight(), this.logger);
		this.builder = new RFrameBuilder(rJavaTranslator);
	}
	
	public RDataTable(AbstractRJavaTranslator rJavaTranslator) {
		this.builder = new RFrameBuilder(rJavaTranslator);
	}
	
	public RDataTable(AbstractRJavaTranslator rJavaTranslator, String rTableVarName) {
		if(rTableVarName != null && !rTableVarName.isEmpty()) {
			this.builder = new RFrameBuilder(rJavaTranslator, rTableVarName);
		} else {
			this.builder = new RFrameBuilder(rJavaTranslator);
		}
	}
	
	public RFrameBuilder getBuilder() {
		return this.builder;
	}
	
	public RConnection getConnection() {
		return this.builder.getConnection();
	}
	
	public String getPort() {
		return this.builder.getPort();
	}
	
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
		this.builder.setLogger(logger);
	}
	
	//////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////

	public void addRowsViaIterator(Iterator<IHeadersDataRow> it) {
		// we really need another way to get the data types....
		Map<String, SemossDataType> rawDataTypeMap = this.metaData.getHeaderToTypeMap();
		
		// TODO: this is annoying, need to get the frame on the same page as the meta
		Map<String, SemossDataType> dataTypeMap = new HashMap<String, SemossDataType>();
		for(String rawHeader : rawDataTypeMap.keySet()) {
			dataTypeMap.put(rawHeader.split("__")[1], rawDataTypeMap.get(rawHeader));
		}
		this.addRowsViaIterator(it, this.getTableName(), dataTypeMap);
		syncHeaders();
	}
	
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it, String tableName, Map<String, SemossDataType> dataTypeMap) {
		this.builder.createTableViaIterator(tableName, it, dataTypeMap);
		syncHeaders();
	}
	
	public Object[] getDataRow(String rScript, String[] headerOrdering) {
		return this.builder.getDataRow(rScript, headerOrdering);
	}
	
	public List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering) {
		return this.builder.getBulkDataRow(rScript, headerOrdering);
	}
	
	public void executeRScript(String rScript) {
		//Validate user input won't break R and crash JVM
		RregexValidator reg = new RregexValidator();
		reg.Validate(rScript);
		
		this.builder.evalR(rScript);
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
	public IRawSelectWrapper query(String query) {
		RIterator output = new RIterator(this.builder, query);
		RawRSelectWrapper it = new RawRSelectWrapper();
		it.directExecution(output);
		return it;
	}

	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) {
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		RInterpreter interp = new RInterpreter();
		interp.setQueryStruct(qs);
		interp.setDataTableName(this.getTableName());
		interp.setColDataTypes(this.metaData.getHeaderToTypeMap());
		interp.setAdditionalTypes(this.metaData.getHeaderToAdtlTypeMap());
		interp.setLogger(this.logger);
		logger.info("Generating R Data Table query...");
		String query = interp.composeQuery();
		logger.info("Done generating R Data Table query");

		logger.info("Executing query...");
		RIterator output = new RIterator(this.builder, query, qs);
		RawRSelectWrapper it = new RawRSelectWrapper();
		it.directExecution(output);
		logger.info("Done executing query");
		return it;
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnName, List<String> attributeUniqueHeaderName) {
		int numSelectors = attributeUniqueHeaderName.size();
		List<SemossDataType> dataTypes = new Vector<SemossDataType>();
		Double[] max = new Double[numSelectors];
		Double[] min = new Double[numSelectors];
		
		for (int i = 0; i < numSelectors; i++) {
			String uniqueHeader = this.metaData.getUniqueNameFromAlias(attributeUniqueHeaderName.get(i));
			if(uniqueHeader == null) {
				uniqueHeader = attributeUniqueHeaderName.get(i);
			}
			SemossDataType dataType = this.metaData.getHeaderTypeAsEnum(uniqueHeader);
			dataTypes.add(dataType);
			if(dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE) {
				max[i] = getMax(uniqueHeader);
				min[i] = getMin(uniqueHeader);
			}
		}

		RScaledUniqueFrameIterator iterator = new RScaledUniqueFrameIterator(this, this.builder, columnName, max, min, dataTypes, attributeUniqueHeaderName);
		return iterator;
	}
	
	public Set<String> getColumnsWithIndexes() {
		Set<String> cols = new HashSet<String>();
		for(String s : this.builder.columnIndexSet) {
			// table name and col name are appended together with +++
			cols.add(s.split("\\+\\+\\+")[1]);
		}
		return cols;
	}

	public void addColumnIndex(String columnName) {
		if(columnName.contains("__")) {
			String[] split = columnName.split("__");
			this.builder.addColumnIndex(split[0], split[1]);
		} else {
			String tableName = getTableName();
			this.builder.addColumnIndex(tableName, columnName);
		}
	}
	
	public void addColumnIndex(String[] columnName) {
		String tableName = getTableName();
		this.builder.addColumnIndex(tableName, columnName);
	}

	@Override
	public void removeColumn(String columnHeader) {
		String tableName = this.builder.getTableName();
		this.builder.evalR(tableName + "[," + columnHeader + ":=NULL]");
		this.metaData.dropProperty(tableName + "__" + columnHeader, tableName);
		syncHeaders();
	}
	
	@Override
	public boolean isEmpty() {
		return this.builder.isEmpty();
	}
	
	public String getTableName() {
		return this.builder.getTableName();
	}
	
	public void setTableName(String tableVarName) {
		this.builder.setTableName(tableVarName);
	}
	
	@Override
	public long size(String tableName) {
		if(this.builder.isEmpty(tableName)) {
			return 0;
		}
		return this.builder.getFrameSize(tableName);
	}
	
	public int getNumRows(String varName) {
		return this.builder.getNumRows(varName);
	}
	
	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CachePropFileFrameObject save(String folderDir) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String frameName = this.getTableName();
		cf.setFrameName(frameName);
		
		// save frame
		String frameFilePath = folderDir + DIR_SEPARATOR + frameName + ".rda";
		cf.setFrameCacheLocation(frameFilePath);
		this.builder.save(frameFilePath, frameName);
		
		// also save the meta details
		this.saveMeta(cf, folderDir, frameName);
		return cf;
	}
	
	@Override
	public void open(CachePropFileFrameObject cf) {
		// set the frame name
		this.builder.dataTableName = cf.getFrameName();
		// load the environment
		this.builder.evalR("load(\"" + cf.getFrameCacheLocation().replace("\\", "/") + "\")");
		// open the meta details
		this.openCacheMeta(cf);
	}
	
	@Override
	public String getDataMakerName() {
		return DATA_MAKER_NAME;
	}
	
	@Override
	protected Boolean calculateIsUnqiueColumn(String columnName) {
		// we override this method because it is faster to get the unique count
		// using the below syntax which works for only a single column
		// than it is using the syntax in the interpreter
		
		String tableName = getTableName();
		String[] cleanCols = new String[1];
		if(columnName.contains("__")) {
			cleanCols[0] = columnName.split("__")[1];
		} else {
			cleanCols[0] = columnName;
		}
		
		long start = System.currentTimeMillis();
		String rQuery = tableName + "[, " + RSyntaxHelper.createStringRColVec(cleanCols) + "]"; 
		int val1 = getNumRows(rQuery);
		long end = System.currentTimeMillis();
		logger.info("R duplicates query1 time = " + (end-start) + "ms");
		
		start = System.currentTimeMillis();
		String distinctQuery = "unique(" + tableName + "[, " + RSyntaxHelper.createStringRColVec(cleanCols) + "])"; 
		int val2 = getNumRows(distinctQuery);
		end = System.currentTimeMillis();
		logger.info("R duplicates query2 time = " + (end-start) + "ms");
		
		boolean isUnique = (long) val1 == (long) val2;
		return isUnique;
	}
	
	// generates a row id and binds it
	public void generateRowIdWithName() {
		this.builder.genRowId(getTableName(), "PRIM_KEY_PLACEHOLDER");
	}
	
	@Override
	public void close() {
		super.close();
		this.builder.dropTable();
		closeConnection();
	}
	
	public void closeConnection() {
		// now we only hold 1 connection
		// do not do this...
//		if(this.builder.getConnection() != null) {
//			try {
//				this.builder.getConnection().shutdown();
//			} catch (RserveException e) {
//				logger.info("R Connection is already closed...");
//			}
//		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Deprecated DataMakerComponent stuff
	 */
	
	@Override
	@Deprecated
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
	
	@Override
	@Deprecated
	public void processDataMakerComponent(DataMakerComponent component) {
		// we have only had RDataTable since PKQL was introduced
		// lets not try to expand this to cover the old stuff
		// assuming only pkql is used
		long startTime = System.currentTimeMillis();
		logger.info("Processing Component..................................");
		processPostTransformations(component, component.getPostTrans());
		long endTime = System.currentTimeMillis();
		logger.info("Component Processed: " + (endTime - startTime) + " ms");		
	}
	
	@Override
	@Deprecated
	public void addRow(Object[] cleanCells, String[] headers) {
		// TODO Auto-generated method stub
		
	}
	

	
	

}
