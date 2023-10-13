package prerna.ds.r;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.crypto.Cipher;

import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.shared.CachedIterator;
import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.poi.main.HeadersException;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.RawRSelectWrapper;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RDataTable extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "RDataTable";
	
	private RFrameBuilder builder;
	Map <String, Object> rJMap = null;

	// THIS CONSTRUCTOR IS USED FOR TESTING
//	public RDataTable(String name) {
//		this.frameName = name;
//		this.originalName = name;
//	}
	
	public RDataTable() {
		AbstractRJavaTranslator rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(new Insight(), this.logger);
		this.builder = new RFrameBuilder(rJavaTranslator);
		this.frameName = getName();
		this.originalName = this.frameName;
	}
	
	public RDataTable(AbstractRJavaTranslator rJavaTranslator) {
		this.builder = new RFrameBuilder(rJavaTranslator);
		this.frameName = getName();
		this.originalName = this.frameName;
	}
	
	public RDataTable(AbstractRJavaTranslator rJavaTranslator, String rTableVarName) {
		if(rTableVarName != null && !rTableVarName.isEmpty()) {
			this.builder = new RFrameBuilder(rJavaTranslator, rTableVarName);
		} else {
			this.builder = new RFrameBuilder(rJavaTranslator);
		}
		this.frameName = getName();
		this.originalName = this.frameName;
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
	
	/**
	 * Get the table name for the current frame
	 * @return
	 */
	@Override
	public String getName() {
		if(this.builder == null) {
			return this.frameName;
		}
		return this.builder.getTableName();
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
		this.addRowsViaIterator(it, this.getName(), dataTypeMap);
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
		// R does not support relations in general
		// so we are going to remove any that may have been added
		// this is important for when the BE changes the frame without 
		// the FE knowing and that frame was native and had joins
		qs.getRelations().clear();
		
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		RInterpreter interp = new RInterpreter();
		interp.setQueryStruct(qs);
		interp.setDataTableName(this.getName());
		interp.setColDataTypes(this.metaData.getHeaderToTypeMap());
//		interp.setAdditionalTypes(this.metaData.getHeaderToAdtlTypeMap());
		interp.setLogger(this.logger);
		// need to do this for subqueries where we flush the values into a filter
		interp.setRDataTable(this);

		boolean cache = true;
		if(qs.getPragmap() != null && qs.getPragmap().containsKey("xCache")) {
			cache = ((String)qs.getPragmap().get("xCache")).equalsIgnoreCase("True") ? true:false;
		}
		
		logger.info("Generating R Data Table query...");
		String query = interp.composeQuery();
		logger.info("Done generating R Data Table query");
		Map<String, SemossDataType> convertedDates = interp.getConvertedDates();
		
		RawRSelectWrapper it = null;
		IRawSelectWrapper retWrapper = null;
		String looker = interp.getMainQuery();
		looker = looker + qs.getLimit() + qs.getOffset();
		// sets the frame builder
		if(!queryCache.containsKey(looker) || !cache) {
			logger.info("Executing query...");
			RIterator output = new RIterator(this.builder, query, qs);
			// add the random var used from the insight
			output.addVarForCleanup(interp.getTempVarName());
			output.setConvertedDates(convertedDates);
			output.setQuery(looker);
			it = new RawRSelectWrapper();
			it.directExecution(output);
			logger.info("Done executing query");
			retWrapper = it;
			
			if(!cache) {
				clearQueryCache();
			}
		} else {
			CachedIterator ci = queryCache.get(looker);
			RawCachedWrapper rcw = new RawCachedWrapper();
			rcw.setIterator(ci);
			retWrapper = rcw;
		}
		return retWrapper;
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter() {
		RInterpreter interp = new RInterpreter();
		interp.setDataTableName(this.getName());
		interp.setColDataTypes(this.metaData.getHeaderToTypeMap());
		return interp;
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
			String tableName = getName();
			this.builder.addColumnIndex(tableName, columnName);
		}
	}
	
	public void addColumnIndex(String[] columnName) {
		String tableName = getName();
		this.builder.addColumnIndex(tableName, columnName);
	}
	
	public void removeAllColumnIndex() {
		this.builder.removeAllColumnIndex();
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
	
	@Override
	public void setName(String tableVarName) {
		this.builder.setTableName(tableVarName);
		this.frameName = this.builder.getTableName();
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
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String frameName = this.getName();
		cf.setFrameName(frameName);
		
		// save frame
		try {
			// this throws an exception if the library doesn't exist
			this.builder.rJavaTranslator.checkPackages(new String[] {"fst"});
			String frameFilePath = folderDir + DIR_SEPARATOR + frameName + ".fst";
			cf.setFrameCacheLocation(frameFilePath);
			this.builder.saveFst(frameFilePath, frameName);
		} catch(Exception e) {
			String frameFilePath = folderDir + DIR_SEPARATOR + frameName + ".rda";
			cf.setFrameCacheLocation(frameFilePath);
			this.builder.saveRda(frameFilePath, frameName);
		}
		
		// also save the meta details
		this.saveMeta(cf, folderDir, frameName, cipher);
		return cf;
	}
	
	@Override
	public void open(CachePropFileFrameObject cf, Cipher cipher) {
		// set the frame name
		this.builder.dataTableName = cf.getFrameName();
		// load the environment
		String filePath = cf.getFrameCacheLocation();
		if(filePath.endsWith(".fst")){
			this.builder.openFst(cf.getFrameCacheLocation(), cf.getFrameName());
		} else if(filePath.endsWith(".rda")) {
			this.builder.openRda(cf.getFrameCacheLocation());
		} else {
			throw new SemossPixelException("Unknown R cache format");
		}
		// open the meta details
		this.openCacheMeta(cf, cipher);
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
		
		String tableName = getName();
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
		this.builder.genRowId(getName(), "PRIM_KEY_PLACEHOLDER");
	}
	
	@Override
	public void close() {
		super.close();
		this.builder.dropTable();
		if(!this.originalName.equals(this.frameName)) {
			this.builder.setTableName(this.originalName);
			this.builder.dropTable();
		}
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
	
	/**
	 * Recreate the metadata for this existing frame
	 */
	public void recreateMeta() {
		String[] colNames = this.builder.getColumnNames();
		String[] colTypes = this.builder.getColumnTypes();
		//clean headers
		HeadersException headerChecker = HeadersException.getInstance();
		colNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		String rColNames = "";
		for (int i = 0; i < colNames.length; i++) {
			rColNames += "\"" + colNames[i] + "\"";
			if (i < colNames.length - 1) {
				rColNames += ", ";
			}
		}
		String script = "colnames(" + getName() + ") <- c(" + rColNames + ")";
		this.builder.evalR(script);

		// grab the existing metadata from the frame
		Map<String, String> additionalDataTypes = this.metaData.getHeaderToAdtlTypeMap();
		Map<String, List<String>> sources = this.metaData.getHeaderToSources();
		Map<String, String[]> complexSelectors = this.metaData.getComplexSelectorsMap();
		// close existing meta
		// make a new one
		// load into it
		this.metaData.close();
		this.metaData = new OwlTemporalEngineMeta();
		ImportUtility.parseTableColumnsAndTypesToFlatTable(this.metaData, colNames, colTypes, getName(), additionalDataTypes, sources, complexSelectors);
		
		// clear the cached info
		this.clearCachedMetrics();
		this.clearQueryCache();
	}
	
	@Override
	public DataFrameTypeEnum getFrameType() {
		return DataFrameTypeEnum.R;
	}
	
	
	@Override
	public Object querySQL(String sql) {
		// columns
		// types
		// data
		if(sql.trim().toUpperCase().startsWith("SELECT")) {
			Map retMap = new HashMap();
			
			String tempFrameName = Utility.getRandomString(5);
			String makeNewFrame = tempFrameName + " <- as.data.table(sqldf(\"" + sql.replace("\"", "\\\"") + "\"));";
			String deleteAll = "rm(" + tempFrameName + ")";
			
			this.builder.getRJavaTranslator().executeEmptyR("library(sqldf);");
			this.builder.getRJavaTranslator().executeEmptyR(makeNewFrame);
			
			String [] columns = getColumnNames(tempFrameName);
			String [] types = getColumnTypes(tempFrameName);
			
			List <Object[]> retObject = getBulkDataRow(tempFrameName, columns); //(frameName + "_dict"); // get the dictionary back
			// will delete later
			this.builder.getRJavaTranslator().executeEmptyR(deleteAll);
			Object [] convertedTypeArray = new Object[columns.length];
			//https://www.tutorialspoint.com/r/r_data_types.htm
			getRJMap();
			for(int columnIndex = 0;columnIndex < columns.length;columnIndex++)
			{
				String thisColumn = columns[columnIndex];
				String thisType = types[columnIndex];

				// convert this to type array
				// for now will just move this all to string
				Object javaType = rJMap.get(thisType);
				if(javaType != null) {
					convertedTypeArray[columnIndex] = javaType;
				} else {
					convertedTypeArray[columnIndex] = java.lang.String.class;
				}
			}
			
			retMap.put("columns", columns);
			retMap.put("types", convertedTypeArray);
			retMap.put("dataArray", retObject);
			
			return retMap;
		} else {
			Map retMap = new HashMap();

			String [] commands = sql.split("\\R");
			// execute each command and drop the result
			String [] columns = new String [] {"Command", "Output"};
			Object [] types = new Object [] {java.lang.String.class, java.lang.String.class};
			
			List <List<Object>> data = new ArrayList<List<Object>>();
			
			for(int commandIndex = 0;commandIndex < commands.length;commandIndex++) {
				List <Object> row = new ArrayList <Object>();
				String thisCommand = commands[commandIndex];
				Object output = this.builder.getRJavaTranslator().runRAndReturnOutput(thisCommand);
				
				row.add(thisCommand);
				row.add(output);
				
				data.add(row);
			}
			
			retMap.put("data", data);
			retMap.put("types", types);
			retMap.put("columns", columns);
			
			return retMap;
		}
	}
	
	@Override
	public Object queryCSV(String sql)
	{
		// columns
		// types
		// data
		if(sql.toUpperCase().startsWith("SELECT"))
		{
			Map retMap = new HashMap();
			
			String loadsqlDF = "library(sqldf);";
			String frameName = Utility.getRandomString(5);
			File fileName = new File(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR),   frameName + ".csv");
			
			String fileNameStr = fileName.getAbsolutePath().replace("\\", "/");

			String newFrame = "write.csv(sqldf('" + sql + "'), '" + fileNameStr + "')";
			
			this.builder.getRJavaTranslator().executeEmptyR(loadsqlDF);
			this.builder.getRJavaTranslator().executeEmptyR(newFrame);
			
			
			return fileName;
		}
		else
		{

			String frameName = Utility.getRandomString(5);
			File fileName = new File(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR),   frameName + ".csv");

			try {
				PrintWriter bw = new PrintWriter(new FileWriter(fileName));
				bw.write("Command, Output");
				bw.println();
				
				String [] commands = sql.split("\\R");
				// execute each command and drop the result
				String [] columns = new String [] {"Command", "Output"};
				Object [] types = new Object [] {java.lang.String.class, java.lang.String.class};
				
				List <List<Object>> data = new ArrayList<List<Object>>();
				
				for(int commandIndex = 0;commandIndex < commands.length;commandIndex++)
				{
					List <Object> row = new ArrayList <Object>();
					String thisCommand = commands[commandIndex];
					Object output = this.builder.getRJavaTranslator().runRAndReturnOutput(thisCommand);
					
					bw.write(thisCommand);
					bw.print(", ");
					bw.print(output);
					
					bw.println();
				}
				bw.flush();
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			return fileName;
		}

		
	}

	@Override
	public Object queryJSON(String sql)
	{
		// columns
		// types
		// data
		if(sql.toUpperCase().startsWith("SELECT"))
		{
			Map retMap = new HashMap();
			
			String loadsqlDF = "library(sqldf);library(jsonlite);";
			String frameName = Utility.getRandomString(5);
			File fileName = new File(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR),   frameName + ".csv");
			
			String fileNameStr = fileName.getAbsolutePath().replace("\\", "/");

			String newFrame = "write_json(sqldf('" + sql + "'), '" + fileNameStr + "')";
			
			this.builder.getRJavaTranslator().executeEmptyR(loadsqlDF);
			this.builder.getRJavaTranslator().executeEmptyR(newFrame);
			
			
			return fileName;
		}
		else
		{
			String frameName = Utility.getRandomString(5);
			File fileName = new File(DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR),   frameName + ".csv");

			try {
				PrintWriter bw = new PrintWriter(new FileWriter(fileName));
				bw.write("Command, Output");
				bw.println();
				
				String [] commands = sql.split("\\R");
				// execute each command and drop the result
				String [] columns = new String [] {"Command", "Output"};
				Object [] types = new Object [] {java.lang.String.class, java.lang.String.class};
				
				List <List<Object>> data = new ArrayList<List<Object>>();
				
				for(int commandIndex = 0;commandIndex < commands.length;commandIndex++)
				{
					List <Object> row = new ArrayList <Object>();
					String thisCommand = commands[commandIndex];
					Object output = this.builder.getRJavaTranslator().runRAndReturnOutput(thisCommand);
					
					bw.write(thisCommand);
					bw.print(", ");
					bw.print(output);
					
					bw.println();
				}
				bw.flush();
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			return fileName;
		}

		
	}

	
	
	private Map getRJMap() {
		if(this.rJMap == null) {
			rJMap = new HashMap();
			rJMap.put("integer", java.lang.Integer.class);
			rJMap.put("character", java.lang.String.class);
			rJMap.put("numeric", java.lang.Double.class);
			rJMap.put("logical", java.lang.Boolean.class);
			rJMap.put("factor", java.lang.String.class);
		}
		return rJMap;
	}
	
	@Override
	public String createVarFrame() {
		RInterpreter interp = new RInterpreter();
		SelectQueryStruct qs = getMetaData().getFlatTableQs(true);
		// add all the frame filter
		qs.setExplicitFilters(this.grf.copy());
		// convert to physical
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		interp.setQueryStruct(qs);
		interp.setDataTableName(this.getName());
		interp.setColDataTypes(this.metaData.getHeaderToTypeMap());
//		interp.setAdditionalTypes(this.metaData.getHeaderToAdtlTypeMap());
		interp.setLogger(this.logger);
		// need to do this for subqueries where we flush the values into a filter
		interp.setRDataTable(this);
		
		String query = interp.composeQuery();
		String newFrame = Utility.getRandomString(6);
		String command = newFrame  + " = " + query;
		// create the frame
		builder.getRJavaTranslator().executeEmptyR(command);
		return newFrame;
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Deprecated DataMakerComponent stuff
	 */
	
//	@Override
//	@Deprecated
//	public Map<String, String> getScriptReactors() {
//		Map<String, String> reactorNames = super.getScriptReactors();
//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.RImportDataReactor");
//		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.RDuplicatesReactor");
//
//		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
//		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
//		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
//		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
//		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
//		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
//		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
//		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
//		reactorNames.put(PKQLEnum.OPEN_DATA, "prerna.sablecc.OpenDataReactor");
//		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
//		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
//		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");
//		reactorNames.put(PKQLEnum.NETWORK_CONNECT, "prerna.sablecc.ConnectReactor");
//		reactorNames.put(PKQLEnum.NETWORK_DISCONNECT, "prerna.sablecc.DisConnectReactor");
//
//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.RVizReactor");
//
//		reactorNames.put(PKQLEnum.SUM, "prerna.sablecc.expressions.r.RSumReactor");
//		reactorNames.put(PKQLEnum.MAX, "prerna.sablecc.expressions.r.RMaxReactor");
//		reactorNames.put(PKQLEnum.MIN, "prerna.sablecc.expressions.r.RMinReactor");
//		reactorNames.put(PKQLEnum.AVERAGE, "prerna.sablecc.expressions.r.RAverageReactor");
//		reactorNames.put(PKQLEnum.STANDARD_DEVIATION, "prerna.sablecc.expressions.r.RStandardDeviationReactor");
//		reactorNames.put(PKQLEnum.MEDIAN, "prerna.sablecc.expressions.r.RMedianReactor");
//		reactorNames.put(PKQLEnum.COUNT, "prerna.sablecc.expressions.r.RCountReactor");
//		reactorNames.put(PKQLEnum.COUNT_DISTINCT, "prerna.sablecc.expressions.r.RUniqueCountReactor");
//
//		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
//		reactorNames.put(PKQLEnum.CSV_API, "prerna.sablecc.RCsvApiReactor");
//		reactorNames.put(PKQLEnum.EXCEL_API, "prerna.sablecc.RExcelApiReactor");
//		reactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");
//
//		return reactorNames;
//	}
	
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
