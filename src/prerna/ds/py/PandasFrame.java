package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PandasFrame extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "PandasFrame";
	
	private static final String PANDAS_IMPORT_VAR = "pandas_import_var";
	private static final String PANDAS_IMPORT_STRING = "import pandas as " + PANDAS_IMPORT_VAR;
	
	static Hashtable <String, SemossDataType> pyS = new Hashtable();
	static Hashtable  spy = new Hashtable();
	
	// gets all the commands in one fell swoop
	List <String> commands = new ArrayList<String>();
	
	PandasFrame prevFrame = null;
	
	private PyExecutorThread py = null;
	PandasInterpreter interp = new PandasInterpreter();
	
	Map<String, SemossDataType>  dataTypeMap = null;
	
	static{
		pyS.put("object", SemossDataType.STRING);
		pyS.put("int64", SemossDataType.INT);
		pyS.put("float64", SemossDataType.DOUBLE);
		pyS.put("datetime64", SemossDataType.DATE);
		
		spy.put(SemossDataType.STRING, "object");
		spy.put(SemossDataType.INT, "np.int64");
		spy.put(SemossDataType.DOUBLE, "np.float64");
		spy.put(SemossDataType.DATE, "np.datetime64");
		spy.put(SemossDataType.TIMESTAMP, "np.datetime64");
		
		spy.put("float64", "np.float64");
		spy.put("int64", "np.int64");
		spy.put("datetime64", "np.datetime64");
		
		spy.put("dtype('O')", "string");
		spy.put("dtype('int64')", "int64");
		spy.put("dtype('float64')", "float64");
	}
	
	public PandasFrame() {
		this(null);
	}
	
	public PandasFrame(String tableName) {
		if(tableName == null || tableName.trim().isEmpty()) {
			tableName = "PYFRAME_" + UUID.randomUUID().toString().replace("-", "_");
		}
		this.frameName = tableName;


	}
	
	public void setJep(PyExecutorThread py) {
		this.py = py;
	}

	public PyExecutorThread getJep() {
		return this.py ;
	}
	
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it) {
		// we really need another way to get the data types....
		Map<String, SemossDataType> rawDataTypeMap = this.metaData.getHeaderToTypeMap();
		
		// TODO: this is annoying, need to get the frame on the same page as the meta
		dataTypeMap = new HashMap<String, SemossDataType>();
		for(String rawHeader : rawDataTypeMap.keySet()) {
			dataTypeMap.put(rawHeader.split("__")[1], rawDataTypeMap.get(rawHeader));
		}
		this.addRowsViaIterator(it, this.frameName, dataTypeMap);
	}
	
	/**
	 * Generate a table from an iterator
	 * @param it
	 * @param tableName
	 * @param dataTypeMap
	 */
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it, String tableName, Map<String, SemossDataType> dataTypeMap) {
		if(it instanceof CsvFileIterator) {
			addRowsViaCsvIterator((CsvFileIterator) it, tableName);
		} else if(it instanceof ExcelSheetFileIterator) {
			throw new IllegalArgumentException("Have yet to implement pandas frame with excel iterator");
		} else {
			// default behavior is to just write this to a csv file
			// and read it back in
			String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".csv";
			File newFile = Utility.writeResultToFile(newFileLoc, it, dataTypeMap);
			
			String importS = new StringBuilder(PANDAS_IMPORT_STRING).toString();
			// generate the script
			runScript(tableName + "=1");
			String fileLocation = newFile.getAbsolutePath();
			String loadS = PandasSyntaxHelper.getCsvFileRead(PANDAS_IMPORT_VAR, fileLocation, tableName);
			// execute the script
			runScript(importS, loadS);
			
			// create the wrapper
			// it is just the frame name with w on it
			//String makeWrapper = tableName+"w = PyFrame.makefm(" + tableName +")";
			//runScript(makeWrapper);

			// delete the generated file
			newFile.delete();
		}
		syncHeaders();
		
		runScript("list(locals())");
		// need to get a pandas frame types and then see if this is the same as 
		if(!isEmpty(tableName)) {
			adjustDataTypes(tableName);
		}
		
		String makeWrapper = tableName+"w = PyFrame.makefm(" + tableName +")";
		runScript(makeWrapper);

	}
	
	/**
	 * Generate a table from a CSV file iterator
	 * @param it
	 * @param tableName
	 */
	private void addRowsViaCsvIterator(CsvFileIterator it, String tableName) {
		// generate the script
		String importS = new StringBuilder(PANDAS_IMPORT_STRING).toString();
		String fileLocation = it.getFileLocation();
		String loadS = PandasSyntaxHelper.getCsvFileRead(PANDAS_IMPORT_VAR, fileLocation, tableName);

		// need to compose a string for names
		StringBuilder header = new StringBuilder("");
		String [] headers = it.getHeaders();
		for(int headerIndex = 0;headerIndex < headers.length;headerIndex++) {
			if(header.length() == 0) {
				header.append("[");
			} else {
				header.append(",");
			}
			header.append("'").append(headers[headerIndex]).append("'");
		}
		header.append("]");
		String headerS = tableName+".columns=" + header.toString();
		// execute all 3 scripts
		runScript(importS, loadS, headerS);
	}
	
	public void merge(String returnTable, String leftTableName, String rightTableName, String joinType, List<Map<String, String>> joinCols) {
		String mergeString = PandasSyntaxHelper.getMergeSyntax(PANDAS_IMPORT_VAR, returnTable, leftTableName, rightTableName, joinType, joinCols);
		runScript(mergeString);
	}
	
	private void adjustDataTypes(String tableName) {
		
		// get the data types from python first
		// check against current
		// if they are different change it
		
		String colScript = PandasSyntaxHelper.getColumns(tableName);
		String typeScript = PandasSyntaxHelper.getTypes(tableName);
		
		//run
		
		//Hashtable response = (Hashtable)runScript(colScript, typeScript);
		
		ArrayList headerList = (ArrayList)runScript(colScript);
		String [] headers = new String[headerList.size()];
		
		headerList.toArray(headers);
		SemossDataType [] stypes = new SemossDataType[headers.length];
		
		
		ArrayList <String> types = (ArrayList)runScript(typeScript);

		// should we jsut run script inline ?
		
		for(int colIndex = 0;colIndex < headers.length;colIndex++)
		{
			String colName = headers[colIndex];
			String colType = types.get(colIndex);
			
			
			if(colType == null)
				colType = "STRING";

			//System.out.println("Working with col type and name " + colType + " <<>> " + colName);
			
			SemossDataType pysColType = (SemossDataType)pyS.get(colType);
			SemossDataType proposedType = dataTypeMap.get(colName);
			
			//System.out.println("Working with col type and name " + proposedType + " <<>>  " + colName);
			
			String pyproposedType = colType;
			if(proposedType != null)
				pyproposedType = (String)spy.get(proposedType);
			else
				pyproposedType = (String)spy.get(colType);
				
			
			if(proposedType != null && pysColType != proposedType)
			{
				// create and execute the type
				if(proposedType != SemossDataType.DATE && proposedType != SemossDataType.TIMESTAMP)
				{
					String	typeChanger = tableName + "['" + colName + "'] = " + tableName + "['" + colName + "'].astype(" + pyproposedType + ", errors='ignore')";
					runScript(typeChanger);
				}
				else
				{
					String	typeChanger = tableName + "['" + colName + "'] = pd.to_datetime(" + tableName + "['" + colName + "'], errors='ignore')";
					runScript(typeChanger);
					
				}
				
			}
			else
			{
				//System.out.println("Types Matched.. !!!!!!!!!!");
			}
			stypes[colIndex] = pysColType;
		}
		Object [] retObject = new Object[2];
		retObject[1] = headers;
	}
	
	public void setDataTypeMap(Map<String, SemossDataType> dataTypeMap) {
		this.dataTypeMap = dataTypeMap;
		interp.setDataTypeMap(dataTypeMap);
	}
	
	private void alignColumns(String tableName, Map<String, SemossDataType> dataTypeMap)
	{
		// look at what are the column types
		// see if they are the same as data type map
		
		String colScript = PandasSyntaxHelper.getColumns(tableName);
		String typeScript = PandasSyntaxHelper.getTypes(tableName);
		
		Hashtable columnTypes = (Hashtable)runScript(colScript, typeScript);
		// get the column names
		ArrayList <String> cols = (ArrayList)columnTypes.get(colScript);
		ArrayList <String> types = (ArrayList)columnTypes.get(typeScript);

		List <String> colChanger = new ArrayList<String>();

		for(int colIndex = 0;colIndex < cols.size();colIndex++)
		{
			String colName = cols.get(colIndex);
			String colType = types.get(colIndex);
			
			SemossDataType pysColType = (SemossDataType)pyS.get(colType);
			
			SemossDataType sColType = dataTypeMap.get(colName);
		
			colChanger = checkColumn(tableName, colName, pysColType, sColType, colChanger);
		}
		
		runScript((String [])colChanger.toArray());
	}
	
	private List<String> checkColumn(String tableName, String colName, SemossDataType curType, SemossDataType newType, List <String> colChanger)
	{
		if(curType == newType)
			return colChanger;
		
		else
		{
			// get the pytype
			String pyType = (String)spy.get(curType);
			
			// get the as type
			String asType = (String)spy.get(pyType);
			
			// column change
			String script = PandasSyntaxHelper.getColumnChange(tableName, colName, asType);
			colChanger.add(script);
			
			return colChanger;
		}
	}
	
	@Override
	public IRawSelectWrapper query(String query) {
		
		System.out.println(".");
		Object output = runScript(query);
		ArrayList response = null;
		
		String [] headers = interp.getHeaders();
		SemossDataType [] types = interp.getTypes();
		ArrayList <String> actHeaders = null;
		
		boolean sync = true;	
		
		// get the types for headers also
		if(interp.isScalar())
		{
			ArrayList val = new ArrayList();
			val.add(output);
			response = new ArrayList();
			response.add(val);
		}
		else if(output instanceof HashMap)
		{
			HashMap map = (HashMap)output;
			
			response = (ArrayList)map.get("data");
			
			// get the columns
			ArrayList columns = (ArrayList)map.get("columns");
			
			actHeaders = mapColumns(columns);
			sync = sync(headers, actHeaders);
		}else if(output instanceof ArrayList)
		{
			response = (ArrayList)output;
			actHeaders = null;
			sync = true;
		}
		
		PandasIterator pi = new PandasIterator(headers, response, types);
		
		// set the actual headers so that it can be processed
		// if not in sync then transform
		pi.setTransform(actHeaders, !sync);
			
		RawPandasWrapper rpw = new RawPandasWrapper();
		rpw.setPandasIterator(pi);
		return rpw	;
	}
	
	// tries to see if the order in which pandas is giving is valid with the order that is being requested
	public boolean sync(String [] headers, ArrayList <String> actHeaders)
	{
		boolean sync = true;
		for(int headerIndex = 0;headerIndex < headers.length && sync;headerIndex++)
			sync = sync && (headers[headerIndex].equals(actHeaders.get(headerIndex)));
		
		return sync;
	}

	
	// get the types of headers
	public Object [] getHeaderAndTypes() {
		String colScript = PandasSyntaxHelper.getColumns(this.frameName);
		String typeScript = PandasSyntaxHelper.getTypes(this.frameName);
		
		Hashtable response = (Hashtable)runScript(colScript, typeScript);

		String [] headers = (String [])((ArrayList)response.get(colScript)).toArray();
		SemossDataType [] stypes = new SemossDataType[headers.length];
		
		ArrayList <String> types = (ArrayList)response.get(typeScript);

		for(int colIndex = 0;colIndex < headers.length;colIndex++)
		{
			String colName = headers[colIndex];
			String colType = types.get(colIndex);
			
			SemossDataType pysColType = (SemossDataType)pyS.get(colType);
			stypes[colIndex] = pysColType;
		}
		Object [] retObject = new Object[2];
		retObject[1] = headers;
		
		return retObject;
	}
	
	

	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) {
		this.metaData.getHeaderToTypeMap();
		interp.setDataTableName(this.frameName);
		interp.setDataTypeMap(dataTypeMap);
		interp.setQueryStruct(qs);
		String query = interp.composeQuery();
		return query(query);
	}

	@Override
	public String getDataMakerName() {
		return DATA_MAKER_NAME;
	}
	
	private ArrayList mapColumns(ArrayList columns)
	{
		ArrayList newHeaders = new ArrayList();
		Map <String, String> funcMap = interp.functionMap();

		for(int colIndex = 0;colIndex < columns.size();colIndex++)
		{
			// every element here is List
			Object item = columns.get(colIndex);
			if(item instanceof List)
			{
				List elem = (List)columns.get(colIndex);
				String key = elem.get(1)+ "" +elem.get(0);
				if(funcMap.containsKey(key))
					newHeaders.add(funcMap.get(key));
				else
					newHeaders.add(elem.get(0));
			}
			else
				newHeaders.add(item+"");
		}
		return newHeaders;
	}
	
	public Object runScript(String... script) {
		// so I am nto sure we need to write it to a file etc. for now.. I will run it
		py.command = script;
		Object monitor = py.getMonitor();
		Object response = null;
		synchronized(monitor) {
			try {
				monitor.notify();
				monitor.wait(4000);
			} catch (Exception ignored) {

			}
			if(script.length == 1) {
				response = py.response.get(script[0]);
			} else {
				response = py.response;
			}
		}

		/*		// write the script to a file
		File f = new File(this.scripFolder + "/" + Utility.getRandomString(6) + ".py");
		try {
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			System.out.println("Error in writing python script for execution!");
			e1.printStackTrace();
		}

		// execute the file
		jep.runScript(f.getAbsolutePath());

		// delete the file
		f.delete();

		 */	
		commands.add(script[0]);
		return response;
	}
	
	@Override
	public long size(String tableName) {
		String command = "len(" + tableName + ")";
		Number num = (Number) runScript(command);
		return num.longValue();
	}
	
	@Override
	public void close() {
		// this should take the variable name and kill it
		// if the user has created others, nothing can be done
		logger.info("Removing variable " + this.frameName);
		runScript("del " + this.frameName);
		// this should take the variable name and kill it
		// if the user has created others, nothing can be done
		
		//logger.info("Removing variable " + tableName + " with GC ");
		//runScript("del " + tableName);
		runScript("gc.collect()");
		
	}
	
	@Override
	public CachePropFileFrameObject save(String folderDir) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();
		// save frame
		String frameFilePath = folderDir + DIR_SEPARATOR + frameName + ".pkl";
		cf.setFrameCacheLocation(frameFilePath);
		String command = this.frameName + ".to_pickle(\"" + frameFilePath.replace("\\", "/") + "\")";
		runScript(command);
		
		// also save the meta details
		this.saveMeta(cf, folderDir, frameName);
		return cf;
	}
	
	private void makeDefaultImports()
	{
		
	}
	
	@Override
	public void open(CachePropFileFrameObject cf) {
		// load the pandas library
		runScript(PANDAS_IMPORT_STRING);
		// load the frame
		String command = this.frameName + "= " + PANDAS_IMPORT_VAR + ".read_pickle(\"" +  cf.getFrameCacheLocation().replace("\\", "/") + "\")";
		runScript(command);
		// open the meta details
		this.openCacheMeta(cf);
		setDataTypeMap(this.metaData.getHeaderToTypeMap());
	}

	@Override
	public boolean isEmpty() {
		return isEmpty(this.frameName);
	}
	
	public boolean isEmpty(String tableName) {
		String command = "\"" + tableName + "\" in vars() and len(" + tableName + ") <= 0";
		Boolean isEmpty = (Boolean) runScript(command);
		return isEmpty;
	}
	
	///////////////////////////////////////////////////////////////////////////////
	
	
	@Override
	public void addRow(Object[] cleanCells, String[] headers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeColumn(String columnHeader) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// TODO Auto-generated method stub
		
	}
	
	// set the previous frame
	public void setPrevFrame(PandasFrame frame)
	{
		this.prevFrame = prevFrame;
	}
	
	public PandasFrame getPrevFrame()
	{
		return this.prevFrame;
	}

}
