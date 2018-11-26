package prerna.ds.py;

import java.io.File;
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
import prerna.sablecc2.reactor.frame.py.PyExecutorThread;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PandasFrame extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "PandasFrame";
	
	private static final String PANDAS_IMPORT_VAR = "pandas_import_var";
	private static final String PANDAS_IMPORT_STRING = "import pandas as " + PANDAS_IMPORT_VAR;
	
	private JepWrapper jep;
	private String scripFolder;
	private String tableName;
	
	static Hashtable pyS = new Hashtable();
	static Hashtable spy = new Hashtable();
	
	// gets all the commands in one fell swoop
	List <String> commands = new ArrayList<String>();
	
	private PyExecutorThread py = null;
	PandasInterpreter interp = new PandasInterpreter();
	
	Map<String, SemossDataType>  dataTypeMap = null;
	
	static{
		pyS.put("dtype('O')", SemossDataType.STRING);
		pyS.put("dtype('int64')", SemossDataType.INT);
		pyS.put("dtype('float64')", SemossDataType.DOUBLE);
		
		spy.put(SemossDataType.STRING, "dtype('O')");
		spy.put(SemossDataType.INT, "dtype('int64')");
		spy.put(SemossDataType.DOUBLE, "dtype('float64')");
		
		spy.put("dtype('O')", "string");
		spy.put("dtype('int64')", "int64");
		spy.put("dtype('float64')", "float64");


	}
	
	public PandasFrame() {
		this(null);
	}
	
	public void setJep(PyExecutorThread py)
	{
		this.py = py;
	}
	
	public PandasFrame(String tableName) {
		this.jep = new JepWrapper();
		
		this.scripFolder = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + 
				"\\" + DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		
		if(tableName == null || tableName.trim().isEmpty()) {
			tableName = "PYFRAME_" + UUID.randomUUID().toString().replace("-", "_");
		}
		this.tableName = tableName;
	}
	
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it) {
		// we really need another way to get the data types....
		Map<String, SemossDataType> rawDataTypeMap = this.metaData.getHeaderToTypeMap();
		
		// TODO: this is annoying, need to get the frame on the same page as the meta
		dataTypeMap = new HashMap<String, SemossDataType>();
		for(String rawHeader : rawDataTypeMap.keySet()) {
			dataTypeMap.put(rawHeader.split("__")[1], rawDataTypeMap.get(rawHeader));
		}
		this.addRowsViaIterator(it, this.tableName, dataTypeMap);
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
			StringBuilder script = new StringBuilder();
			String fileLocation = newFile.getAbsolutePath();
			script.append(PandasSyntaxHelper.getCsvFileRead(PANDAS_IMPORT_VAR, fileLocation, tableName));
			
			// execute the script
			runScript(importS, script.toString());
		}
		
		syncHeaders();
		
		runScript("list(locals())");
		
		// need to get a pandas frame types and then see if this is the same as 
		//alignColumns(tableName, dataTypeMap);
		
		//TODO: testing
		//jep.eval(tableName);
	}
	
	public void setDataTypeMap(Map<String, SemossDataType> dataTypeMap)
	{
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
	
	/**
	 * Generate a table from a CSV file iterator
	 * @param it
	 * @param tableName
	 */
	private void addRowsViaCsvIterator(CsvFileIterator it, String tableName) {
		// generate the script
		StringBuilder script = new StringBuilder(PANDAS_IMPORT_STRING);
		script.append("\n");
		String fileLocation = it.getFileLocation();
		script.append(PandasSyntaxHelper.getCsvFileRead(PANDAS_IMPORT_VAR, fileLocation, tableName));
		
		// execute the script
		runScript(script.toString());
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
	public Object [] getHeaderAndTypes()
	{
		String colScript = PandasSyntaxHelper.getColumns(tableName);
		String typeScript = PandasSyntaxHelper.getTypes(tableName);
		
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
		interp.setDataTableName(this.tableName);
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
	
	private Object runScript(String... script) {
		
		// so I am nto sure we need to write it to a file etc. for now.. I will run it
		py.command = script;
		Object monitor = py.getMonitor();
		Object response = null;
		synchronized(monitor)
		{
			try
			{
				monitor.notify();
				monitor.wait();
			}catch (Exception ignored)
			{
				
			}
			if(script.length == 1)
				response = py.response.get(script[0]);
			else
				response = py.response;
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
		// TODO Auto-generated method stub
		
		return 0;
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
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
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public CachePropFileFrameObject save(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open(CachePropFileFrameObject cf) {
		
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// TODO Auto-generated method stub
		
	}
}
