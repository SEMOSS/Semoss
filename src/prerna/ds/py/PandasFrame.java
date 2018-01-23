package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.util.CsvFileIterator;
import prerna.ds.util.ExcelFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
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
	
	public PandasFrame() {
		this(null);
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
		Map<String, SemossDataType> dataTypeMap = new HashMap<String, SemossDataType>();
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
		} else if(it instanceof ExcelFileIterator) {
			addRowsViaExcelIterator((ExcelFileIterator) it, tableName);
		} else {
			// default behavior is to just write this to a csv file
			// and read it back in
			String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".csv";
			File newFile = Utility.writeResultToFile(newFileLoc, it, dataTypeMap);
			
			// generate the script
			StringBuilder script = new StringBuilder(PANDAS_IMPORT_STRING);
			script.append("\n");
			String fileLocation = newFile.getAbsolutePath();
			script.append(PandasSyntaxHelper.getCsvFileRead(PANDAS_IMPORT_VAR, fileLocation, tableName));
			
			// execute the script
			runScript(script.toString());
		}
		
		syncHeaders();
		
		//TODO: testing
		jep.eval(tableName);
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
	
	/**
	 * Generate a table from an Excel file iterator
	 */
	private void addRowsViaExcelIterator(ExcelFileIterator it, String tableName) {
		// generate the script
		StringBuilder script = new StringBuilder(PANDAS_IMPORT_STRING);
		script.append("\n");
		String fileLocation = it.getFileLocation();
		script.append(PandasSyntaxHelper.getCsvFileRead(PANDAS_IMPORT_VAR, fileLocation, tableName));
		
		// execute the script
		runScript(script.toString());
	}
	
	@Override
	public Iterator<IHeadersDataRow> query(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<IHeadersDataRow> query(QueryStruct2 qs) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDataMakerName() {
		return DATA_MAKER_NAME;
	}
	
	private void runScript(String script) {
		// write the script to a file
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
	public void removeRelationship(String[] columns, Object[] values) {
		// TODO Auto-generated method stub
		
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
	public void processDataMakerComponent(DataMakerComponent component) {
		// TODO Auto-generated method stub
		
	}

}
