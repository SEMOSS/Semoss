package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import jep.Jep;
import jep.JepException;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.util.CsvFileIterator;
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
	
	private String scripFolder;
	private String tableName;
	
	public PandasFrame() {
		this(null);
	}
	
	public PandasFrame(String tableName) {
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
	
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it, String tableName, Map<String, SemossDataType> dataTypeMap) {
		if(it instanceof CsvFileIterator) {
			addRowsViaCsvIterator((CsvFileIterator) it, tableName);
		}
		syncHeaders();
	}
	
	private void addRowsViaCsvIterator(CsvFileIterator it, String tableName) {
		StringBuilder script = new StringBuilder(PANDAS_IMPORT_STRING);
		script.append("\n");
		String fileLocation = it.getFileLocation();
		script.append(PandasSyntaxHelper.getFileRead(PANDAS_IMPORT_VAR, fileLocation, tableName));
		runScript(script.toString());
		
		try (Jep jep = getJep()){
			Object o = jep.eval(tableName);
			System.out.println(o);
		} catch (JepException e) {
			e.printStackTrace();
		}
	}
	
	public Jep getJep() {
		try {
			Jep jep = new Jep(false);
			return jep;
		} catch (JepException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
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
		File f = new File(this.scripFolder + "/" + Utility.getRandomString(6) + ".py");
		try {
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			System.out.println("Error in writing python script for execution!");
			e1.printStackTrace();
		}
		
		try (Jep jep = getJep()){
			jep.runScript(f.getAbsolutePath());
			Object o = jep.eval(tableName);
			System.out.println(o);
		} catch (JepException e) {
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			f.delete();
		}
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
