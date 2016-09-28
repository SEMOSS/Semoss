package prerna.algorithm.impl;

import java.util.Hashtable;
import java.util.Vector;

import prerna.algorithm.api.IAction;
import prerna.algorithm.api.ITableDataFrame;

public class ImportAction implements IAction {
	
	Hashtable <String, Object> settings = new Hashtable();
	String [] cols = null;
	

	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		this.settings.put(key, value);
	}

	@Override
	public void processCell(String nodeName, Object data) {
	}

	@Override
	public void processRow(String nodeName, Object data) {
		// TODO Auto-generated method stub
		// grab the frame from the settings
		getCols();
		ITableDataFrame thisFrame = (ITableDataFrame)settings.get("TF");
		// now get the headers
		Object [] vals = convertVectorToArrayO((Vector)data);
		System.err.println("Will insert now.. " + cols + vals);
		
		thisFrame.addRow(vals, cols);
		
		
	}

	@Override
	public void processTable(String nodeName, Object data) {
		// TODO Auto-generated method stub

	}
	
	private void getCols()
	{
		if(cols == null && settings.containsKey("COL_DEF"))
		{
			Vector <String> colV = (Vector <String>)settings.get("COL_DEF");
			cols = convertVectorToArray(colV);
		}
	}

	private String[] convertVectorToArray(Vector <String> columns)
	{
		// convert this column array
		String [] colArr = new String[columns.size()];
		for(int colIndex = 0;colIndex < columns.size();colArr[colIndex] = columns.get(colIndex),colIndex++);
		return colArr;
	}

	private Object[] convertVectorToArrayO(Vector columns)
	{
		// convert this column array
		Object [] colArr = new Object[columns.size()];
		for(int colIndex = 0;colIndex < columns.size();colArr[colIndex] = columns.get(colIndex),colIndex++);
		return colArr;
	}

}
