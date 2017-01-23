package prerna.sablecc2.om;

import java.util.Hashtable;
import java.util.Iterator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;

public class MapFunction {
	
	// this is the map class - yeah well no shit
	// eventually we can make it extend the whole spark stuff
	// First is the options object which can be accessed through 
	// getOption
	// The input is always a row iterator IHeadersDataRow
	// seems like I need a new implementation for it
	// I need a set query function which will pull all the data that is needed
	// The function will work through each of the information
	// store is where you can keep variable outside of the map if you want to

	public Hashtable <String, Object> options = new Hashtable <String, Object>();
	public Hashtable <String, Object> store = new Hashtable <String, Object>();
	public String query = null;
	public ITableDataFrame frame = null;
	
	
	public IHeadersDataRow RESULT = null;
	
	public void setVar(String variableName, Object value)
	{
		store.put(variableName, value);
	}

	public Object getVar(String variableName)
	{
		return store.get(variableName);
	}
	
	public void execute()
	{
		// run the query
		// get the headerrow
		// call the map with every headerrow
		// this iterator needs to be chhanged to something different
		Iterator <IHeadersDataRow> iter = frame.query("abcd");
		// need something to get the headers so I know the cardinality eventually
		while(iter.hasNext())
		{
			IHeadersDataRow row = (IHeadersDataRow)iter.next();
		}
	}
	
	// for now I am assuming the curRow is a hashtable
	public Object[] getRow(String [] inputStrings, IHeadersDataRow curRow)
	{
		Object [] retRow = new Object[inputStrings.length];
		// aligns it based on the input string
		// need to take all the data and align the values based on inputStrings
		for(int inputIndex = 0;inputIndex < inputStrings.length;inputIndex++)
		{
			retRow[inputIndex] = curRow.getField(inputStrings[inputIndex]);

			// try to set it from store if it is null 
			if(retRow[inputIndex] == null)
				retRow[inputIndex] = store.get(inputStrings[inputIndex]);
		}
		
		return retRow;
	}
	
	
	public Object[] map(Object [] row)
	{
		// this is where the code would sit
		// I would actually put this method
		Object [] retRow = row;
		// take the existing data and format it in a way that is useful for us to send it for processing
		// then send it back
		
		
		return retRow;
	}
	
}
