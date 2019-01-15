package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class Lambda implements Iterator{
	
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
	public QueryStruct qs = null;
	Iterator <IHeadersDataRow> thisIterator = null;
	public Vector<String> strVector = null;
	public Vector<Double> decVector = null;
	
	public Vector<String> cols = null;
	public Vector<Object> filters = null;
	public Vector<Object> joins = null;
	
	// all the input strings are stored here
	public Hashtable <String, String[]> inputStore = new Hashtable<String, String[]>();
	
	
	public IHeadersDataRow RESULT = null;
	
	public void mergeQueryStruct(QueryStruct incomingQS) {
//		if
	}
	public void makeQuery()
	{
		// this will be overridden
		System.out.println("Making Query Struct");
		String tableName = "";
		if(this.frame != null)
			tableName = frame.getName();
		
		// need to think about the as here
		if(cols != null)
		{
			for(int colIndex = 0;cols != null && colIndex < cols.size();colIndex++)
			{
				String input = cols.elementAt(colIndex);
				String [] column = input.split("__");
				if(column.length == 1)
					qs.addSelector(tableName , input);
				else
					qs.addSelector(tableName , input);
			}
		}
		if(filters != null)
		{
			for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
			{
				Filter filter = (Filter)filters.elementAt(filterIndex);
				GenRowStruct values = filter.getRComparison();
				if(values.size() > 0)
				{
					// predict what the type is
					PixelDataType type = values.vector.get(1).getNounType();
					String pad = "";
					if(type == PixelDataType.CONST_STRING)
					{
						Vector <String> strVector = new Vector<String>();
						// now make this into a vector
						for(int valIndex = 1;valIndex < values.size();valIndex++)
							strVector.addElement(values.get(valIndex)+"");
						qs.addFilter(filter.getLComparison().get(0).toString(), filter.getComparator(), strVector);
					}
					else
					{
						Vector <Double> decVector = new Vector<Double>();
						for(int valIndex = 1;valIndex < values.size();valIndex++)
						{
							Object doubVal = values.get(valIndex);
							decVector.addElement(new Double(" + values.elementAt(valIndex) +"));
						}
						qs.addFilter(filter.getLComparison().get(0).toString(), filter.getComparator(), decVector);
					}
				}		
			}
		}
		if(joins != null)
		{
			for(int joinIndex = 0; joinIndex < joins.size();joinIndex++)
			{
				Join join = (Join)joins.elementAt(joinIndex);
				qs.addRelation(join.getSelector() , join.getQualifier(), join.getJoinType());
			}
		}
		if(frame != null) {
			//TODO: UPDATE FOR NEW QS
			//TODO: UPDATE FOR NEW QS
			//TODO: UPDATE FOR NEW QS
			//TODO: UPDATE FOR NEW QS
			//TODO: UPDATE FOR NEW QS
			//TODO: UPDATE FOR NEW QS

//			thisIterator = frame.query(qs);
		}
	}
	
	public void addInputs()
	{
		// this will be overridden
	}

	public IHeadersDataRow executeCode(IHeadersDataRow row)
	{
		// this will be overridden
		return null;
	}

	public void setVar(String variableName, Object value)
	{
		store.put(variableName, value);
	}
	
	public void addStore(Hashtable <String, Object> newStore)
	{
		Enumeration <String> keys = newStore.keys();
		while(keys.hasMoreElements())
		{
			String key = keys.nextElement();
			this.store.put(key, newStore.get(key));
		}
	}
	
	public void setFrame(ITableDataFrame frame)
	{
		this.frame = frame;
	}

	public Object getVar(String variableName)
	{
		return store.get(variableName);
	}
	
	public NounMetadata execute()
	{
		// run the query
		// get the headerrow
		// call the map with every headerrow
		// this iterator needs to be chhanged to something different
		// need something to get the headers so I know the cardinality eventually
		if(thisIterator == null)
		{
			System.out.println("NOthing to process");
			executeCode(null);
			return null;
		}
		while(thisIterator.hasNext())
		{
			IHeadersDataRow row = (IHeadersDataRow)thisIterator.next();
			executeCode(row);
		}
		return null;
	}
	
	public boolean hasNext()
	{
		return thisIterator.hasNext();
	}
	
	public IHeadersDataRow next()
	{
		IHeadersDataRow row = (IHeadersDataRow)thisIterator.next();
		return executeCode(row);
	}
	
	// for now I am assuming the curRow is a hashtable
	public Object[] getRow(String inputStringRef, IHeadersDataRow curRow)
	{
		String [] inputStrings = inputStore.get(inputStringRef);
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
	
	public void test()
	{
		System.out.println("Hello Lambda !!");
	}
}
