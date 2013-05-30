package prerna.ui.components;

import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.util.DIHelper;


// Core class for showing which classes are data properties vs. which are object properties

public class PropertySpecData {

	
	// for every predicate
	// get the predicate class name upto the URI
	// show only those
	// additionally, allow the user to update and add if there are others the user wants to add
	
	Hashtable <String, String> predHash = new Hashtable<String, String>();
	public String [] columnNames = {"Select", "Property"};

	public String [] columnNames2 = {"Select", "Relation"};

	public Object[][] dataList = null;
	public Object[][] dataList2 = null;
	
	Hashtable <String, String> objectHash = new Hashtable<String,String>();
	Hashtable <String, String> propHash = new Hashtable<String,String>();
	

	
	Logger logger = Logger.getLogger(getClass());
	
	public void addPredicate(String predicate)
	{
		//String className = Utility.getQualifiedClassName(predicate);
		logger.debug("Adding predicate " + predicate);
		if(!predHash.containsKey(predicate))
			predHash.put(predicate, predicate);
	}
	
	public void genPredList()
	{
		// this will read through all the predicate hash and generate the list
		Enumeration <String> keys = predHash.keys();
		// shows the boolean and predicate 
		dataList = new Object[predHash.size()][2];
		dataList2 = new Object[predHash.size()][2];
		for(int predIndex = 0;keys.hasMoreElements();predIndex++)
		{
			String key = keys.nextElement();
			dataList[predIndex][0] = new Boolean(false);
			if(propHash.containsKey(key))
			{
				logger.debug("Found the key " + key);
				dataList[predIndex][0] = new Boolean(true);
			}
			dataList[predIndex][1] = key;
			dataList2[predIndex][0] = new Boolean(false);
			if(objectHash.containsKey(key))
			{
				logger.debug("Found the key " + key);
				dataList2[predIndex][0] = new Boolean(true);
			}
			dataList2[predIndex][1] = key;
		}
	}
	
	public Object getValueAt(int row, int column)
	{
		return dataList[row][column];
	}

	public Object getValueAt2(int row, int column)
	{
		return dataList2[row][column];
	}

	public int getNumRows()
	{
		// use this call to convert the thing to array
		return dataList.length;
	}
	
	public void setValueAt(String uriVal, Object value, int row, int column)
	{
		// this will not only set the value here but also adjust the string accordingly
		dataList[row][column] = value;
		if((Boolean)value)
		{
			String soFar = DIHelper.getInstance().getProperty(uriVal);
			soFar = soFar +";"+dataList[row][1];
			DIHelper.getInstance().putProperty(uriVal, soFar);
			logger.debug(" URL " + uriVal + " VALUE >>> " + soFar);
			propHash.put(dataList[row][1]+"", dataList[row][1]+"");
		}
		else	
		{
			String soFar = DIHelper.getInstance().getProperty(uriVal);
			String replacement = "";
			String takeOut = (String)dataList[row][1];
			soFar = soFar.replace(takeOut,replacement);
			//soFar = soFar +";"+dataList[row][1];
			DIHelper.getInstance().putProperty(uriVal, soFar);			
			logger.debug(" URL " + uriVal + " VALUE >>> " + soFar);
			propHash.remove(dataList[row][1]+"");
		}
	}

	public void setValueAt2(String uriVal, Object value, int row, int column)
	{
		// this will not only set the value here but also adjust the string accordingly
		dataList2[row][column] = value;
		if((Boolean)value)
		{
			String soFar = DIHelper.getInstance().getProperty(uriVal);
			soFar = soFar +";"+dataList2[row][1];
			DIHelper.getInstance().putProperty(uriVal, soFar);
			logger.debug(" URL " + uriVal + " VALUE >>> " + soFar);
			objectHash.put(dataList2[row][1]+"", dataList2[row][1]+"");
		}
		else	
		{
			String soFar = DIHelper.getInstance().getProperty(uriVal);
			String replacement = "";
			String takeOut = (String)dataList2[row][1];
			soFar = soFar.replace(takeOut,replacement);
			//soFar = soFar +";"+dataList[row][1];
			DIHelper.getInstance().putProperty(uriVal, soFar);			
			logger.debug(" URL " + uriVal + " VALUE >>> " + soFar);
			objectHash.remove(dataList2[row][1]+"");
		}
	}
}
