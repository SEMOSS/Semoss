/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

/**
 * Functions needed for System Duplication functionality.
 */
public class SysDupeFunctions {

	ArrayList <Object []> list;
	SesameJenaSelectWrapper wrapper;
	ArrayList <String> sysList;
	protected Logger logger = Logger.getLogger(getClass());
	final static String COUNT = "Count";
	final static String VALUE = "Value";

	/**
	 * Populates class-level 'list' var with binding values retrieved from running input query.
	 * 
	 * @param dbName String		Name of database to be queried
	 * @param query String		Query to be run against db
	 */
	public void createTable(String dbName, String query)
	{
		// uses the engine to create the sparql result
		// once created find the binding names
		// compose a array
		// and then create filter data and a table
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(dbName);
		list = new ArrayList();
		wrapper = new SesameJenaSelectWrapper();
		if(engine!= null){
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
		}

		// get the bindings from it
		String [] names = wrapper.getVariables();

		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();

				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = getVariable(names[colIndex], sjss);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}

	/**
	 * Since data and blu get added together needed to create a custom query function that did all of this.
	 * Returns combined list of DataObject and BLUs.
	 * 
	 * @param dbName String		Name of database to be queried
	 * @param dataQuery String	Query to retrieve DataObjects
	 * @param bluQuery String	Query to retrieve BLUs
	 * @param option String		Option to calculate Count or Value
	
	 * @return Hashtable<String,Hashtable<String,Double>>	System-specific list of scores/values
	 */
	public Hashtable<String, Hashtable<String,Double>> getDataBLUDataSet(String dbName, String dataQuery, String bluQuery, String option)
	{
		//first create hashtable of arraylist with system as key and corresponding data + blu as the values
		Hashtable<String, Hashtable<String,Double>> dataRetHash = new Hashtable<String, Hashtable<String,Double>>();
		Hashtable<String, Hashtable<String,String>> dataBLUHash = new Hashtable<String, Hashtable<String,String>>();
		createTable(dbName, dataQuery);

		//because in data's case, we also have crm, we need to append taht information
		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String sysName = (String) listElement[0];
			String dataName = (String) listElement[1];
			String crm = (String) listElement[2];
			if(dataBLUHash.get(sysName) != null)
			{
				Hashtable<String,String> sysSpecDataBLUHash= (Hashtable<String,String>) dataBLUHash.get(sysName);
				sysSpecDataBLUHash.put(dataName, crm.replace("\"", ""));
			}
			else
			{
				Hashtable<String,String> sysSpecDataBLUHash = new Hashtable<String,String>();
				sysSpecDataBLUHash.put(dataName, crm.replace("\"", ""));
				dataBLUHash.put(sysName,  sysSpecDataBLUHash);
			}
		}
		createTable(dbName, bluQuery);
		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String sysName = (String) listElement[0];
			String bluName = (String) listElement[1];
			//make all blus create so the format matches the data object
			if(dataBLUHash.get(sysName) != null)
			{
				Hashtable<String,String> sysSpecDataBLUHash= (Hashtable<String,String>) dataBLUHash.get(sysName);
				sysSpecDataBLUHash.put(bluName, "C");
			}
			else
			{
				Hashtable<String,String> sysSpecDataBLUHash = new Hashtable<String,String>();
				sysSpecDataBLUHash.put(bluName, "C");
				dataBLUHash.put(sysName,  sysSpecDataBLUHash);
			}
		}
		for(int i=0; i<sysList.size();i++)
		{
			String sysName = sysList.get(i);
			if (dataBLUHash.containsKey(sysName))
			{
				Hashtable<String,String> currentSysHash =  dataBLUHash.get(sysName);
				Hashtable<String,Double> sysElementHash =  new Hashtable<String,Double>();
				double totalElement = currentSysHash.size();
				for(Entry<String,Hashtable<String,String>> e : dataBLUHash.entrySet()) 
				{
				    String sys = e.getKey();
				    Hashtable<String,String> otherSysHash = e.getValue();
				    double matchingElement = 0;
				    for(Entry<String,String> e2 : currentSysHash.entrySet()) 
				    {
				    	String element = e2.getKey();
				    	String elementCRM = e2.getValue();
			    		if(otherSysHash.containsKey(element))
			    		{
			    			if(elementCRM.equals(otherSysHash.get(element)))
			    			{
			    				matchingElement++;
			    			}
			    			else
			    			{
			    				String otherElementCRM = otherSysHash.get(element);
			    				boolean readSatisfy = otherElementCRM!=null && elementCRM.equals("R") && (otherElementCRM.equals("C") || otherElementCRM.equals("M"));
			    				boolean modifySatisfy = otherElementCRM!=null && elementCRM.equals("M") && otherElementCRM.equals("C");
			    				if(readSatisfy || modifySatisfy)
	    						{
			    					matchingElement++;
	    						}
			    			
				    		}
			    		}

				    }
				    
				    if(option.equals("Count"))
				    {
				    	sysElementHash.put(sys, matchingElement);
				    }
				    if(option.equals("Value"))
				    {
				    	double score = matchingElement/totalElement;
				    	sysElementHash.put(sys, score);
				    }
					dataRetHash.put(sysName,  sysElementHash);
				}
			}

		}

		return dataRetHash;
	}


	/**
	 * Generic function that can compare a given property of a system given three choices where doubleOverlap fulfills the first two
	 * 
	 * @param dbName String					Name of database to be queried
	 * @param query String					Query to be run against db
	 * @param valueCheckA String			Value for var A to be checked against
	 * @param valueCheckB String			Value for var B to be checked against
	 * @param doubleOverlapCheck String		Value for vars A & B to be checked against
	
	 * @return Hashtable					Table of system-specific comparison values
	 */
	public Hashtable stringCompareBinaryResultGetter(String dbName, String query, String valueCheckA, String valueCheckB, String doubleOverlapCheck)
	{
		//TODO: Comments, var name refactoring
		Hashtable<String, Hashtable<String,Double>> dataRetHash = new Hashtable<String, Hashtable<String,Double>>();
		createTable(dbName, query);
		//first create hashtable of overall SysArray
		Hashtable<String,String> valueHash = new Hashtable<String,String>();

		//first match all data with the original system list
		for(int i=0; i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String sysName = (String) listElement[0];
			String value = ((String) listElement[1]).replace("\"","");
			if(sysList.contains(sysName))
			{
				valueHash.put(sysName, value );
			}
		}

		//iterate through the newly populated syslist
		for(Entry<String,String> e : valueHash.entrySet()) 
		{
			String sysName = e.getKey();
			String valueA = e.getValue();
			boolean sysBooleanA = valueA.equals(valueCheckA);
			boolean sysBooleanB = valueA.equals(valueCheckB);
			boolean doubleBoolean = valueA.equals(doubleOverlapCheck);
			Hashtable<String,Double> sysElementHash =  new Hashtable<String,Double>();
			//go through all data from query and find the match scores
			for(int j=0;j<list.size();j++)
			{
				Object[] listElement2 = list.get(j);
				String sysName2 = (String) listElement2[0];
				String valueB = ((String) listElement2[1]).replace("\"","");
				boolean sysBooleanA2 = valueB.equals(valueCheckA);
				boolean sysBooleanB2 = valueB.equals(valueCheckB);
				boolean doubleBoolean2 = valueB.equals(doubleOverlapCheck);
				double score = 0.0;
				//only if both match one way or another
				boolean a_a = sysBooleanA && sysBooleanA2;
				boolean b_b = sysBooleanB && sysBooleanB2;
				boolean c_a = doubleBoolean && sysBooleanA2;
				boolean c_b = doubleBoolean && sysBooleanB2;
				boolean c_c = doubleBoolean && doubleBoolean2;

				//only give score of 1.0 if both systems are value a, b, c, OR system of interest is a or b and system replaced is c 
				if (a_a || b_b || c_a || c_b || c_c)
				{
					score = 1.0;
				}
				if (sysName.equals(sysName2))
				{
					score = 1.0;
				}
		    	sysElementHash.put(sysName2, score);
				dataRetHash.put(sysName,  sysElementHash);
			}

		}
		return dataRetHash;
	}	

	//TODO: Unused method?
	/**
	 * Gets all the counts for how much another system duplicates the given system in raw values.
	 * 
	 * @param dbName String		Name of database to query
	 * @param query String		Query to be run against db
	 */
	private void compareSystemParameterCount(String dbName, String query)
	{
		//query put in has to be

		createTable(dbName, query);
		Hashtable<String, Hashtable<String,String>> dataRetHash = new Hashtable<String, Hashtable<String,String>>();
		Hashtable<String, ArrayList<String>> dataStoreHash = new Hashtable<String, ArrayList<String>>();

		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String sysName = (String) listElement[0];
			String elementName = (String) listElement[1];
			if(dataStoreHash.get(sysName) != null)
			{
				ArrayList<String> elementArray= dataStoreHash.get(sysName);
				elementArray.add(elementName);
			}
			else
			{
				ArrayList<String> elementArray = new ArrayList<String>();
				elementArray.add(elementName);
				dataStoreHash.put(sysName,  elementArray);
			}
		}

		for(Entry<String,ArrayList<String>> e : dataStoreHash.entrySet()) 
		{
			String sysName = e.getKey();
			ArrayList<String> valueA = e.getValue();
		}
	}
	
	/**
	 * Get data/actual percentage related to the duplication of the parameter
	 * 
	 * @param dbName String		Name of the database
	 * @param query String		Query to be run against db
	 * @param option String		Option to calculate Count or Value
	 * 
	 * @return Hashtable		Values for duplication of parameter
	 */
	public Hashtable compareSystemParameterScore(String dbName, String query, String option)
	{
		//query put in has to be

		createTable(dbName, query);
		Hashtable<String, Hashtable<String,Double>> dataRetHash = new Hashtable<String, Hashtable<String,Double>>();
		Hashtable<String, ArrayList<String>> dataStoreHash = new Hashtable<String, ArrayList<String>>();

		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String sysName = (String) listElement[0];
			String elementName = (String) listElement[1];
			if(dataStoreHash.get(sysName) != null)
			{
				ArrayList<String> elementArray= dataStoreHash.get(sysName);
				elementArray.add(elementName);
			}
			else
			{
				ArrayList<String> elementArray = new ArrayList<String>();
				elementArray.add(elementName);
				dataStoreHash.put(sysName,  elementArray);
			}
		}
		for(int i=0; i<sysList.size();i++)
		{
			String sysName = sysList.get(i);
			if (dataStoreHash.containsKey(sysName))
			{
				ArrayList<String> currentSysList =  dataStoreHash.get(sysName);
				Hashtable<String,Double> sysElementHash =  new Hashtable<String,Double>();
				double totalElement = currentSysList.size();
				for(Entry<String,ArrayList<String>> sysArrayEntry : dataStoreHash.entrySet()) 
				{
				    String sysName2 = sysArrayEntry.getKey();
				    ArrayList<String> otherSysList = sysArrayEntry.getValue();
				    double matchingElement = 0;
				    for(int elementIdx=0;elementIdx<currentSysList.size();elementIdx++) 
				    {
				    	String element = currentSysList.get(elementIdx);
			    		if(otherSysList.contains(element))
			    		{
		    				matchingElement++;
			    		}

				    }
				    
				    if(option.equals("Count"))
				    {
				    	sysElementHash.put(sysName2, matchingElement);
				    }
				    if(option.equals("Value"))
				    {
				    	double score = matchingElement/totalElement;
				    	sysElementHash.put(sysName2, score);
				    }
					dataRetHash.put(sysName,  sysElementHash);
				}
			}

		}
		return dataRetHash;
	}

	/**
	 * Gets variable value from wrapper
	 * 
	 * @param varName String					Name of variable to retrieve
	 * @param sjss SesameJenaSelectStatement	SelectStatement wrapper to retrieve var from
	 * 
	 * @return Object	Value of the var
	 */
	public Object getVariable(String varName, SesameJenaSelectStatement sjss)
	{
		return sjss.getVar(varName);
	}
	
	/**
	 * Creates list of systems from database.
	 * 
	 * @param database String	Name of database
	 * @param query String		Query to be run to get list of systems
	 * 
	 * @return ArrayList<String>	List of systems from database
	 */
	public ArrayList<String> createSystemList(String database, String query)
	{
		createTable(database, query);
		ArrayList<String> retList = new ArrayList<String>();
		//first query returns one column
		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String sysName = (String) listElement[0];
			retList.add(sysName);
		}
		Collections.sort(retList);
		return retList;
	}

	/**
	 * Sets list of systems.
	 * 
	 * @param sysList ArrayList<String>	List of systems to be set
	 */
	public void setSysList(ArrayList<String> sysList)
	{
		this.sysList=sysList;
	}
	
	//TODO: Unused method?
	/**
	 * Gets list of systems.
	
	 * @return ArrayList<String>	List of systems to be returned
	 */
	public ArrayList<String> getSysList()
	{
		return sysList;
	}
}
