/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Functions needed for determining Similarity functionality.
 */
public class SimilarityFunctions {

	ArrayList <Object []> list;
	ISelectWrapper wrapper;
	ArrayList <String> comparisonObjectList;
	protected static final Logger logger = LogManager.getLogger(SimilarityFunctions.class.getName());
	final static String COUNT = "Count";
	public final static String VALUE = "Value";

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
		wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*wrapper = new SesameJenaSelectWrapper();
		if(engine!= null){
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
		}*/

		// get the bindings from it
		String [] names;
		try
		{
			names = wrapper.getVariables();
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+dbName);
			return;
		}

		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();

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
		} catch (RuntimeException e) {
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
		createTable(dbName, dataQuery);
		Hashtable<String, Hashtable<String,String>> dataBLUHash = makeDataHash();
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
		Hashtable<String, Hashtable<String,Double>> dataRetHash = makeComparisonWithCRM(dataBLUHash,option);

		return dataRetHash;
	}
	
	public Hashtable<String, Hashtable<String,String>> makeDataHash()
	{
		Hashtable<String, Hashtable<String,String>> dataHash = new Hashtable<String, Hashtable<String,String>>();
		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String sysName = (String) listElement[0];
			String dataName = (String) listElement[1];
			String crm = (String) listElement[2];
			if(dataHash.get(sysName) != null)
			{
				Hashtable<String,String> sysSpecDataHash= (Hashtable<String,String>) dataHash.get(sysName);
				sysSpecDataHash.put(dataName, crm.replace("\"", ""));
			}
			else
			{
				Hashtable<String,String> sysSpecDataHash = new Hashtable<String,String>();
				sysSpecDataHash.put(dataName, crm.replace("\"", ""));
				dataHash.put(sysName,  sysSpecDataHash);
			}
		}
		return dataHash;
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
	public Hashtable<String, Hashtable<String,Double>> getDataSet(String dbName, String dataQuery,String option)
	{
		createTable(dbName, dataQuery);

		//because in data's case, we also have crm, we need to append taht information
		Hashtable<String, Hashtable<String,String>> dataBLUHash = makeDataHash();
		Hashtable<String, Hashtable<String,Double>> dataRetHash = makeComparisonWithCRM(dataBLUHash,option);

		return dataRetHash;
	}
	
	public Hashtable<String, Hashtable<String,Double>> makeComparisonWithCRM(Hashtable<String, Hashtable<String,String>> dataBLUHash, String option)
	{
		Hashtable<String, Hashtable<String,Double>> dataRetHash = new Hashtable<String, Hashtable<String,Double>>();
		for(int i=0; i<comparisonObjectList.size();i++)
		{
			String sysName = comparisonObjectList.get(i);
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
	 * Generic function that can compare a given property of a comparison object given three choices where doubleOverlap fulfills the first two
	 * 
	 * @param dbName String					Name of database to be queried
	 * @param query String					Query to be run against db
	 * @param valueCheckA String			Value for var A to be checked against
	 * @param valueCheckB String			Value for var B to be checked against
	 * @param doubleOverlapCheck String		Value for vars A & B to be checked against
	
	 * @return Hashtable					Table of comparison values
	 */
	public Hashtable stringCompareBinaryResultGetter(String dbName, String query, String valueCheckA, String valueCheckB, String doubleOverlapCheck)
	{
		//TODO: Comments, var name refactoring
		Hashtable<String, Hashtable<String,Double>> dataRetHash = new Hashtable<String, Hashtable<String,Double>>();
		createTable(dbName, query);
		//first create hashtable of overall ObjectToCompareArray
		Hashtable<String,String> valueHash = new Hashtable<String,String>();

		//first match all data with the original object to compare list
		for(int i=0; i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String comparisonObjectName = (String) listElement[0];
			String value = ((String) listElement[1]).replace("\"","");
			if(comparisonObjectList.contains(comparisonObjectName))
			{
				valueHash.put(comparisonObjectName, value );
			}
		}

		//iterate through the newly populated comparisonObjectList
		for(Entry<String,String> e : valueHash.entrySet()) 
		{
			String comparisonObjectName = e.getKey();
			String valueA = e.getValue();
			boolean comparisonObjectBooleanA = valueA.equals(valueCheckA);
			boolean comparisonObjectBooleanB = valueA.equals(valueCheckB);
			boolean doubleBoolean = valueA.equals(doubleOverlapCheck);
			Hashtable<String,Double> comparisonObjectElementHash =  new Hashtable<String,Double>();
			//go through all data from query and find the match scores
			for(int j=0;j<list.size();j++)
			{
				Object[] listElement2 = list.get(j);
				String comparisonObjectName2 = (String) listElement2[0];
				String valueB = ((String) listElement2[1]).replace("\"","");
				boolean comparisonObjectBooleanA2 = valueB.equals(valueCheckA);
				boolean comparisonObjectBooleanB2 = valueB.equals(valueCheckB);
				boolean doubleBoolean2 = valueB.equals(doubleOverlapCheck);
				double score = 0.0;
				//only if both match one way or another
				boolean a_a = comparisonObjectBooleanA && comparisonObjectBooleanA2;
				boolean b_b = comparisonObjectBooleanB && comparisonObjectBooleanB2;
				boolean c_a = doubleBoolean && comparisonObjectBooleanA2;
				boolean c_b = doubleBoolean && comparisonObjectBooleanB2;
				boolean c_c = doubleBoolean && doubleBoolean2;

				//only give score of 1.0 if both objects compared are value a, b, c, OR object of interest is a or b and object replaced is c 
				if (a_a || b_b || c_a || c_b || c_c)
				{
					score = 1.0;
				}
				if (comparisonObjectName.equals(comparisonObjectName2))
				{
					score = 1.0;
				}
				comparisonObjectElementHash.put(comparisonObjectName2, score);
				dataRetHash.put(comparisonObjectName,  comparisonObjectElementHash);
			}

		}
		return dataRetHash;
	}	
	
	/**
	 * Get data/actual percentage related to the similarity of the parameter
	 * 
	 * @param dbName String		Name of the database
	 * @param query String		Query to be run against db
	 * @param option String		Option to calculate Count or Value
	 * 
	 * @return Hashtable		Values for similarity of parameter
	 */
	public Hashtable compareObjectParameterScore(String dbName, String query, String option)
	{
		//query put in has to be

		createTable(dbName, query);
		Hashtable<String, Hashtable<String,Double>> dataRetHash = new Hashtable<String, Hashtable<String,Double>>();
		Hashtable<String, ArrayList<String>> dataStoreHash = new Hashtable<String, ArrayList<String>>();

		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String comparisonObjectName = (String) listElement[0];
			String elementName = (String) listElement[1];
			if(dataStoreHash.get(comparisonObjectName) != null)
			{
				ArrayList<String> elementArray= dataStoreHash.get(comparisonObjectName);
				elementArray.add(elementName);
			}
			else
			{
				ArrayList<String> elementArray = new ArrayList<String>();
				elementArray.add(elementName);
				dataStoreHash.put(comparisonObjectName,  elementArray);
			}
		}
		for(int i=0; i<comparisonObjectList.size();i++)
		{
			String comparisonObjectName = comparisonObjectList.get(i);
			if (dataStoreHash.containsKey(comparisonObjectName))
			{
				ArrayList<String> currentComparisonObjectList =  dataStoreHash.get(comparisonObjectName);
				Hashtable<String,Double> comparisonObjectElementHash =  new Hashtable<String,Double>();
				double totalElement = currentComparisonObjectList.size();
				for(Entry<String,ArrayList<String>> comparisonObjectArrayEntry : dataStoreHash.entrySet()) 
				{
				    String comparisonObjectName2 = comparisonObjectArrayEntry.getKey();
				    ArrayList<String> otherComparisonObjectList = comparisonObjectArrayEntry.getValue();
				    double matchingElement = 0;
				    for(int elementIdx=0;elementIdx<currentComparisonObjectList.size();elementIdx++) 
				    {
				    	String element = currentComparisonObjectList.get(elementIdx);
			    		if(otherComparisonObjectList.contains(element))
			    		{
		    				matchingElement++;
			    		}

				    }
				    
				    if(option.equals("Count"))
				    {
				    	comparisonObjectElementHash.put(comparisonObjectName2, matchingElement);
				    }
				    if(option.equals("Value"))
				    {
				    	double score = matchingElement/totalElement;
				    	comparisonObjectElementHash.put(comparisonObjectName2, score);
				    }
					dataRetHash.put(comparisonObjectName,  comparisonObjectElementHash);
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
	public Object getVariable(String varName, ISelectStatement sjss)
	{
		return sjss.getVar(varName);
	}
	
	/**
	 * Creates list of comparison objects from database.
	 * 
	 * @param database String	Name of database
	 * @param query String		Query to be run to get list of comparison objects
	 * 
	 * @return ArrayList<String>	List of comparison objects from database
	 */
	public ArrayList<String> createComparisonObjectList(String database, String query)
	{
		createTable(database, query);
		ArrayList<String> retList = new ArrayList<String>();
		//first query returns one column
		for (int i=0;i<list.size();i++)
		{
			Object[] listElement = list.get(i);
			String comparisonObjectName = (String) listElement[0];
			retList.add(comparisonObjectName);
		}
		Collections.sort(retList);
		return retList;
	}

	/**
	 * Sets list of comparison objects.
	 * 
	 * @param comparisonObjectList ArrayList<String>	List of comparison objects to be set
	 */
	public void setComparisonObjectList(ArrayList<String> comparisonObjectList)
	{
		this.comparisonObjectList=comparisonObjectList;
	}
	
	//System-Capability Comparison Function
	
	/**
     * Get data/actual percentage related to the similarity of the parameter
     * 
      * @param dbName String           Name of the database
     * @param query String            Query to be run against db
     * @param option String           Option to calculate Count or Value
     * 
      * @return Hashtable       Values for similarity of parameter
     */
     public Hashtable compareDifferentObjectParameterScore(String dbName, String query, String option)
     {
            createTable(dbName, query);
            Hashtable<String, Hashtable<String,Double>> dataRetHash = new Hashtable<String, Hashtable<String,Double>>();
            Hashtable<String, ArrayList<String>> dataStoreHash = new Hashtable<String, ArrayList<String>>();
            Hashtable<String, ArrayList<String>> systemStoreHash = new Hashtable<String, ArrayList<String>>();

            for (int i=0;i<list.size();i++)
            {
                   Object[] listElement = list.get(i);
                   String comparisonObjectName = (String) listElement[0];
                   String elementName = (String) listElement[2];
                   if(dataStoreHash.get(comparisonObjectName) != null)
                   {
                         ArrayList<String> elementArray= dataStoreHash.get(comparisonObjectName);
                         elementArray.add(elementName);
                   }
                   else
                   {
                         ArrayList<String> elementArray = new ArrayList<String>();
                         elementArray.add(elementName);
                         dataStoreHash.put(comparisonObjectName,  elementArray);
                   }
                   if (!(listElement[1].equals("NA"))) {
	                   String comparisonSystemName = (String) listElement[1];
	                   if(systemStoreHash.get(comparisonSystemName) != null)
	                   {
	                         ArrayList<String> elementArray= systemStoreHash.get(comparisonSystemName);
	                         elementArray.add(elementName);
	                   }
	                   else
	                   {
	                         ArrayList<String> elementArray = new ArrayList<String>();
	                         elementArray.add(elementName);
	                         systemStoreHash.put(comparisonSystemName,  elementArray);
	                   }
                   }
            }
            for(int i=0; i<comparisonObjectList.size();i++) //this will be my capability list
            {
                   String comparisonObjectName = comparisonObjectList.get(i);
                   if (dataStoreHash.containsKey(comparisonObjectName))
                   {
                         ArrayList<String> currentComparisonObjectList =  dataStoreHash.get(comparisonObjectName);
                         Hashtable<String,Double> comparisonObjectElementHash =  new Hashtable<String,Double>();
                         double totalElement = currentComparisonObjectList.size();
                         for(Entry<String,ArrayList<String>> comparisonObjectArrayEntry : systemStoreHash.entrySet()) 
                         {
                             String comparisonObjectName2 = comparisonObjectArrayEntry.getKey();
                             ArrayList<String> otherComparisonObjectList = comparisonObjectArrayEntry.getValue();
                             double matchingElement = 0;
                             for(int elementIdx=0;elementIdx<currentComparisonObjectList.size();elementIdx++) 
                             {
                                String element = currentComparisonObjectList.get(elementIdx);
                                 if(otherComparisonObjectList.contains(element))
                                 {
                                       matchingElement++;
                                 }

                             }
                             
                             if(option.equals("Count"))
                             {
                                comparisonObjectElementHash.put(comparisonObjectName2, matchingElement);
                             }
                             if(option.equals("Value"))
                             {
                                double score = matchingElement/totalElement;
                                comparisonObjectElementHash.put(comparisonObjectName2, score);
                             }
                                dataRetHash.put(comparisonObjectName,  comparisonObjectElementHash);
                         }
                   }

            }
            return dataRetHash;
     }

}
