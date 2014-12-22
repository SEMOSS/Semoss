/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.comparison.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

/**
 * Used to create ArrayLists of Object Arrays holding comparative and duplicate data for databases
 * @author kepark
 *
 */
public class GenericDBComparer
{
	private static final Logger LOGGER = LogManager.getLogger(GenericDBComparer.class.getName());
	
	private IEngine newDB;
	private IEngine oldDB;
	private IEngine newMetaDB;
	private IEngine oldMetaDB;
	
	private String newDBName;
	private String oldDBName;
	
	public GenericDBComparer(IEngine newDB, IEngine oldDB, IEngine newMetaDB, IEngine oldMetaDB) throws EngineException
	{
		this.newDB = newDB;
		this.oldDB = oldDB;
		this.newMetaDB = newMetaDB;
		this.oldMetaDB = oldMetaDB;
		
		this.newDBName = newDB.getEngineName();
		this.oldDBName = oldDB.getEngineName();
	}
	
	/**
	 * Compares the number of Instances for instance level or Relations/Properties for metamodel level existing for a
	 * Concept
	 * 
	 * @param query
	 *            String form of query to be run
	 * @param isMeta
	 *            true if the method is used to query the OWL file i.e. metamodel level. false if the method is used to
	 *            query at the instance level
	 * @return ArrayList of Object arrays, each array represents a row to be printed into excel
	 */
	public ArrayList<Object[]> compareConceptCount(String query, boolean isMeta)
	{
		ArrayList<Object[]> finalComparison = new ArrayList<Object[]>();
		ArrayList<Object[]> newDBList = new ArrayList<Object[]>();
		ArrayList<Object[]> oldDBList = new ArrayList<Object[]>();
		Object[] row = null;
		
		SesameJenaSelectWrapper newSjsw = null;
		SesameJenaSelectWrapper oldSjsw = null;
		
		if (isMeta)
		{
			newSjsw = Utility.processQuery(newMetaDB, query);
			oldSjsw = Utility.processQuery(oldMetaDB, query);
		} else
		{
			newSjsw = Utility.processQuery(newDB, query);
			oldSjsw = Utility.processQuery(oldDB, query);
		}
		
		String[] newValues = newSjsw.getVariables();
		String[] oldValues = oldSjsw.getVariables();
		
		while (newSjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = newSjsw.next();
			// Index 0 is for concept, 1 is for count
			Object[] tempCount = { sjss.getRawVar(newValues[0]), sjss.getVar(newValues[1]) };
			newDBList.add(tempCount);
		}
		while (oldSjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = oldSjsw.next();
			// Index 0 is for concept, 1 is for count
			Object[] tempCount = { sjss.getRawVar(oldValues[0]), sjss.getVar(oldValues[1]) };
			oldDBList.add(tempCount);
		}
		
		for (int n = 0; n < newDBList.size(); n++)
		{
			boolean matchFound = false;
			for (int o = 0; o < oldDBList.size(); o++)
			{
				// if concept and counts are exactly the same
				if (newDBList.get(n)[0].toString().equals(oldDBList.get(o)[0].toString())
						&& newDBList.get(n)[1].toString().equals(oldDBList.get(o)[1].toString()))
				{
					matchFound = true;
					oldDBList.remove(o);
					break;
				}
				// concepts are same, but count is different
				else if (newDBList.get(n)[0].toString().equals(oldDBList.get(o)[0].toString()))
				{
					matchFound = true;
					System.out.println("Mismatch in " + newDBList.get(n)[0] + ". New DB has " + newDBList.get(n)[1] + ". Old DB has "
							+ oldDBList.get(o)[1]);
					row = new Object[] { newDBList.get(n)[0], newDBList.get(n)[1], oldDBList.get(o)[1], "" };
					finalComparison.add(row);
					oldDBList.remove(o);
					break;
				}
			}
			// Things in new DB not found in old DB were newly added
			if (!matchFound)
			{
				System.out.println("New object added: " + newDBList.get(n)[0] + ". Number added: " + newDBList.get(n)[1]);
				row = new Object[] { newDBList.get(n)[0], newDBList.get(n)[1], "empty", "added to " + newDBName };
				finalComparison.add(row);
				newDBList.remove(n);
				n--;
			}
		}
		// This for loop is necessary to find things removed from old db
		for (int o = 0; o < oldDBList.size(); o++)
		{
			System.out.println("Old object removed: " + oldDBList.get(o)[0] + ". Number removed: " + oldDBList.get(o)[1]);
			row = new Object[] { oldDBList.get(o)[0], "empty", oldDBList.get(o)[1], "removed from " + oldDBName };
			finalComparison.add(row);
		}
		
		if (finalComparison.isEmpty())
		{
			System.out.println("No Changes.");
			row = new Object[] { "No Changes.", "", "", "" };
			finalComparison.add(row);
		}
		
		return finalComparison;
	}
	
	/**
	 * Used to compare counts of data elements at the instance level such as relationships and properties.
	 * 
	 * @param query
	 * @return ArrayList of Object arrays, each array represents a row to be printed into excel
	 */
	public ArrayList<Object[]> compareInstanceCount(String query)
	{
		ArrayList<Object[]> finalComparison = new ArrayList<Object[]>();
		ArrayList<Object[]> newDBList = new ArrayList<Object[]>();
		ArrayList<Object[]> oldDBList = new ArrayList<Object[]>();
		Object[] row = null;
		
		SesameJenaSelectWrapper newSjsw = Utility.processQuery(newDB, query);
		String[] newValues = newSjsw.getVariables();
		SesameJenaSelectWrapper oldSjsw = Utility.processQuery(oldDB, query);
		String[] oldValues = oldSjsw.getVariables();
		
		while (newSjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = newSjsw.next();
			// Index 0 is for concept, 1 is for instance, 2 is for count
			Object[] tempCount = { sjss.getRawVar(newValues[0]), sjss.getRawVar(newValues[1]), sjss.getVar(newValues[2]) };
			newDBList.add(tempCount);
		}
		while (oldSjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = oldSjsw.next();
			// Index 0 is for concept, 1 is for instance, 2 is for count
			Object[] tempCount = { sjss.getRawVar(oldValues[0]), sjss.getRawVar(oldValues[1]), sjss.getVar(newValues[2]) };
			oldDBList.add(tempCount);
		}
		
		for (int n = 0; n < newDBList.size(); n++)
		{
			boolean matchFound = false;
			for (int o = 0; o < oldDBList.size(); o++)
			{
				if (newDBList.get(n)[0].toString().equals(oldDBList.get(o)[0].toString())
						&& newDBList.get(n)[1].toString().equals(oldDBList.get(o)[1].toString())
						&& newDBList.get(n)[2].toString().equals(oldDBList.get(o)[2].toString()))
				{
					matchFound = true;
					oldDBList.remove(o);
					break;
				} else if (newDBList.get(n)[0].toString().equals(oldDBList.get(o)[0].toString())
						&& newDBList.get(n)[1].toString().equals(oldDBList.get(o)[1].toString()))
				{
					matchFound = true;
					System.out.println("Mismatch in " + newDBList.get(n)[0] + "~" + newDBList.get(n)[1] + ". New DB has " + newDBList.get(n)[2]
							+ ". Old DB has " + oldDBList.get(o)[2]);
					row = new Object[] { newDBList.get(n)[0], newDBList.get(n)[1], newDBList.get(n)[2], oldDBList.get(o)[2], "" };
					finalComparison.add(row);
					oldDBList.remove(o);
					break;
				}
			}
			if (!matchFound)
			{
				System.out.println("New object added: " + newDBList.get(n)[0] + "~" + newDBList.get(n)[1] + ". Number added: " + newDBList.get(n)[2]);
				row = new Object[] { newDBList.get(n)[0], newDBList.get(n)[1], newDBList.get(n)[2], "empty", "added to " + newDBName };
				finalComparison.add(row);
				newDBList.remove(n);
				n--;
			}
		}
		// This for loop is necessary to only find things removed from previous db
		for (int o = 0; o < oldDBList.size(); o++)
		{
			System.out.println("Old object removed: " + oldDBList.get(o)[0] + "~" + oldDBList.get(o)[1] + ". Number removed: " + oldDBList.get(o)[2]);
			row = new Object[] { oldDBList.get(o)[0], oldDBList.get(o)[1], "empty", oldDBList.get(o)[2], "removed from " + oldDBName };
			finalComparison.add(row);
		}

		if (finalComparison.isEmpty())
		{
			System.out.println("No Changes.");
			row = new Object[] { "No Changes.", "", "", "", "" };
			finalComparison.add(row);
		}
		
		return finalComparison;
	}
	
	/**
	 * Used for total count comparisons at the metamodel level
	 * 
	 * @param query
	 * @return ArrayList of Object arrays, each array represents a row to be printed into excel
	 */
	public ArrayList<Object[]> compareMetaSingleCount(String query)
	{
		ArrayList<Object[]> finalComparison = new ArrayList<Object[]>();
		
		SesameJenaSelectWrapper newSjsw = Utility.processQuery(newMetaDB, query);
		SesameJenaSelectWrapper oldSjsw = Utility.processQuery(oldMetaDB, query);
		
		String[] newValues = newSjsw.getVariables();
		String[] oldValues = oldSjsw.getVariables();
		
		SesameJenaSelectStatement newSjss = newSjsw.next();
		SesameJenaSelectStatement oldSjss = oldSjsw.next();
		Object[] tempCount = { newSjss.getVar(newValues[0]), oldSjss.getVar(oldValues[0]) };
		finalComparison.add(tempCount);
		
		return finalComparison;
	}
	
	public ArrayList<Object[]> compareMetaRelationPropertyCount(String query)
	{
		ArrayList<Object[]> finalComparison = new ArrayList<Object[]>();
		ArrayList<Object[]> newDBList = new ArrayList<Object[]>();
		ArrayList<Object[]> oldDBList = new ArrayList<Object[]>();
		Object[] row = null;
		
		SesameJenaSelectWrapper newSjsw = Utility.processQuery(newDB, query);
		String[] newValues = newSjsw.getVariables();
		SesameJenaSelectWrapper oldSjsw = Utility.processQuery(oldDB, query);
		String[] oldValues = oldSjsw.getVariables();
		
		while (newSjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = newSjsw.next();
			// Index 0 is for subject's concept, 1 is for relation, 2 is for object's concept, 3 is for count
			Object[] tempCount = { sjss.getRawVar(newValues[0]), sjss.getRawVar(newValues[1]), sjss.getRawVar(newValues[2]),
					sjss.getVar(newValues[3]) };
			newDBList.add(tempCount);
		}
		while (oldSjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = oldSjsw.next();
			// Index 0 is for subject's concept, 1 is for relation, 2 is for object's concept, 3 is for count
			Object[] tempCount = { sjss.getRawVar(oldValues[0]), sjss.getRawVar(oldValues[1]), sjss.getRawVar(newValues[2]),
					sjss.getVar(newValues[3]) };
			oldDBList.add(tempCount);
		}
		
		for (int n = 0; n < newDBList.size(); n++)
		{
			boolean matchFound = false;
			for (int o = 0; o < oldDBList.size(); o++)
			{
				if (newDBList.get(n)[0].toString().equals(oldDBList.get(o)[0].toString())
						&& newDBList.get(n)[1].toString().equals(oldDBList.get(o)[1].toString())
						&& newDBList.get(n)[2].toString().equals(oldDBList.get(o)[2].toString())
						&& newDBList.get(n)[3].toString().equals(oldDBList.get(o)[3].toString()))
				{
					matchFound = true;
					oldDBList.remove(o);
					break;
				} else if (newDBList.get(n)[0].toString().equals(oldDBList.get(o)[0].toString())
						&& newDBList.get(n)[1].toString().equals(oldDBList.get(o)[1].toString())
						&& newDBList.get(n)[2].toString().equals(oldDBList.get(o)[2].toString()))
				{
					matchFound = true;
					System.out.println("Mismatch with " + newDBList.get(n)[0] + "~" + newDBList.get(n)[1] + "~" + newDBList.get(n)[2]
							+ ". New DB has " + newDBList.get(n)[3] + ". Old DB has " + oldDBList.get(o)[3]);
					row = new Object[] { newDBList.get(n)[0], newDBList.get(n)[1], newDBList.get(n)[2], newDBList.get(n)[3], oldDBList.get(o)[3], "" };
					finalComparison.add(row);
					oldDBList.remove(o);
					break;
				}
			}
			if (!matchFound)
			{
				System.out.println("New object added: " + newDBList.get(n)[0] + "~" + newDBList.get(n)[1] + "~" + newDBList.get(n)[2]
						+ ". Number added: " + newDBList.get(n)[3]);
				row = new Object[] { newDBList.get(n)[0], newDBList.get(n)[1], newDBList.get(n)[2], newDBList.get(n)[3], "empty",
						"added to " + newDBName };
				finalComparison.add(row);
				newDBList.remove(n);
				n--;
			}
		}
		// This for loop is necessary to only find things removed from previous db
		for (int o = 0; o < oldDBList.size(); o++)
		{
			System.out.println("Old object removed: " + oldDBList.get(o)[0] + "~" + oldDBList.get(o)[1] + "~" + oldDBList.get(o)[2]
					+ ". Number removed: " + oldDBList.get(o)[3]);
			row = new Object[] { oldDBList.get(o)[0], oldDBList.get(o)[1], oldDBList.get(o)[2], "empty", oldDBList.get(o)[3],
					"removed from " + oldDBName };
			finalComparison.add(row);
		}
		if (finalComparison.isEmpty())
		{
			System.out.println("No Changes.");
			row = new Object[] { "No Changes.", "", "", "", "", "" };
			finalComparison.add(row);
		}
		return finalComparison;
	}
	
	/**
	 * Finds duplicates of instances without case sensitivity
	 * 
	 * @param query
	 * @return ArrayList of Object arrays, each array represents a row to be printed into excel
	 */
	public ArrayList<Object[]> findCaseInstanceDuplicate(String query)
	{
		ArrayList<Object[]> finalComparison = new ArrayList<Object[]>();
		ArrayList<Object[]> allProps = new ArrayList<Object[]>();
		
		Object[] row = null;
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(newDB, query);
		String[] values = sjsw.getVariables();
		
		while (sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			
			Object[] singleProp = { sjss.getRawVar(values[0]), sjss.getRawVar(values[1]) };
			allProps.add(singleProp);
		}
		
		for (int i = 0; i < allProps.size(); i++)
		{
			boolean valueOneWasOutput = false;
			for (int n = (i + 1); n < allProps.size(); n++)
			{
				if (allProps.get(i)[0].toString().equals(allProps.get(n)[0].toString())
						&& allProps.get(i)[1].toString().equalsIgnoreCase(allProps.get(n)[1].toString()))
				{
					if (!valueOneWasOutput)
					{
						row = new Object[] { allProps.get(i)[0], allProps.get(i)[1] };
						finalComparison.add(row);
						valueOneWasOutput = true;
					}
					System.out.println("Instance duplicate found.");
					row = new Object[] { allProps.get(n)[0], allProps.get(n)[1] };
					finalComparison.add(row);
					allProps.remove(n);
					n--;
				}
			}
		}
		if (finalComparison.isEmpty())
		{
			System.out.println("No duplicate found.");
			row = new Object[] { "No Duplicate Found.", "" };
			finalComparison.add(row);
		}
		return finalComparison;
	}
	
	/**
	 * Finds duplicates of instance properties
	 * 
	 * @param query
	 * @return ArrayList of Object arrays, each array represents a row to be printed into excel
	 */
	public ArrayList<Object[]> findInstancePropertyDuplicate(String query)
	{
		ArrayList<Object[]> finalComparison = new ArrayList<Object[]>();
		
		// HashMap keys are instance, and values are String arrays holding property and property value
		HashMap<String, ArrayList<Object[]>> allInstancesAndProps = new HashMap<String, ArrayList<Object[]>>();
		ArrayList<Object[]> allProps = new ArrayList<Object[]>();
		Object[] row = null;
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(newDB, query);
		String[] values = sjsw.getVariables();
		
		while (sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String key = sjss.getRawVar(values[0]).toString().concat("+++").concat(sjss.getRawVar(values[1]).toString());
			if (!allInstancesAndProps.containsKey(key))
			{
				allProps = new ArrayList<Object[]>();
				allInstancesAndProps.put(key, allProps);
			}
			// Index 2 is property, 3 is property value
			Object[] singleProp = { sjss.getRawVar(values[2]), sjss.getVar(values[3]) };
			allInstancesAndProps.get(key).add(singleProp);
		}
		Set<String> keySet = allInstancesAndProps.keySet();
		for (String key : keySet)
		{
			ArrayList<Object[]> instanceProp = allInstancesAndProps.get(key);
			String[] conceptInstance = key.split("\\+\\+\\+");
			for (int i = 0; i < instanceProp.size(); i++)
			{
				boolean valueOneWasOutput = false;
				for (int n = (i + 1); n < instanceProp.size(); n++)
				{
					if (instanceProp.get(i)[0].toString().equalsIgnoreCase(instanceProp.get(n)[0].toString()))
					{
						System.out.println("Property type duplicate found.");
						if (!valueOneWasOutput)
						{
							row = new Object[] { conceptInstance[0], conceptInstance[1], instanceProp.get(i)[0], instanceProp.get(i)[1] };
							finalComparison.add(row);
							valueOneWasOutput = true;
						}
						row = new Object[] { conceptInstance[0], conceptInstance[1], instanceProp.get(n)[0], instanceProp.get(n)[1] };
						finalComparison.add(row);
						instanceProp.remove(n);
						n--;
					}
				}
			}
		}
		
		if (finalComparison.isEmpty())
		{
			System.out.println("No duplicate found.");
			row = new Object[] { "No Duplicate Found.", "", "", "" };
			finalComparison.add(row);
		}
		return finalComparison;
	}
	
	/**
	 * Finds duplicates of relation properties. Outputs entire triple that the relation is used in because subject and
	 * object make the relation unique.
	 * 
	 * @param query
	 * @return ArrayList of Object arrays, each array represents a row to be printed into excel
	 */
	public ArrayList<Object[]> findRelationPropertyDuplicate(String query)
	{
		ArrayList<Object[]> finalComparison = new ArrayList<Object[]>();
		
		// HashMap keys are the triple, and values are String arrays holding property and property value
		HashMap<String, ArrayList<Object[]>> allInstancesAndProps = new HashMap<String, ArrayList<Object[]>>();
		ArrayList<Object[]> allProps = new ArrayList<Object[]>();
		Object[] row = null;
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(newDB, query);
		String[] values = sjsw.getVariables();
		
		

		while (sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String key = sjss.getRawVar(values[0]).toString().concat("+++").concat(sjss.getRawVar(values[1]).toString()).concat("+++").concat(
					sjss.getRawVar(values[2]).toString()).concat("+++").concat(
					sjss.getRawVar(values[3]).toString().concat("+++").concat(sjss.getRawVar(values[4]).toString()));
			if (!allInstancesAndProps.containsKey(key))
			{
				allProps = new ArrayList<Object[]>();
				allInstancesAndProps.put(key, allProps);
			}
			// Index 5 is property, 6 is property value
			Object[] singleProp = { sjss.getRawVar(values[5]), sjss.getVar(values[6]) };
			allInstancesAndProps.get(key).add(singleProp);
		}
		
		Set<String> keySet = allInstancesAndProps.keySet();
		for (String key : keySet)
		{
			ArrayList<Object[]> instanceProp = allInstancesAndProps.get(key);
			String[] conceptInstance = key.split("\\+\\+\\+");
			for (int i = 0; i < instanceProp.size(); i++)
			{
				boolean valueOneWasOutput = false;
				for (int n = (i + 1); n < instanceProp.size(); n++)
				{
					if (instanceProp.get(i)[0].toString().equalsIgnoreCase(instanceProp.get(n)[0].toString()))
					{
						System.out.println("Property type duplicate found.");
						if (!valueOneWasOutput)
						{
							row = new Object[] { conceptInstance[0], conceptInstance[1], conceptInstance[2], conceptInstance[3], conceptInstance[4],
									instanceProp.get(i)[0], instanceProp.get(i)[1] };
							finalComparison.add(row);
							valueOneWasOutput = true;
						}
						row = new Object[] { conceptInstance[0], conceptInstance[1], conceptInstance[2], conceptInstance[3], conceptInstance[4],
								instanceProp.get(n)[0], instanceProp.get(n)[1] };
						finalComparison.add(row);
						instanceProp.remove(n);
						n--;
					}
				}
			}
		}
		
		if (finalComparison.isEmpty())
		{
			System.out.println("No duplicate found.");
			row = new Object[] { "No Duplicate Found.", "", "", "", "", "", "" };
			finalComparison.add(row);
		}
		return finalComparison;
	}
	
}
