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
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

/**
 * This class is used to generate the system duplication calculations for the fact sheet report.
 */
public class FactSheetSysDupeCalculator {
	
	Logger logger = Logger.getLogger(getClass());

	ArrayList<String> sysList = new ArrayList<String>();
	final String crmKey = "!CRM!";
	String tapCoreDB = "TAP_Core_Data";
	String tapSiteDB = "TAP_Site_Data";
	Hashtable<String, Hashtable<String, ArrayList<Object>>> allDataHash = new Hashtable<String, Hashtable<String, ArrayList<Object>>>();
	public ArrayList<String> criteriaList = new ArrayList<String>();
	public Hashtable<String, ArrayList<ArrayList<Object>>> priorityAllDataHash = new Hashtable<String, ArrayList<ArrayList<Object>>>();
	public Hashtable<String, ArrayList<String>> prioritySysHash = new Hashtable<String, ArrayList<String>>();
	public Hashtable<String, ArrayList<Double>> priorityValueHash = new Hashtable<String, ArrayList<Double>>();
	int comparisonSysNum = 5;
	/**
	 * Constructor for FactSheetSysDupeCalculator.
	 */
	public FactSheetSysDupeCalculator()
	{
		performAnalysis();
		prioritizeValues();
		organizeFinalPriorityHash();
		printValues2();
	}
	
	/**
	 * Performs analysis on the TAP Core database for system duplication calculations.
	 * Takes into business processes, data, BLU, theater/garrison, transactions, data warehouse, users, sites, and user interface.
	 */
	public void performAnalysis() 
	{
		//declare 
		SysDupeFunctions sdf = new SysDupeFunctions();
		//get list of systems first
		
		String query = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		sysList = sdf.createSystemList(tapCoreDB, query);
		sdf.setSysList(sysList);
		
		//BP First
		criteriaList.add("BusinessProcess");
		String bpQuery ="SELECT DISTINCT ?System ?BusinessProcess WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?System ?Supports ?BusinessProcess}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable bpHash = sdf.compareSystemParameterScore(tapCoreDB, bpQuery, SysDupeFunctions.VALUE);
		processHashForScoring(bpHash,0);
		
		//Data and BLU 2
		criteriaList.add("Data/BLU");
		String dataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		String bluQuery = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?provide ?BLU }}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable<String, Hashtable<String,Double>> dataBLUHash = sdf.getDataBLUDataSet(tapCoreDB, dataQuery, bluQuery, SysDupeFunctions.VALUE);
		processHashForScoring(dataBLUHash, 1);
		
		
		//Theater and Garrison 3
		criteriaList.add("Theater/Garrison");
		String theaterQuery = "SELECT DISTINCT ?System ?Theater WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Theater}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable theaterHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, theaterQuery, "Theater", "Garrison", "Both");
		processHashForScoring(theaterHash,2);
		
		//Transaction and Dataware House 4
		criteriaList.add("Transaction/DataWarehouse");
		String dwQuery = "SELECT DISTINCT ?System ?Trans WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?System <http://semoss.org/ontologies/Relation/Contains/Transactional> ?Trans}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable dwHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, dwQuery, "Yes", "No", "Both");
		processHashForScoring(dwHash,3);
		
		//User Types 5
		criteriaList.add("Users");
		String userQuery ="SELECT DISTINCT ?System ?User WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?UsedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>;} {?User <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/User> ;} {?System ?UsedBy ?User}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable userHash = sdf.compareSystemParameterScore(tapCoreDB, userQuery, SysDupeFunctions.VALUE);
		processHashForScoring(userHash,4);
		
		//Sites 6
		criteriaList.add("Sites");
		String siteQuery ="SELECT DISTINCT ?System ?Facility WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?SystemDCSite ?DeployedAt ?Facility;}{?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;}{?Facility <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Facility>;}  {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System ?DeployedAt1 ?SystemDCSite;}} ";
		Hashtable siteHash = sdf.compareSystemParameterScore(tapSiteDB, siteQuery, SysDupeFunctions.VALUE);
		processHashForScoring(siteHash,5);
		
		//User Interface
		criteriaList.add("User Interface");
		String uiQuery ="SELECT DISTINCT ?System ?UserInterface WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;} {?UserInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface> ;} {?System ?Utilizes ?UserInterface}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable uiHash = sdf.compareSystemParameterScore(tapCoreDB, uiQuery, SysDupeFunctions.VALUE);
		processHashForScoring(uiHash,6);
		
		//
		
//		Hashtable dataHash = new Hashtable();
//		Hashtable overallHash;
//		String actQuery ="SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System ?Supports ?Activity}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
//		Hashtable actHash = sdf.compareSystemParameterScore(tapCoreDB, actQuery, SysDupeFunctions.VALUE);
//		processHashForScoring(actHash,4);
		
	}	
	

	/**
	 * Processes the hashtable that is used for system duplication storing.
	 * Creates hashtable of arraylists with system as the key and corresponding data/BLU as the values.
	 * @param dataHash 	Hashtable<String,Hashtable<String,Double>>
	 * @param idx 		Index for the system value hash (int).
	 */
	public void processHashForScoring(Hashtable<String, Hashtable<String,Double>>dataHash, int idx)
	{

		for(Entry<String, Hashtable<String, Double>> sysEntry : dataHash.entrySet()) 
		{
			String sysName = sysEntry.getKey();
		    Hashtable<String,Double> sysDataHash = sysEntry.getValue();
		    Hashtable<String, ArrayList<Object>> dataSysHash;
		    //get the right system data hash from allDataHash
		    if(!allDataHash.containsKey(sysName))
		    {
		    	dataSysHash = new Hashtable<String, ArrayList<Object>>();
		    	allDataHash.put(sysName, dataSysHash);
		    }
		    else
		    {
		    	dataSysHash = allDataHash.get(sysName);
		    }
		    	
		    for(Entry<String, Double> sysCompEntry : sysDataHash.entrySet()) 
			{
				String sysName2 = sysCompEntry.getKey();
			    Double value = sysCompEntry.getValue();
			    if(!dataSysHash.containsKey(sysName2))
			    {
			    	ArrayList<Object> sysValueList = new ArrayList<Object>(){{
			    		  add("N/A");
			    		  add("N/A");
			    		  add("N/A");
			    		  add("N/A");
			    		  add("N/A");
			    		  add("N/A");
			    		  add("N/A");
			    		}};;
			    	sysValueList.remove(idx);
			    	sysValueList.add(idx, value);
			    	dataSysHash.put(sysName2, sysValueList);
			    }
			    else
			    {
			    	ArrayList<Object> sysValueList  = dataSysHash.get(sysName2);
			    	sysValueList.remove(idx);
			    	sysValueList.add(idx, value);
			    }
			}
		}
	}
	
	
	/**
	 * This method prioritizes the top systems compared to a given system.
	 */
	public void prioritizeValues()
	{
		
		//go through all the data for one system at a time
		for(Entry<String, Hashtable<String, ArrayList<Object>>> e : allDataHash.entrySet()) 
		{
			String sysName = e.getKey();
			Hashtable<String, ArrayList<Object>> valueHash = e.getValue();
			ArrayList<Double> priorityList = new ArrayList<Double>();
			ArrayList<String> priorityNameList = new ArrayList<String>();
			
			//for a given system go through all criteria
			for(Entry<String, ArrayList<Object>> e1 : valueHash.entrySet()) 
			{
				String sysName2 = e1.getKey();
				ArrayList<Object> valueList = e1.getValue();
				double totalValue = 0.0;
				//do not need to evaluate system against itself
				//if theater or data warehouse ones both dont work
				if(( valueList.get(1) instanceof Double && (Double)valueList.get(1)==0 ) || (valueList.get(2) instanceof Double && (Double)valueList.get(2) ==0)||sysName.equals(sysName2))
				{
					continue;
				}
				for(int i=0;i<criteriaList.size();i++)
				{
					if(valueList.get(i) instanceof Double)
					{
						totalValue = totalValue+ (Double)valueList.get(i);
					}
				}
				
				//When a new total value is determined for a sys-sys comparison, compare to see if it is top 5 or n 
				for (int i=0; i<comparisonSysNum;i++)
				{
					//if element doesn't even exist, just add it in
					if (priorityList.size()<=i)
					{
						priorityList.add(i, totalValue);
						priorityNameList.add(i, sysName2);
						break;
					}
					//if it does exist, you ened to insert it and ensure it doesn't overpopulate the total number
					else if (totalValue>priorityList.get(i))
					{
						//get % estimate
						priorityList.add(i, totalValue);
						priorityNameList.add(i, sysName2);
						//always remove nth element because we want to keep the size of this arraylist to that number
						if(priorityList.size()>5)
						{
							priorityList.remove(comparisonSysNum);
							priorityNameList.remove(comparisonSysNum);
						}
						break;
					}
				}
			}
			//finally put everything into those two hashes
			
			for (int i=0; i<priorityList.size();i++)
			{
				double curValue = priorityList.get(i);
				curValue =(double) curValue/criteriaList.size();
				priorityList.remove(i);
				priorityList.add(i, curValue);
			}
			prioritySysHash.put(sysName, priorityNameList);
			priorityValueHash.put(sysName, priorityList);
		}
	}
	/**
	 * Organizes the final priority hash by looping through the list of priority systems.
	 */
	public void organizeFinalPriorityHash()
	{
		for(Entry<String, ArrayList<String>> e : prioritySysHash.entrySet()) 
		{
			String sysName = e.getKey();
			ArrayList<String> sysList = e.getValue();
			Hashtable<String, ArrayList<Object>> sysSpecAllDataHash = allDataHash.get(sysName);
			ArrayList<ArrayList<Object>> sysSpecPriorityList = new ArrayList<ArrayList<Object>>();
			for(int i=0;i<sysList.size();i++) 
			{
				String sysName2 = sysList.get(i);
				ArrayList<Object> allSpecData = sysSpecAllDataHash.get(sysName2);
				sysSpecPriorityList.add(allSpecData);
			}
			priorityAllDataHash.put(sysName, sysSpecPriorityList);
		}
	}
	
	/**
	 * Prints values for system duplication, looping through the all data hash.
	 */
	public void printValues1()
	{
		for(Entry<String, Hashtable<String, ArrayList<Object>>> e : allDataHash.entrySet()) 
		{
			String sysName = e.getKey();
			Hashtable<String, ArrayList<Object>> valueHash = e.getValue();
			for(Entry<String, ArrayList<Object>> e1 : valueHash.entrySet()) 
			{
				String sysName2 = e1.getKey();
				ArrayList<Object> valueList = e1.getValue();
				String displayString = sysName+" Fulfillment by "+sysName2+ " through: ";
				for(int i=0;i<criteriaList.size();i++)
				{
					displayString = displayString+" "+criteriaList.get(i)+ "-" +valueList.get(i)+";";
				}
				logger.debug(displayString);
			}
		}
	}
	
	/**
	 * Prints values for system duplication, looping through the priority all data hash.
	 */
	public void printValues2()
	{
		for(Entry<String, ArrayList<ArrayList<Object>>> e : priorityAllDataHash.entrySet()) 
		{
			String sysName = e.getKey();
			ArrayList<ArrayList<Object>> valueList = e.getValue();
			ArrayList<String> sysNameList = prioritySysHash.get(sysName);
			for(int i=0;i<valueList.size();i++) 
			{
				String sysName2 = sysNameList.get(i);
				ArrayList<Object> printList = valueList.get(i);
				String displayString = sysName+" Fulfillment by "+sysName2+ " through: ";
				for(int j=0;j<criteriaList.size();j++)
				{
					displayString = displayString+" "+criteriaList.get(j)+ "-" +printList.get(j)+";";
				}
				logger.debug(displayString);
			}
		}
	}
	
}
