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
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.ArrayListUtilityMethods;
import prerna.util.DIHelper;

/**
 * This class is a temporary fix for queries to run across multiple databases
 * The query passed through this class must have the format engine1&engine2&engine1query&engine2query
 * The two queries must have exactly one variable name in common--which is how this class will line up the table
 */
public class DualEngineGridPlaySheet extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(DualEngineGridPlaySheet.class.getName());
	String query1;
	String query2;
	String engineName1;
	String engineName2;
	IEngine engine1;
	IEngine engine2;
	LinkedHashMap<Object, ArrayList<Object[]>> dataHash1 = new LinkedHashMap<Object, ArrayList<Object[]>>();
	LinkedHashMap<Object, ArrayList<Object[]>> dataHash2 = new LinkedHashMap<Object, ArrayList<Object[]>>();
	private int names1size;
	private int names2size;
	private Set<String> uniqueNames = new LinkedHashSet<String>();
	private Integer[] index;
	private boolean match1 = true;
	private boolean match2 = true;

	public ArrayList<Object[]> getList(){
		return list;
	}
	
	public String[] getNames() {
		return names;
	}
	
	/**
	 * This is the function that is used to create the first view 
	 * of any play sheet.  It often uses a lot of the variables previously set on the play sheet, such as {@link #setQuery(String)},
	 * {@link #setJDesktopPane(JDesktopPane)}, {@link #setRDFEngine(IEngine)}, and {@link #setTitle(String)} so that the play 
	 * sheet is displayed correctly when the view is first created.  It generally creates the model for visualization from 
	 * the specified engine, then creates the visualization, and finally displays it on the specified desktop pane
	 * 
	 * <p>This is the function called by the PlaysheetCreateRunner.  PlaysheetCreateRunner is the runner used whenever a play 
	 * sheet is to first be created, most notably in ProcessQueryListener.
	 */
	@Override
	public void createData() {

		list = new ArrayList<Object[]>();

		//Process query 1
		SesameJenaSelectWrapper wrapper1 = new SesameJenaSelectWrapper();
		if(engine1!= null){
			wrapper1.setQuery(query1);
			updateProgressBar("10%...Querying RDF Repository", 10);
			wrapper1.setEngine(engine1);
			updateProgressBar("20%...Querying RDF Repository", 30);
			wrapper1.executeQuery();
			updateProgressBar("30%...Processing RDF Statements	", 60);
		}
		// get the bindings from it
		String [] names1 = wrapper1.getVariables();
		names1size = names1.length;

		//process query 2
		SesameJenaSelectWrapper wrapper2 = new SesameJenaSelectWrapper();
		if(engine2!= null){
			wrapper2.setQuery(query2);
			updateProgressBar("40%...Querying RDF Repository", 10);
			wrapper2.setEngine(engine2);
			updateProgressBar("50%...Querying RDF Repository", 30);
			wrapper2.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}
		// get the bindings from it
		String[] names2 = wrapper2.getVariables();
		names2size = names2.length;

		//find the common variable in the wrapper names (this will be the hashtable key)
		Set<String> setNames1 = new LinkedHashSet<String> (Arrays.asList(names1));
		Set<String> setNames2 = new LinkedHashSet<String> (Arrays.asList(names2));

		uniqueNames.addAll(setNames1);
		uniqueNames.addAll(setNames2);
		names = uniqueNames.toArray(new String[uniqueNames.size()]);

		index = new Integer[uniqueNames.size()];
		for(int i = 0; i < names1size; i++) {
			index[i] = i;
		}
		int counter = names1size;
		for(int i = 0; i < names2size; i++) {
			if(!setNames1.contains(names2[i])) {
				index[counter] = i + names1size;
				counter++;
			}
		}

		Set<String> setDifference = new LinkedHashSet<String>();
		setDifference.addAll(setNames1);
		setDifference.retainAll(setNames2);
		String commonVar = setDifference.iterator().next();

		processWrapper(commonVar, wrapper1, dataHash1, names1);
		processWrapper(commonVar, wrapper2, dataHash2, names2);

		updateProgressBar("60%...Preparing List", 80);

		prepareList(dataHash1, dataHash2);		
		
		list = ArrayListUtilityMethods.orderQuery(list);

	}

	/**
	 * Method prepareList.  This method essentially combines the results of two separate query results.
	 * Iterates through hash1, gets the list associated with each key, then combines each array in the list with each array in the list of hash2.
	 * @param hash1 Hashtable<Object,ArrayList<Object[]>> - The results from processWrapper() on the first query
	 * @param hash2 Hashtable<Object,ArrayList<Object[]>> - The results from processWrapper() on the second query
	 */
	private void prepareList(HashMap<Object, ArrayList<Object[]>> hash1, HashMap<Object, ArrayList<Object[]>> hash2)
	{
		ArrayList<Object[]> combinedList = new ArrayList<Object[]>();

		Iterator<Object> hash1it = hash1.keySet().iterator();
		while (hash1it.hasNext()){
			Object key = hash1it.next();
			ArrayList<Object[]> hash1list = hash1.get(key);
			ArrayList<Object[]> hash2list = hash2.remove(key);
			for(Object[] hash1array : hash1list){
				if(hash2list != null){
					for(Object[] hash2array : hash2list){
						Object[] fullRow = new Object[names1size + names2size];

						//combine the two arrays into one row
						for(int i = 0; i<fullRow.length; i++){
							if(i < names1size) {
								fullRow[i] = hash1array[i];
							} else {
								fullRow[i] = hash2array[i-names1size];
							}
						}
						// add to the list
						combinedList.add(fullRow);
					}
				}
				else if(match1){
					Object[] fullRow = new Object[names1size + names2size];
					//combine the two arrays into one row
					for(int i = 0; i < names1size; i++){
						if(i < names1size){
							fullRow[i] = hash1array[i];
						}
					}
					// add to the list
					combinedList.add(fullRow);
				}
			}
		}
		if(match2)
		{
			// now add any results that were returned from the second query but don't match with the first
			Iterator<Object> hash2it = hash2.keySet().iterator();
			while(hash2it.hasNext()) {
				Object key = hash2it.next();
				ArrayList<Object[]> hash2list = hash2.get(key);
				for(Object[] hash2array : hash2list){
					Object[] fullRow = new Object[names1size + names2size];
	
					for(int i = names1size; i<fullRow.length; i++){
						fullRow[i] = hash2array[i-names1size];
					}
	
					// add to the list
					combinedList.add(fullRow);
				}
			}
		}
		// remove the duplicated columns
		Iterator<Object[]> removeDuplicateColumnsIt = combinedList.iterator();
		while(removeDuplicateColumnsIt.hasNext()) {
			Object[] fullRow = removeDuplicateColumnsIt.next();
			Object[] reducedList = new Object[uniqueNames.size()];
			for(int i = 0; i < index.length; i++) {
				reducedList[i] = fullRow[index[i]];
			}
			list.add(reducedList);
		}
	}

	/**
	 * Method processWrapper.  Processes the wrapper for the results of a query to a specific database, and adds the results to a Hashtable.
	 * @param commonVar String - the variable name that the two queries have in common.
	 * @param sjw SesameJenaSelectWrapper - the wrapper for the query
	 * @param hash Hashtable<Object,ArrayList<Object[]>> - The data structure where the data from the query will be stored.
	 * @param names String[] - An array consisting of all the variables from the query.
	 */
	private void processWrapper(String commonVar, SesameJenaSelectWrapper sjw, HashMap<Object, ArrayList<Object[]>> hash, String[] names){
		// now get the bindings and generate the data
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();

				Object [] values = new Object[names.length];
				Object commonVal = null;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = sjss.getVar(names[colIndex]);
					if(names[colIndex].equals(commonVar)){ 
						commonVal = sjss.getVar(names[colIndex]);
					}
				}
				ArrayList<Object[]> overallArray = new ArrayList<Object[]>();
				if(hash.containsKey(commonVal))
					overallArray = hash.get(commonVal);

				overallArray.add(values);
				hash.put(commonVal, overallArray);
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}

	/**
	 * Sets the String version of the SPARQL query on the play sheet. <p> The query must be set before creating the model for
	 * visualization.  Thus, this function is called before createView(), extendView(), overlayView()--everything that 
	 * requires the play sheet to pull data through a SPARQL query.
	 * @param query the full SPARQL query to be set on the play sheet
	 * @see	#createView()
	 * @see #extendView()
	 * @see #overlayView()
	 */
	@Override
	public void setQuery(String query) {

		StringTokenizer queryTokens = new StringTokenizer(query, "&");
		for (int queryIdx = 0; queryTokens.hasMoreTokens(); queryIdx++){
			String token = queryTokens.nextToken();
			if (queryIdx == 0){
				this.engineName1 = token;
				this.engine1 = (IEngine) DIHelper.getInstance().getLocalProp(engineName1);
			}
			else if (queryIdx == 1){
				this.engineName2 = token;
				this.engine2 = (IEngine) DIHelper.getInstance().getLocalProp(engineName2);
			}
			else if (queryIdx == 2)
				this.query1 = token;
			else if (queryIdx == 3)
				this.query2 = token;
			else if (queryIdx == 4)
				this.match1 = Boolean.parseBoolean(token);
			else if (queryIdx == 5)
				this.match2 = Boolean.parseBoolean(token);
		}
	}
}
