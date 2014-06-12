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
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JDesktopPane;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;

/**
 * This class is a temporary fix for queries to run across multiple databases
 * The query passed through this class must have the format engine1&engine2&engine1query&engine2query
 * The two queries must have exactly one variable name in common--which is how this class will line up the table
 */
public class LPIInterfaceReportGenerator extends GridPlaySheet {

	String query1;
	String query2;
	String engineName1;
	String engineName2;
	IEngine engine1;
	IEngine engine2;
	String commentKey = "Comment";
	String sysDataKey = "sysDataKey";
	String sorCheck = "This is 3b";

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
		names = new String[names1.length-1];
		int count = 0;
		for(String name : names1){
			if(!name.equals(sysDataKey)){
				names[count] = name;
				count++;
			}
		}

		//process query 2
		SesameJenaSelectWrapper sorWrapper = new SesameJenaSelectWrapper();
		if(engine2!= null){
			sorWrapper.setQuery(query2);
			updateProgressBar("40%...Querying RDF Repository", 10);
			sorWrapper.setEngine(engine2);
			updateProgressBar("50%...Querying RDF Repository", 30);
			sorWrapper.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}
		// get the bindings from it
		String[] names2 = sorWrapper.getVariables();

		Vector<String> sysDataSOR = processSORWrapper(sorWrapper, names2);
		processWrapper(wrapper1, names1, sysDataSOR);

	}

	/**
	 * Method processWrapper.  Processes the wrapper for the results of a query to a specific database, and adds the results to a Hashtable.
	 * @param commonVar String - the variable name that the two queries have in common.
	 * @param sjw SesameJenaSelectWrapper - the wrapper for the query
	 * @param hash Hashtable<Object,ArrayList<Object[]>> - The data structure where the data from the query will be stored.
	 * @param names String[] - An array consisting of all the variables from the query.
	 */
	private void processWrapper(SesameJenaSelectWrapper sjw, String[] names, Vector<String> sorV){
		// now get the bindings and generate the data
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();

				Object [] values = new Object[names.length - 1];
				int count = 0;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(!names[colIndex].equals(sysDataKey)){
						if(names[colIndex].contains(commentKey)){ 
							String sysData = sjss.getRawVar(sysDataKey) + "";
							String comment = sjss.getVar(names[colIndex]) + "";
							if(comment.contains(sorCheck)) {
								System.out.println(" We are in 3b");
								if(sorV.contains(sysData)){
									System.out.println(" this is a sor : " + sysData);
									//Interfaces where SOR system sends data to DHMSM is added
									comment = "Interface " + values[0] + "->DHMSM is added";
								}
								else{
									System.out.println(" this is NOT a sor : " + sysData);
									//LP should be LPI
									comment = values[2] + " should be LPI";
								}
							}
							values[count] = comment;
						}
						else
							values[count] = sjss.getVar(names[colIndex]);
						count++;
					}
				}
				list.add(values);
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}
	
	private Vector<String> processSORWrapper(SesameJenaSelectWrapper sjw, String[] names){
		// now get the bindings and generate the data
		Vector<String> retV = new Vector<String>();
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();

				int colIndex = 0;
				String sor = sjss.getRawVar(names[colIndex]) + "";
				retV.add(sor);
				System.out.println("adding as SOR: " + sor);

			}
		} catch (Exception e) {
			logger.fatal(e);
		}
		return retV;
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

		StringTokenizer queryTokens = new StringTokenizer(query, "$");
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
			else if (queryIdx == 2){
				System.out.println("query 1 " + token);
				this.query1 = token;
			}
			else if (queryIdx == 3){
				System.out.println("query 2 " + token);
				this.query2 = token;
			}
		}
	}
}
