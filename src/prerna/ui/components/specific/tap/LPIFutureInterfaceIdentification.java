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
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;

public class LPIFutureInterfaceIdentification extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(LPIFutureInterfaceIdentification.class.getName());
	String statusQuery;
	String statusQueryEngineName;
	IEngine statusEngine;
	String DHMSMQuery;
	String DHMSMQueryEngineName;
	IEngine DHMSMEngine;
	String lpiSorQuery;
	String lpiSorQueryEngineName;
	IEngine lpiSorEngine;
	String statusKey = "Status";
	String dataKey = "Data";
	String statusCheck = "Needed but not Present";

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
		ISelectWrapper statusWrapper = WrapperManager.getInstance().getSWrapper(statusEngine, statusQuery);

		/*SesameJenaSelectWrapper statusWrapper = new SesameJenaSelectWrapper();
		if(statusEngine!= null){
			statusWrapper.setQuery(statusQuery);
			updateProgressBar("10%...Querying RDF Repository", 10);
			statusWrapper.setEngine(statusEngine);
			updateProgressBar("20%...Querying RDF Repository", 30);
			statusWrapper.executeQuery();
			updateProgressBar("30%...Processing RDF Statements	", 60);
		}*/
		// get the bindings from it
		names = statusWrapper.getVariables();
		
		//process query 2
		ISelectWrapper DHMSMWrapper = WrapperManager.getInstance().getSWrapper(DHMSMEngine, DHMSMQuery);

		/*SesameJenaSelectWrapper DHMSMWrapper = new SesameJenaSelectWrapper();
		if(DHMSMEngine!= null){
			DHMSMWrapper.setQuery(DHMSMQuery);
			updateProgressBar("40%...Querying RDF Repository", 10);
			DHMSMWrapper.setEngine(DHMSMEngine);
			updateProgressBar("50%...Querying RDF Repository", 30);
			DHMSMWrapper.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}*/
		// get the bindings from it
		String[] names2 = DHMSMWrapper.getVariables();

		//process query 2
		ISelectWrapper lpiSorWrapper = WrapperManager.getInstance().getSWrapper(lpiSorEngine, lpiSorQuery);

		
		/*SesameJenaSelectWrapper lpiSorWrapper = new SesameJenaSelectWrapper();
		if(lpiSorEngine!= null){
			lpiSorWrapper.setQuery(lpiSorQuery);
			updateProgressBar("40%...Querying RDF Repository", 10);
			lpiSorWrapper.setEngine(lpiSorEngine);
			updateProgressBar("50%...Querying RDF Repository", 30);
			lpiSorWrapper.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}*/
		// get the bindings from it
		String[] names3 = lpiSorWrapper.getVariables();

		Vector<String> DHMSMVector = processPeripheralWrapper(DHMSMWrapper, names2);
		Hashtable<String, String> lpiSorHash = processPeripheralHashWrapper(lpiSorWrapper, names3);
		processWrapper(statusWrapper, names, DHMSMVector, lpiSorHash);

	}

	/**
	 * Method processWrapper.  Processes the wrapper for the results of a query to a specific database, and adds the results to a Hashtable.
	 * @param commonVar String - the variable name that the two queries have in common.
	 * @param sjw SesameJenaSelectWrapper - the wrapper for the query
	 * @param hash Hashtable<Object,ArrayList<Object[]>> - The data structure where the data from the query will be stored.
	 * @param names String[] - An array consisting of all the variables from the query.
	 */
	private void processWrapper(ISelectWrapper sjw, String[] names, Vector<String> DHMSMVect, Hashtable<String, String> lsHash){
		// now get the bindings and generate the data
		try {
			while(sjw.hasNext()) //while still row beneath (while data left)
			{
				ISelectStatement sjss = sjw.next(); //go to new row 

				Object [] values = new Object[names.length]; 
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(names[colIndex].contains(statusKey)){
						String status = sjss.getVar(names[colIndex]) + "";
						String data = sjss.getRawVar(dataKey) + "";
						if(status.contains(statusCheck)) {
							
							if(DHMSMVect.contains(data)){
								status = "\"Interface proposed with: DHMSM\"";
							}
							else if(lsHash.containsKey(data)){
								status = "\"Interface Proposed with: " + lsHash.get(data) + "\"";
							}
							else{
								status = "\"Needed, Not Present, and Neither DHMSM nor Low Prob SOR System Provides this Data\"";
							}
						}
						values[colIndex] = status;
					}
					else
						values[colIndex] = sjss.getVar(names[colIndex]);
				}
				list.add(values);
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}
	
	private Vector<String> processPeripheralWrapper(ISelectWrapper sjw, String[] names){
		// now get the bindings and generate the data
		Vector<String> retV = new Vector<String>();
		try {
			while(sjw.hasNext())
			{
				ISelectStatement sjss = sjw.next();

				int colIndex = 0;
				String val = sjss.getRawVar(names[colIndex]) + "";
				if(val.substring(0, 1).equals("\""))
					val = val.substring(1, val.length()-1);
					
				retV.add(val);
				System.out.println("adding to peripheral list: " + val);

			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
		return retV;
	}
	
	private Hashtable<String, String> processPeripheralHashWrapper(ISelectWrapper sjw, String[] names){
		// now get the bindings and generate the data
		Hashtable<String, String> retHash = new Hashtable<String, String>();
		
		try {
			while(sjw.hasNext())
			{
				ISelectStatement sjss = sjw.next();
				
				String val0 = sjss.getRawVar(names[0]) + "";
								if(val0.substring(0, 1).equals("\""))
					val0 = val0.substring(1, val0.length()-1);
				
				String val1 = sjss.getRawVar(names[1]) + "";
				if(val1.substring(0, 1).equals("\""))
					val1 = val1.substring(1, val1.length()-1);
				
				retHash.put(val0,val1);
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
		return retHash;
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
				this.statusQueryEngineName = token;
				this.statusEngine = (IEngine) DIHelper.getInstance().getLocalProp(statusQueryEngineName);
			}
			else if (queryIdx == 1){
				this.DHMSMQueryEngineName = token;
				this.DHMSMEngine = (IEngine) DIHelper.getInstance().getLocalProp(DHMSMQueryEngineName);
			}
			else if (queryIdx == 2){
				this.lpiSorQueryEngineName = token;
				this.lpiSorEngine = (IEngine) DIHelper.getInstance().getLocalProp(lpiSorQueryEngineName);
			}
			else if (queryIdx == 3){
				System.out.println("query 1 " + token);
				this.statusQuery = token;
			}
			else if (queryIdx == 4){
				System.out.println("query 2 " + token);
				this.DHMSMQuery = token;
			}
			else if (queryIdx == 5){
				System.out.println("query 3 " + token);
				this.lpiSorQuery = token;
			}
		}
	}
}
