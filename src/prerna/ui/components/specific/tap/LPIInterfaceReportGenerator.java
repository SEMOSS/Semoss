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

public class LPIInterfaceReportGenerator extends GridPlaySheet {

	String interfaceQuery;
	String interfaceQueryEngineName;
	IEngine interfaceEngine;
	String sorQuery;
	String sorQueryEngineName;
	IEngine sorEngine;
	String lpiQuery;
	String lpiQueryEngineName;
	IEngine lpiEngine;
	String commentKey = "Comment";
	String lpiSysKey = "LPISystem";
	String otherSysKey = "InterfacingSystem";
	String interfaceTypeKey = "InterfaceType";
	String dataKey = "Data";
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
		if(interfaceEngine!= null){
			wrapper1.setQuery(interfaceQuery);
			updateProgressBar("10%...Querying RDF Repository", 10);
			wrapper1.setEngine(interfaceEngine);
			updateProgressBar("20%...Querying RDF Repository", 30);
			wrapper1.executeQuery();
			updateProgressBar("30%...Processing RDF Statements	", 60);
		}
		// get the bindings from it
		String [] names1 = wrapper1.getVariables();
		names = names1;

		//process query 2
		SesameJenaSelectWrapper sorWrapper = new SesameJenaSelectWrapper();
		if(sorEngine!= null){
			sorWrapper.setQuery(sorQuery);
			updateProgressBar("40%...Querying RDF Repository", 10);
			sorWrapper.setEngine(sorEngine);
			updateProgressBar("50%...Querying RDF Repository", 30);
			sorWrapper.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}
		// get the bindings from it
		String[] names2 = sorWrapper.getVariables();

		//process query 2
		SesameJenaSelectWrapper lpiWrapper = new SesameJenaSelectWrapper();
		if(lpiEngine!= null){
			lpiWrapper.setQuery(lpiQuery);
			updateProgressBar("40%...Querying RDF Repository", 10);
			lpiWrapper.setEngine(lpiEngine);
			updateProgressBar("50%...Querying RDF Repository", 30);
			lpiWrapper.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}
		// get the bindings from it
		String[] names3 = lpiWrapper.getVariables();

		Vector<String> sysDataSOR = processPeripheralWrapper(sorWrapper, names2);
		Vector<String> sysLPI = processPeripheralWrapper(lpiWrapper, names3);
		processWrapper(wrapper1, names1, sysDataSOR, sysLPI);

	}

	/**
	 * Method processWrapper.  Processes the wrapper for the results of a query to a specific database, and adds the results to a Hashtable.
	 * @param commonVar String - the variable name that the two queries have in common.
	 * @param sjw SesameJenaSelectWrapper - the wrapper for the query
	 * @param hash Hashtable<Object,ArrayList<Object[]>> - The data structure where the data from the query will be stored.
	 * @param names String[] - An array consisting of all the variables from the query.
	 */
	private void processWrapper(SesameJenaSelectWrapper sjw, String[] names, Vector<String> sorV, Vector<String> lpiV){
		// now get the bindings and generate the data
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();

				Object [] values = new Object[names.length];
				int count = 0;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(names[colIndex].contains(commentKey)){ 
						String comment = sjss.getVar(names[colIndex]) + "";
						if(comment.contains(sorCheck)) {
							String lpiSysData = sjss.getRawVar(lpiSysKey) + "" + sjss.getRawVar(dataKey);
							String otherSysData = sjss.getRawVar(otherSysKey) + "" + sjss.getRawVar(dataKey);
							String otherSys = sjss.getRawVar(otherSysKey) + "";
							System.out.println(" We are in 3b");
							if(sorV.contains(lpiSysData) && sorV.contains(otherSysData) && !lpiV.contains(otherSys)){
								System.out.println(" this is 3bi AND 3bii");
								//Interfaces where SOR system sends data to DHMSM is added
								comment = "\"Interfaces " + sjss.getVar(lpiSysKey) + "->DHMSM and " + sjss.getVar(otherSysKey) +"->DHMSM are added. " + sjss.getVar(otherSysKey) + 
										" should be LPI\"";
							}
							else if(sorV.contains(lpiSysData)){
								System.out.println(" this is 3bi");
								comment = "\"Interface " + sjss.getVar(lpiSysKey) + "->DHMSM is added\"";
							}
							else if(sorV.contains(otherSysData) && !lpiV.contains(otherSys)){
								System.out.println(" this is 3bii");
								comment = "\"Interface " + sjss.getVar(otherSysKey) +"->DHMSM is added. " + sjss.getVar(otherSysKey) + 
										" should be LPI\"";
							}
							else{
								System.out.println(" this is neither ");
								//LP should be LPI
								comment = "\"Stays as-is\"";
							}
						}
						values[count] = comment;
					}
					else
						values[count] = sjss.getVar(names[colIndex]);
					count++;
				}
				list.add(values);
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}
	
	private Vector<String> processPeripheralWrapper(SesameJenaSelectWrapper sjw, String[] names){
		// now get the bindings and generate the data
		Vector<String> retV = new Vector<String>();
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();

				int colIndex = 0;
				String val = sjss.getRawVar(names[colIndex]) + "";
				if(val.substring(0, 1).equals("\""))
					val = val.substring(1, val.length()-1);
					
				retV.add(val);
				System.out.println("adding to peripheral list: " + val);

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
				this.interfaceQueryEngineName = token;
				this.interfaceEngine = (IEngine) DIHelper.getInstance().getLocalProp(interfaceQueryEngineName);
			}
			else if (queryIdx == 1){
				this.sorQueryEngineName = token;
				this.sorEngine = (IEngine) DIHelper.getInstance().getLocalProp(sorQueryEngineName);
			}
			else if (queryIdx == 2){
				this.lpiQueryEngineName = token;
				this.lpiEngine = (IEngine) DIHelper.getInstance().getLocalProp(lpiQueryEngineName);
			}
			else if (queryIdx == 3){
				System.out.println("query 1 " + token);
				this.interfaceQuery = token;
			}
			else if (queryIdx == 4){
				System.out.println("query 2 " + token);
				this.sorQuery = token;
			}
			else if (queryIdx == 5){
				System.out.println("query 3 " + token);
				this.lpiQuery = token;
			}
		}
	}
}
