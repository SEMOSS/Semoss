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
	String hieQuery;
	String hieQueryEngineName;
	IEngine hieEngine;
	
	String lpiSysKey = "LPISystem";
	String interfaceTypeKey = "InterfaceType";
	String interfacingSystemKey = "InterfacingSystem";
	String probabilityKey = "Probability";
	String dhmsmSORKey = "DHMSM";
	String commentKey = "Comment";
	String interfaceKey = "Interface";
	
	String hpKey = "High";
	String downstreamKey = "Downstream";
	String sorKey = "Provide";	
	String dataKey = "Data";
	
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

		//process query 3
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
		
		//process query 4
		SesameJenaSelectWrapper hieWrapper = new SesameJenaSelectWrapper();
		if(hieEngine!= null){
			hieWrapper.setQuery(hieQuery);
			updateProgressBar("40%...Querying RDF Repository", 10);
			hieWrapper.setEngine(hieEngine);
			updateProgressBar("50%...Querying RDF Repository", 30);
			hieWrapper.executeQuery();
			updateProgressBar("60%...Processing RDF Statements	", 60);
		}
		// get the bindings from it
		String[] names4 = hieWrapper.getVariables();

		Vector<String> sysDataSOR = processPeripheralWrapper(sorWrapper, names2);
		Vector<String> sysLPI = processPeripheralWrapper(lpiWrapper, names3);
		Vector<String> hieSystems = processPeripheralWrapper(hieWrapper, names4);
		processWrapper(wrapper1, names1, sysDataSOR, sysLPI, hieSystems);

	}

	/**
	 * Method processWrapper.  Processes the wrapper for the results of a query to a specific database, and adds the results to a Hashtable.
	 * @param commonVar String - the variable name that the two queries have in common.
	 * @param sjw SesameJenaSelectWrapper - the wrapper for the query
	 * @param hash Hashtable<Object,ArrayList<Object[]>> - The data structure where the data from the query will be stored.
	 * @param names String[] - An array consisting of all the variables from the query.
	 */
	private void processWrapper(SesameJenaSelectWrapper sjw, String[] names, Vector<String> sorV, Vector<String> lpiV, Vector<String> hieV){
		// now get the bindings and generate the data
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();
							
				String lpiSys = sjss.getVar(lpiSysKey) + "";
				String interfacingSys = sjss.getVar(interfacingSystemKey) + ""; 
				
				String lpiSystem = sjss.getRawVar(lpiSysKey) + "";
				String interfaceType = sjss.getRawVar(interfaceTypeKey) + "";
				String interfacingSystem = sjss.getRawVar(interfacingSystemKey) + "";
				String interfaceVar = sjss.getRawVar(interfaceKey) + "";
				String probability = sjss.getRawVar(probabilityKey) + "";
				String dhmsmSOR = sjss.getRawVar(dhmsmSORKey) + "";
				String comment = sjss.getRawVar(commentKey) + "";
				String data = sjss.getRawVar(dataKey) + "";
				
				String lpiSysData = lpiSystem + "" + data;
				String interfacingSysData = interfacingSystem + "" + data;				

				Object[] values = new Object[names.length];
				int count = 0;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(names[colIndex].contains(commentKey)){ 
						if (probability.length() < 3) {
							if (interfaceVar.length() < 3) {
								comment = "\"No interfaces identified.\"";
							}
							else {comment = "\"Stays as-is.\""; }								
						}
						else if (probability.contains(hpKey)) {
							if (hieV.contains(interfacingSystem)) {
								comment = "\"Replaced by DHMSM HIE service.\"";
							}
							else {
								if (interfaceType.contains(downstreamKey)) {
									if (dhmsmSOR.length() < 3) {
										comment = "\"Kill the interface.\"";
									}
									else {
										comment = "\"Need to add interface " + lpiSys + "->DHMSM.\"";
									}
								}
								else { //LPI is downstream of HP
									if (dhmsmSOR.length() < 3) {
										comment = "\"LPI IS LOSING DATA.\"";
									}
									else {
										comment = "\"Need to add interface DHMSM->" + lpiSys + "\"";
									}
								}
							}
						}
						else { //probability is low
							if (lpiSys.equals("DMLSS") && interfacingSys.equals("AIDC")) {
								System.out.println("Test.");
							}
							if (dhmsmSOR.contains(sorKey)) {
								String upStreamSys, downStreamSys, upStreamSysRaw, downStreamSysRaw = "";
								if (interfaceType.contains(downstreamKey)) {
									upStreamSys = lpiSys; upStreamSysRaw = lpiSystem;									
									downStreamSys = interfacingSys; downStreamSysRaw = interfacingSystem;
								}
								else {
									upStreamSys = interfacingSys; upStreamSysRaw = interfacingSystem;	
									downStreamSys = lpiSys; downStreamSysRaw = lpiSystem;
								}
								if (lpiV.contains(upStreamSysRaw) || (lpiV.contains(upStreamSysRaw) && lpiV.contains(downStreamSysRaw)) ) {
									comment = "\"Need to add interface DHMSM->" + upStreamSys + ".\"";
								}
								else {
									comment = "\"Need to add interface DHMSM->" + downStreamSys + ".\"";
								}
							}
							else {
								if(sorV.contains(lpiSysData)){
									System.out.println(" this is 3bi");
									comment = "\"Need to add interface " + lpiSys + "->DHMSM.\"";
								}
								else if(sorV.contains(interfacingSysData) && !lpiV.contains(interfacingSystem)){
									System.out.println(" this is 3bii");
									comment = "\"Need to add interface " + interfacingSys +"->DHMSM. " + interfacingSys + 
											" should be LPI.\"";
								}
								else{
									System.out.println(" this is neither ");
									//LP should be LPI
									comment = "\"Stays as-is.\"";
								}
							}
						}
						values[count] = comment;
						System.out.println("This is the comment: " + comment);
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
				this.hieQueryEngineName = token;
				this.hieEngine = (IEngine) DIHelper.getInstance().getLocalProp(hieQueryEngineName);
			}
			else if (queryIdx == 4){
				System.out.println("query 1 " + token);
				this.interfaceQuery = token;
			}
			else if (queryIdx == 5){
				System.out.println("query 2 " + token);
				this.sorQuery = token;
			}
			else if (queryIdx == 6){
				System.out.println("query 3 " + token);
				this.lpiQuery = token;
			}
			else if (queryIdx == 7){
				System.out.println("query 4 " + token);
				this.hieQuery = token;
			}
		}
	}	
}
