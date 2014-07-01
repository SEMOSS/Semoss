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
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JDesktopPane;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;

public class LPInterfaceReportGenerator extends GridPlaySheet {

	private String lpSystemInterfacesQuery = "HR_Core$HR_Core$HR_Core$HR_Core$ SELECT DISTINCT ?LPSystem ?InterfaceType ?InterfacingSystem ?Probability (COALESCE(?interface,'') AS ?Interface) ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?Freq,'') AS ?Frequency) (COALESCE(?Prot,'') AS ?Protocol) ?DHMSM ?Comment WHERE { {SELECT DISTINCT (IF(BOUND(?y),?DownstreamSys,IF(BOUND(?x),?UpstreamSys,'')) AS ?LPSystem) (IF(BOUND(?y),'Upstream',IF(BOUND(?x),'Downstream','')) AS ?InterfaceType) (IF(BOUND(?y),?UpstreamSys,IF(BOUND(?x),?DownstreamSys,'')) AS ?InterfacingSystem) (COALESCE(IF(BOUND(?y),IF(!(?UpstreamSysProb1 != 'High' ||?UpstreamSysProb1 != 'Question'),'Low','High'),IF(BOUND(?x),IF(!(?DownstreamSysProb1 != 'High' ||?DownstreamSysProb1 != 'Question'),'Low','High'),'')), '') AS ?Probability) ?interface ?Data ?format ?Freq ?Prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provides','Consumes')) AS ?DHMSM) (COALESCE(?HIEsys, '') AS ?HIE) ?DHMSMcrm WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} LET(?d := 'd') OPTIONAL{ { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb;}OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb1;}}{?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?x :=REPLACE(str(?d), 'd', 'x')) } UNION {{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb;}OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb1;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?y :=REPLACE(str(?d), 'd', 'y')) } } {SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ){?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability.}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Task ?Needs ?Data.}} } GROUP BY ?Data} }} FILTER(REGEX(STR(?LPSystem), '@SYSTEMNAME@')) } ORDER BY ?Data $ SELECT DISTINCT (CONCAT(STR(?system), STR(?data)) AS ?sysDataKey) WHERE { { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> } {?provideData <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd} {?provideData <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} filter( !regex(str(?crm),'R')) {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} {?system ?provideData ?data} } UNION { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?system <http://semoss.org/ontologies/Relation/Provide> ?icd } {?icd <http://semoss.org/ontologies/Relation/Payload> ?data} OPTIONAL{ {?icd2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd2 <http://semoss.org/ontologies/Relation/Consume> ?system} {?icd2 <http://semoss.org/ontologies/Relation/Payload> ?data} } FILTER(!BOUND(?icd2)) } } ORDER BY ?data ?system $ SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?System <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'} } BINDINGS ?Probability {('Medium')('Low')('Medium-High')} $ SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;} } BINDINGS ?HIEsys {('Y')}";
	private String headerKey = "headers";
	private String resultKey = "data";

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

	String lpSysKey = "LPSystem";
	String interfaceTypeKey = "InterfaceType";
	String interfacingSystemKey = "InterfacingSystem";
	String probabilityKey = "Probability";
	String dhmsmSORKey = "DHMSM";
	String commentKey = "Comment";
	String interfaceKey = "Interface";	

	String hpKey = "High";
	String lpKey = "Low";
	String downstreamKey = "Downstream";
	String dhmsmProvideKey = "Provide";
	String dhmsmConsumeKey = "Consumes";
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

				//For comment writing
				String lpSysName = sjss.getVar(lpSysKey) + "";
				String interfacingSysName = sjss.getVar(interfacingSystemKey) + ""; 

				//For logic
				String lpSystem = sjss.getRawVar(lpSysKey) + "";
				String interfacingSystem = sjss.getRawVar(interfacingSystemKey) + "";
				String interfaceType = sjss.getRawVar(interfaceTypeKey) + "";
				String dhmsmSOR = sjss.getRawVar(dhmsmSORKey) + "";
				String comment = "";
				String data = sjss.getRawVar(dataKey) + "";
				String probability = sjss.getVar(probabilityKey) + "";

				//				if (lpSysName.contains("MMM") && data.contains("Facility")) {
				//					System.out.println("Test");
				//				}		

				Object[] values = new Object[names.length];
				int count = 0;
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					if(names[colIndex].contains(commentKey))
					{ 
						// DHMSM is SOR of data
						if(dhmsmSOR.contains(dhmsmProvideKey)) {
							// determine which system is upstream or downstream
							String upstreamSysName = "";
							String upstreamSystemURI = "";
							String downstreamSysName = "";
							String downstreamSystemURI = "";
							if(interfaceType.contains(downstreamKey)) { // lp system is providing data to interfacing system
								upstreamSystemURI = lpSystem;
								upstreamSysName = lpSysName;
								downstreamSystemURI = interfacingSystem;
								downstreamSysName = interfacingSysName;
							} else { // lp system is receiving data from interfacing system
								upstreamSystemURI = interfacingSystem;
								upstreamSysName = interfacingSysName;
								downstreamSystemURI = lpSystem;
								downstreamSysName = lpSysName;
							}
							if(lpiV.contains(upstreamSystemURI)) { // upstream system is LPI
								comment = "Need to add interface DHMSM->" + upstreamSysName + ".";
							} else if(lpiV.contains(downstreamSystemURI)) { // upstream system is not LPI and downstream system is LPI
								comment = "Need to add interface DHMSM->" + downstreamSysName + "."; 
								if(upstreamSysName.equals(lpSysName) || ( !probability.equals("null") && !probability.equals("")) ) {
									comment = comment + " Recommend review of removing interface " + upstreamSysName + "->" + downstreamSysName + ".";
								}
							} else {
								comment = "Stay as-is.";
							}
						} // DHMSM is consumer of data
						else if(dhmsmSOR.contains(dhmsmConsumeKey)) {
							// determine which system is upstream or downstream
							String upstreamSysName = "";
							String upstreamSystemURI = "";
							String downstreamSysName = "";
							String downstreamSystemURI = "";
							if(interfaceType.contains(downstreamKey)) { // lp system is providing data to interfacing system
								upstreamSystemURI = lpSystem;
								upstreamSysName = lpSysName;
								downstreamSystemURI = interfacingSystem;
								downstreamSysName = interfacingSysName;
							} else { // lp system is receiving data from interfacing system
								upstreamSystemURI = interfacingSystem;
								upstreamSysName = interfacingSysName;
								downstreamSystemURI = lpSystem;
								downstreamSysName = lpSysName;
							}
							if(lpiV.contains(upstreamSystemURI) && sorV.contains(upstreamSystemURI + data)) { // upstream system is LPI and SOR of data
								comment = "Need to add interface " + upstreamSysName  + " -> DHMSM.";
							} else if(sorV.contains(upstreamSystemURI + data)) { // upstream system is SOR
								if( (upstreamSysName.equals(lpSysName) || (!probability.equals("null") && !probability.equals(""))) ) { 
									comment = "Recommend review of developing interface between " + upstreamSysName  + " -> DHMSM.";
								} else { //upstream system does not have a probability
									comment = "Stay as-is.";
								}
							} else if(lpiV.contains(downstreamSystemURI) && sorV.contains(downstreamSystemURI + data)) { // downstream system is LPI and SOR of data
								comment = "Need to add interface " + downstreamSysName  + " -> DHMSM.";
							} else if(sorV.contains(downstreamSystemURI + data)) { // downstream system is SOR 
								if( (downstreamSysName.equals(lpSysName) || (!probability.equals("null") && !probability.equals(""))) ) { // downstream system is LPNI or HP system
									comment = "Recommend review of developing interface between " + downstreamSysName  + " -> DHMSM.";
								} else { // downstream system does not have a probability
									comment = "Stay as-is.";
								}
							} else {
								comment = "Stay as-is.";
							}
						} // other cases DHMSM doesn't touch data object
						else {
							comment = "Stay as-is.";
						}
						values[count] = comment;
					} else {
						values[count] = sjss.getVar(names[colIndex]);
					}
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
		//String output = "";

		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();
				int colIndex = 0;
				String val = sjss.getRawVar(names[colIndex]) + "";
				if(val.substring(0, 1).equals("\""))
					val = val.substring(1, val.length()-1);

				retV.add(val);
				//output = output + "(<" + val + ">)";
				//System.out.println("adding to peripheral list: " + val);
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
		//System.out.println(output);
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
			else if (queryIdx == 8) {
				if (token.equals("LPNI")) {
					this.interfaceQuery = (this.interfaceQuery).replace("BIND('Y' AS ?InterfaceDHMSM)", "BIND('N' AS ?InterfaceDHMSM)");
				}
			}
		}
	}	

	public HashMap<String, Object> getSysLPIInterfaceData(String systemName) {
		HashMap<String, Object> sysLPIInterfaceHash = new HashMap<String, Object>();
		lpSystemInterfacesQuery = lpSystemInterfacesQuery.replaceAll("@SYSTEMNAME@", systemName);
		setQuery(lpSystemInterfacesQuery);
		createData();			
		sysLPIInterfaceHash.put(headerKey, removeSystemFromStringArray(getNames()));
		sysLPIInterfaceHash.put(resultKey, removeSystemFromArrayList(getList()));
		return sysLPIInterfaceHash;
	}

	private String[] removeSystemFromStringArray(String[] names)
	{
		String[] retArray = new String[names.length-1];
		for(int j=0;j<retArray.length;j++)
			retArray[j] = names[j+1];
		return retArray;
	}

	private ArrayList<Object[]> removeSystemFromArrayList(ArrayList<Object[]> dataRow)
	{
		ArrayList<Object[]> retList = new ArrayList<Object[]>();
		for(int i=0;i<dataRow.size();i++)
		{
			Object[] row = new Object [dataRow.get(i).length-1];
			for(int j=0;j<row.length;j++)
				row[j] = dataRow.get(i)[j+1];
			retList.add(row);
		}
		return retList;
	}
}
