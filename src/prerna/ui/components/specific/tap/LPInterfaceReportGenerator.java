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
import java.util.Vector;

import javax.swing.JDesktopPane;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class LPInterfaceReportGenerator extends GridPlaySheet {

	private String lpSystemInterfacesQuery = "SELECT DISTINCT ?LPSystem ?InterfaceType ?InterfacingSystem ?Probability (COALESCE(?interface,'') AS ?Interface) ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?Freq,'') AS ?Frequency) (COALESCE(?Prot,'') AS ?Protocol) ?DHMSM ?Recommendation WHERE { {SELECT DISTINCT (IF(BOUND(?y),?DownstreamSys,IF(BOUND(?x),?UpstreamSys,'')) AS ?LPSystem) (IF(BOUND(?y),'Upstream',IF(BOUND(?x),'Downstream','')) AS ?InterfaceType) (IF(BOUND(?y),?UpstreamSys,IF(BOUND(?x),?DownstreamSys,'')) AS ?InterfacingSystem)  (COALESCE(IF(BOUND(?y),IF(?UpstreamSysProb1 != 'High' && ?UpstreamSysProb1 != 'Question','Low','High'),IF(BOUND(?x),IF(?DownstreamSysProb1 != 'High' &&?DownstreamSysProb1 != 'Question','Low','High'),'')), '') AS ?Probability) ?interface ?Data ?format ?Freq ?Prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provides','Consumes')) AS ?DHMSM) (COALESCE(?HIEsys, '') AS ?HIE) ?DHMSMcrm WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND('N' AS ?InterfaceYN) LET(?d := 'd') OPTIONAL{ { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb;} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb1;}} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?x :=REPLACE(str(?d), 'd', 'x')) } UNION {{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb;}OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb1;}} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?y :=REPLACE(str(?d), 'd', 'y')) } } {SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ){?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability.}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Task ?Needs ?Data.}} } GROUP BY ?Data} }} FILTER(REGEX(STR(?LPSystem), '^http://health.mil/ontologies/Concept/System/@SYSTEMNAME@$')) } ORDER BY ?Data";
	private final String HEADER_KEY = "headers";
	private final String RESULT_KEY = "data";

	private final String LP_SYS_KEY = "LPSystem";
	private final String INTERFACE_TYPE_KEY = "InterfaceType";
	private final String INTERFACING_SYS_KEY = "InterfacingSystem";
	private final String PROBABILITY_KEY = "Probability";
	private final String DHMSM_SOR_KEY = "DHMSM";
	private final String COMMENT_KEY = "Recommendation";
	private final String DOWNSTREAM_KEY = "Downstream";
	private final String DHMSM_PROVIDE_KEY = "Provide";
	private final String DHMSM_CONSUME_KEY = "Consumes";
	private final String DATA_KEY = "Data";	

	private final String LPI_KEY = "LPI";
//	private final String lpniKey = "LPNI"; 
	private final String HPI_KEY = "HPI";
	private final String HPNI_KEY = "HPNI";

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

		Vector<String> sysDataSOR = processPeripheralWrapper();
		HashMap<String, String> sysTypeHash = DHMSMTransitionUtility.processReportTypeQuery(engine);

		//Process main query
		SesameJenaSelectWrapper wrapper1 = new SesameJenaSelectWrapper();
		wrapper1.setQuery(query);
		wrapper1.setEngine(engine);
		wrapper1.executeQuery();
		// get the bindings from it
		String [] names1 = wrapper1.getVariables();
		names = names1;

		processWrapper(wrapper1, names1, sysDataSOR, sysTypeHash);
	}

	/**
	 * Method processWrapper.  Processes the wrapper for the results of a query to a specific database, and adds the results to a Hashtable.
	 * @param commonVar String - the variable name that the two queries have in common.
	 * @param sjw SesameJenaSelectWrapper - the wrapper for the query
	 * @param hash Hashtable<Object,ArrayList<Object[]>> - The data structure where the data from the query will be stored.
	 * @param names String[] - An array consisting of all the variables from the query.
	 */
	private void processWrapper(SesameJenaSelectWrapper sjw, String[] names, Vector<String> sorV, HashMap<String, String> sysTypeHash){
		// now get the bindings and generate the data
		while(sjw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjw.next();

			//For comment writing
			String sysName = sjss.getVar(LP_SYS_KEY).toString();
			String interfacingSysName = sjss.getVar(INTERFACING_SYS_KEY).toString(); 
			//For logic
			String system = sjss.getRawVar(LP_SYS_KEY).toString();
			String interfacingSystem = sjss.getRawVar(INTERFACING_SYS_KEY).toString();
			String interfaceType = sjss.getRawVar(INTERFACE_TYPE_KEY).toString();
			String dhmsmSOR = sjss.getRawVar(DHMSM_SOR_KEY).toString();
			String comment = "";
			String data = sjss.getRawVar(DATA_KEY).toString();
			String probability = sjss.getVar(PROBABILITY_KEY).toString();

			Object[] values = new Object[names.length];
			int count = 0;
			for(int colIndex = 0;colIndex < names.length;colIndex++)
			{
				if(names[colIndex].contains(COMMENT_KEY))
				{ 
					// determine which system is upstream or downstream
					String upstreamSysName = "";
					String upstreamSystemURI = "";
					String downstreamSysName = "";
					String downstreamSystemURI = "";
					if(interfaceType.contains(DOWNSTREAM_KEY)) { // lp system is providing data to interfacing system
						upstreamSystemURI = system;
						upstreamSysName = sysName;
						downstreamSystemURI = interfacingSystem;
						downstreamSysName = interfacingSysName;
					} else { // lp system is receiving data from interfacing system
						upstreamSystemURI = interfacingSystem;
						upstreamSysName = interfacingSysName;
						downstreamSystemURI = system;
						downstreamSysName = sysName;
					}
					
					String upstreamSysType = sysTypeHash.get(upstreamSystemURI);
					if(upstreamSysType == null) {
						upstreamSysType = "No Probability";
					}
					String downstreamSysType = sysTypeHash.get(downstreamSystemURI);
					if(downstreamSysType == null) {
						downstreamSysType = "No Probability";
					}
					
					// DHMSM is SOR of data
					if(dhmsmSOR.contains(DHMSM_PROVIDE_KEY)) 
					{
						if(upstreamSysType.equals(LPI_KEY)) { // upstream system is LPI
							comment = comment.concat("Need to add interface DHMSM->").concat(upstreamSysName).concat(". ");
						} 
						// new business rule might be added - will either un-comment or remove after discussion today
//						else if (upstreamSysType.equals(lpniKey)) { // upstream system is LPNI
//							comment += "Recommend review of developing interface DHMSM->" + upstreamSysName + ". ";
//						} 
						else if (downstreamSysType.equals(LPI_KEY)) { // upstream system is not LPI and downstream system is LPI
							comment = comment.concat("Need to add interface DHMSM->").concat(downstreamSysName).concat(".").concat(" Recommend review of removing interface ")
									.concat(upstreamSysName).concat("->").concat(downstreamSysName).concat(". ");
						} 
						if (upstreamSysType.equals(HPI_KEY)) { // upstream is HPI
							comment = comment.concat("Provide temporary integration between DHMSM->").concat(upstreamSysName).concat(" until all deployment sites for ").concat(upstreamSysName)
									.concat(" field DHMSM (and any additional legal requirements). ");
						} else if(downstreamSysType.equals(HPI_KEY)) { // upstream sys is not HPI and downstream is HPI
							comment = comment.concat("Provide temporary integration between DHMSM->").concat(downstreamSysName).concat(" until all deployment sites for ").concat(downstreamSysName).concat(" field DHMSM (and any additional legal requirements).")
									.concat(" Recommend review of removing interface ").concat(upstreamSysName).concat("->").concat(downstreamSysName).concat(". ");
						} 
						if(!upstreamSysType.equals(LPI_KEY) && !upstreamSysType.equals(HPI_KEY) && !downstreamSysType.equals(LPI_KEY) && !downstreamSysType.equals(HPI_KEY))
						{
							if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
								comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
							} else {
								comment = "Stay as-is beyond FOC.";
							}
						}
					} // DHMSM is consumer of data
					else if(dhmsmSOR.contains(DHMSM_CONSUME_KEY)) 
					{
						boolean otherwise = true;
						if(upstreamSysType.equals(LPI_KEY) && sorV.contains(upstreamSystemURI + data)) { // upstream system is LPI and SOR of data
							otherwise = false;
							comment = comment.concat("Need to add interface ").concat(upstreamSysName).concat("->DHMSM. ");
						} else if(sorV.contains(upstreamSystemURI + data) && !probability.equals("null") && !probability.equals("") ) { // upstream system is SOR and has a probability
							otherwise = false;
							comment = comment.concat("Recommend review of developing interface between ").concat(upstreamSysName).concat("->DHMSM. ");
						} 
						if(downstreamSysType.equals(LPI_KEY) && sorV.contains(downstreamSystemURI + data)) { // downstream system is LPI and SOR of data
							otherwise = false;
							comment = comment.concat("Need to add interface ").concat(downstreamSysName).concat("->DHMSM. ");
						} else if(sorV.contains(downstreamSystemURI + data) && (!probability.equals("null") && !probability.equals("")) ) { // downstream system is SOR and has a probability
							otherwise = false;
							comment = comment.concat("Recommend review of developing interface between ").concat(downstreamSysName).concat("->DHMSM. ");
						} 
						if(otherwise) {
							if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
								comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
							} else {
								comment = "Stay as-is beyond FOC.";
							}
						}
					} // other cases DHMSM doesn't touch data object
					else {
						if(upstreamSysType.equals(HPI_KEY) || upstreamSysType.equals(HPNI_KEY) || downstreamSysType.equals(HPI_KEY) || downstreamSysType.equals(HPNI_KEY)) { //if either system is HP
							comment = "Stay as-is until all deployment sites for HP system field DHMSM (and any additional legal requirements)." ;
						} else {
							comment = "Stay as-is beyond FOC.";
						}
					}
					values[count] = comment;
				} else {
					values[count] = sjss.getVar(names[colIndex]);
				}
				count++;
			}
			list.add(values);
		}
	}

	private Vector<String> processPeripheralWrapper(){
		String[] names = new String[1];
		SesameJenaSelectWrapper sorWrapper = new SesameJenaSelectWrapper();
		sorWrapper.setQuery(DHMSMTransitionUtility.SYS_SOR_DATA_CONCAT_QUERY);
		sorWrapper.setEngine(engine);
		sorWrapper.executeQuery();
		names = sorWrapper.getVariables();

		Vector<String> retV = new Vector<String>();
		while(sorWrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = sorWrapper.next();
			int colIndex = 0;
			String val = sjss.getRawVar(names[colIndex]).toString();
			if(val.substring(0, 1).equals("\"")) {
				val = val.substring(1, val.length()-1);
			}
			retV.add(val);
		}
		return retV;
	}

	public HashMap<String, Object> getSysLPIInterfaceData(String systemName) {
		HashMap<String, Object> sysLPIInterfaceHash = new HashMap<String, Object>();
		systemName = systemName.replaceAll("\\(", "\\\\\\\\\\(").replaceAll("\\)", "\\\\\\\\\\)");
		lpSystemInterfacesQuery = lpSystemInterfacesQuery.replace("@SYSTEMNAME@", systemName);
		this.query = lpSystemInterfacesQuery;
		this.engine = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		createData();			
		sysLPIInterfaceHash.put(HEADER_KEY, removeSystemFromStringArray(getNames()));
		sysLPIInterfaceHash.put(RESULT_KEY, removeSystemFromArrayList(getList()));
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
