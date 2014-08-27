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

import javax.swing.JDesktopPane;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class LPInterfaceReportGenerator extends GridPlaySheet {

	private String lpSystemInterfacesQuery = "SELECT DISTINCT ?LPSystem ?InterfaceType ?InterfacingSystem ?Probability (COALESCE(?interface,'') AS ?Interface) ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?Freq,'') AS ?Frequency) (COALESCE(?Prot,'') AS ?Protocol) ?DHMSM ?Recommendation WHERE { {SELECT DISTINCT (IF(BOUND(?y),?DownstreamSys,IF(BOUND(?x),?UpstreamSys,'')) AS ?LPSystem) (IF(BOUND(?y),'Upstream',IF(BOUND(?x),'Downstream','')) AS ?InterfaceType) (IF(BOUND(?y),?UpstreamSys,IF(BOUND(?x),?DownstreamSys,'')) AS ?InterfacingSystem)  (COALESCE(IF(BOUND(?y),IF(?UpstreamSysProb1 != 'High' && ?UpstreamSysProb1 != 'Question','Low','High'),IF(BOUND(?x),IF(?DownstreamSysProb1 != 'High' &&?DownstreamSysProb1 != 'Question','Low','High'),'')), '') AS ?Probability) ?interface ?Data ?format ?Freq ?Prot (IF((STRLEN(?DHMSMcrm)<1),'',IF((REGEX(STR(?DHMSMcrm),'C')),'Provides','Consumes')) AS ?DHMSM) (COALESCE(?HIEsys, '') AS ?HIE) ?DHMSMcrm WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND('N' AS ?InterfaceYN) LET(?d := 'd') OPTIONAL{ { {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb;} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb1;}} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?x :=REPLACE(str(?d), 'd', 'x')) } UNION {{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{{?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?DownstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?DownstreamSysProb;}OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/HIE> ?HIEsys;}{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?UpstreamSysProb1;}} OPTIONAL{{?UpstreamSys <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> ?InterfaceYN;}} {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?interface ?carries ?Data;} {?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?interface ;}{?interface ?Downstream ?DownstreamSys ;} { {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Freq ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Prot ;} } LET(?y :=REPLACE(str(?d), 'd', 'y')) } } {SELECT DISTINCT ?Data (GROUP_CONCAT(DISTINCT ?Crm ; separator = ',') AS ?DHMSMcrm) WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} OPTIONAL{BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> AS ?DHMSM ){?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability.}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Task ?Needs ?Data.}} } GROUP BY ?Data} }} FILTER(REGEX(STR(?LPSystem), '^http://health.mil/ontologies/Concept/System/@SYSTEMNAME@$')) } ORDER BY ?Data";
	private final String HEADER_KEY = "headers";
	private final String RESULT_KEY = "data";

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

		LPInterfaceProcessor processor = new LPInterfaceProcessor();
		processor.setQuery(query);
		processor.setEngine(engine);
		
		list = processor.generateReport();
		names = processor.getNames();
	}

	public HashMap<String, Object> getSysLPIInterfaceData(String systemName) throws EngineException 
	{
		HashMap<String, Object> sysLPIInterfaceHash = new HashMap<String, Object>();
		systemName = systemName.replaceAll("\\(", "\\\\\\\\\\(").replaceAll("\\)", "\\\\\\\\\\)");
		lpSystemInterfacesQuery = lpSystemInterfacesQuery.replace("@SYSTEMNAME@", systemName);
		this.query = lpSystemInterfacesQuery;
		this.engine = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(engine == null) {
			throw new EngineException("HR_Core Database is not available");
		}
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
