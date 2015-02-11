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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.DeconflictingReportSheetWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

//TODO: lots of processing, should be put into a different class

/**
 * Listener for btnDeconflictingReport on the MHS TAP tab
 * Results in the creation of the deconflicting/missing ICD data report
 */
public class DeconflictingReportButtonListener implements IChakraListener{
	static final Logger logger = LogManager.getLogger(DeconflictingReportButtonListener.class.getName());
	
	ArrayList<String> outputArray = new ArrayList<String>();
	Hashtable<String,String> queries = new Hashtable<String,String>();
	Hashtable<String,String[]> headerNames = new Hashtable<String,String[]>();
	Hashtable<String,String[]> addOnNames = new Hashtable<String,String[]>();
	
	/**
	 * This is executed when the btnDeconflictingReport is pressed by the user
	 * Uses DeconflictingReportSheetWriter to generate the report
	 * @param arg0 ActionEvent
	 */
	public void actionPerformed(ActionEvent arg0) {

		//creates all queries and tab names needed
		createDataStores();
		
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine;

		Hashtable<String, ArrayList<ArrayList<String>>> allSystemsDataHash = new Hashtable<String, ArrayList<ArrayList<String>>>();
		//runs through each query for all systems at one time.
		for(String tabName : queries.keySet())
		{
			ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
			
			engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");

			/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(queries.get(tabName));
			if(tabName.contains("F"))
				engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Site_Data");
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			*/
			
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, queries.get(tabName));

			String[] names = wrapper.getVariables();
			try {
				while(wrapper.hasNext()) {
					ISelectStatement sjss = wrapper.next();
					ArrayList<String> values = new ArrayList<String>();
					for(int colIndex = 0;colIndex < names.length;colIndex++) {
						if(sjss.getVar(names[colIndex]) != null) {
							if(names[colIndex].toLowerCase().equals("quantity"))
								values.add(colIndex,((Double)sjss.getVar(names[colIndex])).toString());
							else
								values.add(colIndex,(String)sjss.getVar(names[colIndex]));
						}
					}
					list.add(values);
				}
			} 
			catch (RuntimeException e) {
				e.printStackTrace();
			}
			allSystemsDataHash.put(tabName, list);
		}
		createFiles(allSystemsDataHash);
	}
	
	/**
	 * Store all the queries for each tab in a hashtable
	 * Store tab names in the same order as the queries in another hashtable
	 */
	public void createDataStores()
	{
		String out="";

		out = "TAB-A";
		outputArray.add(out);
		queries.put(out,"SELECT DISTINCT ?system ?description WHERE{ BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{?system <http://semoss.org/ontologies/Relation/Contains/Description> ?description}}");
		
//		out = "TAB-B";
//		outputArray.add(out);
//		queries.put(out,"SELECT DISTINCT ?system ?businessprocess WHERE{ BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>}{?businessprocess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>} {?system ?supports ?businessprocess}}");
//		
//		out = "TAB-C";
//		outputArray.add(out);
//		queries.put(out,"SELECT DISTINCT ?system ?activity WHERE{ BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>}{?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>} {?system ?supports ?activity}}");
		
//		out = "TAB-D";
//		outputArray.add(out);
//		queries.put(out,"SELECT DISTINCT ?System1 (COALESCE(?Data1, \"\") AS ?Data) (COALESCE(?ICD1, \"\") AS ?icd) WHERE { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} MINUS { {?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?provide) {?System1 ?provide ?Data1 .} } { SELECT DISTINCT (SAMPLE(?Data1) AS ?Data1) (SAMPLE(?System1) AS ?System1) (SAMPLE(?ICD) AS ?ICD1) WHERE { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?carries) {?ICD ?carries ?Data1;} { BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?Upstream) {?System1 ?Upstream ?ICD ;}} UNION {BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?Downstream){?ICD ?Downstream ?System1 ;} } BIND(URI(CONCAT(STR(?System1),STR(?Data1))) AS ?sysData) } GROUP BY ?sysData}}");
//		
//		out = "TAB-E";
//		outputArray.add(out);
//		queries.put(out,"SELECT DISTINCT (SAMPLE(?System1) AS ?System1) (SAMPLE(?Data1) AS ?Data1) ?crm (COALESCE(SAMPLE(?ICD), \"\") AS ?ICD1) WHERE { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System1 ?provide ?Data1 .} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm} OPTIONAL { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?carries) {?ICD ?carries ?Data1;} BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?Upstream) {?System1 ?Upstream ?ICD ;} } OPTIONAL { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?carries) {?ICD ?carries ?Data1;} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?Downstream) {?System1 ?Downstream ?ICD ;} } BIND(URI(CONCAT(STR(?System1),STR(?Data1))) AS ?sysData) FILTER(!BOUND(?ICD))} GROUP BY ?sysData ?crm");
		
		out = "TAB-D";
		outputArray.add(out);
		queries.put(out,"SELECT DISTINCT ?system ?hardwareVersion ?quantity WHERE { BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?hardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/HardwareModule>} {?system ?has ?hardwareModule} {?typeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?hardwareModule ?typeOf ?hardwareVersion} {?hardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?quantity} } ORDER BY ?system ?hardwareVersion");

		out = "TAB-E";
		outputArray.add(out);
		queries.put(out,"SELECT DISTINCT ?system ?softwareVersion ?quantity WHERE { BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?system ?consists ?softwareModule} {?typeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?softwareModule ?typeOf ?softwareVersion} {?softwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?quantity} } ORDER BY ?system ?softwareVersion");
		
		out = "TAB-F";
		outputArray.add(out);
		queries.put(out,"SELECT DISTINCT ?system ?facility WHERE{ {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?deployedAtSite <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?systemSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?system ?deployedAtSite ?systemSite} {?deployedAtFacility <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?facility <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Facility>} {?systemSite ?deployedAtFacility ?facility} } ORDER BY ?system ?facility");
		
		out = "TAB-G";
		outputArray.add(out);
		queries.put(out,"SELECT DISTINCT ?owner ?Interface (COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) ?Data ?Frequency ?Protocol ?Format ?Source WHERE { { SELECT DISTINCT ?owner ?System ?type ?Interface ?Data ?Format ?Frequency ?Protocol ?Source(COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) WHERE {  BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Source> ?Source ;}{?DownstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSys ;} BIND(\"Upstream\" AS ?type){?Phase <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase> ;}{?DownstreamSys ?Phase ?LifeCycle ;}BIND(<http://health.mil/ontologies/Concept/LifeCycle/Supported> AS ?LifeCycle){?Phase2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase> ;}{?System ?Phase2 ?LifeCycle2 ;}BIND(<http://health.mil/ontologies/Concept/LifeCycle/Supported> AS ?LifeCycle2)} } UNION { SELECT DISTINCT ?owner ?System ?type ?Interface ?Data ?Format ?Frequency ?Protocol ?Source(COALESCE(?UpstreamSys, ?System) AS ?UpstreamSystem) (COALESCE(?DownstreamSys, ?System) AS ?DownstreamSystem) WHERE {BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?Format ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?Frequency ;}{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?Protocol ;} {?carries <http://semoss.org/ontologies/Relation/Contains/Source> ?Source ;}{?UpstreamSys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?UpstreamSys ?Upstream ?Interface ;}{?Interface ?Downstream ?System ;} BIND(\"Downstream\" AS ?type) {?Phase <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase> ;}{?UpstreamSys ?Phase ?LifeCycle ;}BIND(<http://health.mil/ontologies/Concept/LifeCycle/Supported> AS ?LifeCycle){?Phase2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase> ;}{?System ?Phase2 ?LifeCycle2 ;}BIND(<http://health.mil/ontologies/Concept/LifeCycle/Supported> AS ?LifeCycle2)}  } } ORDER BY ?Interface ?UpstreamSystem ?DownstreamSystem");
		
		out = "TAB-H";
		outputArray.add(out);
		queries.put(out,"SELECT DISTINCT ?system ?blu WHERE{ BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system ?provide ?blu}}");
		
		out = "TAB-I";
		outputArray.add(out);
		queries.put(out,"SELECT DISTINCT ?system ?data ?crm WHERE{ BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?system ?provide ?data} {?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm}}");
		
//		out = "TAB-L";
//		outputArray.add(out);
//		queries.put(out,"SELECT DISTINCT ?System ?Data ?crm (COALESCE(?ICD, ?ICD2) AS ?ICDOUTPUT) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?crm;}{?System ?Provide ?Data}BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}{{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?ICD ?Downstream ?System} {?Carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?ICD ?Carries ?Data}} UNION {{?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System ?Upstream ?ICD2}{?Carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?ICD2 ?Carries2 ?Data} }} ORDER BY ?System ?Data ?crm ?ICDOUTPUT");
	}
	
	/**
	 * Store all the systems within DHIMS
	 * @param query 	String containing the query to get the systems
	 * @return list		ArrayList<String> containing the query output, the list of the systems within DHIMS
	 */
	public ArrayList<String> createSystemList(String query)
	{
		//create a list of systems for the DHIMS tag
		ArrayList<String> list = new ArrayList<String>();
		
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");
		
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();*/
		
		String[] names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				list.add((String)sjss.getVar(names[0]));
			}
		}
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	/**Runs a query and stores multiple outputs
	 * @param query 	String containing the query to run
	 * @return list		ArrayList<ArrayList<String>> containing the query output
	 */
	public ArrayList<ArrayList<String>> createMapList(String query)
	{
		//create a list of instances for a given concepts with an extra spot to hold a mapping
		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		
		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		*/
		String[] names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				ArrayList<String> mapList = new ArrayList<String>();
				mapList.add((String)sjss.getVar(names[0]));
				mapList.add("");
				list.add(mapList);
			}
		}
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	/**
	 * Outputs the query results in separate tabs and writes the workbook
	 * Uses DeconflictingReportSheetWriter to generate the workbook
	 * @param allSystemsDataHash 	Hashtable<String,ArrayList<ArrayList<String>>>
	 */
	public void createFiles(Hashtable<String, ArrayList<ArrayList<String>>> allSystemsDataHash)
	{
		//gets an arraylist of all sytems that are tagged with DHIMS
		ArrayList<String> systemList = createSystemList("SELECT DISTINCT ?System  WHERE {BIND(<http://health.mil/ontologies/Concept/SystemOwner/DHIMS> AS ?owner) {?ownedby <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>} {?system ?ownedby ?owner}} ORDER BY ?System");
		
		//gets arraylists of all concepts that are needed for the mapping tabs.
		//has a spot in the arraylist to hold the mapping (either X or C,R,M) for each data/blu/businessprocess/activity
		//these spots will be filled in later when the systems are processed
		Hashtable<String,ArrayList<ArrayList<String>>> conceptLists = new Hashtable<String,ArrayList<ArrayList<String>>>();
//		conceptLists.put("TAB-B",createMapList("SELECT DISTINCT ?businessprocess  WHERE { {?businessprocess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}} ORDER BY ?businessprocess"));
//		conceptLists.put("TAB-C",createMapList("SELECT DISTINCT ?activity  WHEREing { {?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;}} ORDER BY ?activity "));
		conceptLists.put("TAB-H",createMapList("SELECT DISTINCT ?blu  WHERE { {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}} ORDER BY ?blu"));
		conceptLists.put("TAB-I",createMapList("SELECT DISTINCT ?data  WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}} ORDER BY ?data"));
		
		//processes and writes workbook for 1 system at a type.
		//for each individual query, go through the whole, all systems, query results
		//for each row in the query results, (row entry), checks to see if the system being processed is involved
		//if the system is, adds this row to the datahash for that system
		//special case: if doing a query for a mapping tab, then for each row where the system is involved, put a mapping in the concept list
		//special case: if doing a query for a list of concepts tab, then just store the list of concepts.
		//pass all the results for the specific system to the writer and write the file.
		for(int i=0;i<systemList.size();i++)
		{
			String system=systemList.get(i);
			Hashtable<String, ArrayList<ArrayList<String>>> oneSystemDataHash = new Hashtable<String, ArrayList<ArrayList<String>>>();
			
			for(int tabInd=0;tabInd<outputArray.size();tabInd++)
			{
				String tabName = outputArray.get(tabInd);
				ArrayList<ArrayList<String>> queryResults = new ArrayList<ArrayList<String>>(allSystemsDataHash.get(tabName));
				ArrayList<ArrayList<String>> oneSystemQueryResults = new ArrayList<ArrayList<String>>();
				ArrayList<ArrayList<String>> bluAndDataMappings=new ArrayList<ArrayList<String>>();
				if(conceptLists.containsKey(tabName))
				{
					ArrayList<ArrayList<String>> conceptListTabName = conceptLists.get(tabName);
					bluAndDataMappings = deepCopy(conceptListTabName);
				}
				for(int rowInd=0;rowInd<queryResults.size();rowInd++)
				{
					ArrayList<String> rowEntry = queryResults.get(rowInd);
					if(rowEntry.contains(system))
					{
						if(tabName.contains("-H")||tabName.contains("-I"))
						{
							int count=0;
							while(count<bluAndDataMappings.size())
							{
								if(bluAndDataMappings.get(count).get(0).equals(rowEntry.get(1)))
								{
									if(rowEntry.size()<3)
										bluAndDataMappings.get(count).set(1,"X");
									else
										bluAndDataMappings.get(count).set(1,rowEntry.get(2));
								}
								count++;
							}
						}
						else
							oneSystemQueryResults.add(rowEntry);
					}
				}
				if(tabName.contains("-H")||tabName.contains("-I"))//||tabName.contains("-B")||tabName.contains("-C"))
					oneSystemDataHash.put(tabName, bluAndDataMappings);
				else	
					oneSystemDataHash.put(tabName, oneSystemQueryResults);
			}
			
//			ArrayList<ArrayList<String>> mergedTabAData = mergeRows(oneSystemDataHash.get("TAB-D"));
//			oneSystemDataHash.put("TAB-D",mergedTabAData);
//			ArrayList<ArrayList<String>> mergedTabLData = mergeRows4Columns(oneSystemDataHash.get("TAB-L"));
//			oneSystemDataHash.put("TAB-L",mergedTabLData);
							
			DeconflictingReportSheetWriter writer = new DeconflictingReportSheetWriter();
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator") + "Deconflicting" + System.getProperty("file.separator");
			String writeFileName = system+"_System Export_30102013.xlsx";
			String fileLoc = workingDir + folder + writeFileName;
			String templateLoc = workingDir + folder + "Report_Deconflicting_Template.xlsx";
			logger.info(fileLoc);
			
			writer.ExportLoadingSheets(fileLoc, oneSystemDataHash,templateLoc,outputArray);
		}
	}
	
	//TODO: method below not being used since commented out

	/**
	 * Method mergeRows.
	 * @param tabData ArrayList<ArrayList<String>>
	 * @return ArrayList<ArrayList<String>> 
	 */
	public ArrayList<ArrayList<String>> mergeRows(ArrayList<ArrayList<String>> tabData)
	{
		ArrayList<ArrayList<String>> merged = new ArrayList<ArrayList<String>>();
		if(tabData.size()<1)
			return merged;

		ArrayList<String> rowToAdd = tabData.get(0); //row being added
		rowToAdd.set(2,"=\""+rowToAdd.get(2)+"\"");
		ArrayList<String> lastRowEntered = tabData.get(0); //last row that has been entered into the returned value, used for comparison to next row
		merged.add(rowToAdd);
		int count=1;
		while(count<tabData.size())	
		{
			rowToAdd=tabData.get(count);
			if(rowToAdd.get(0).equals(lastRowEntered.get(0))&&rowToAdd.get(1).equals(lastRowEntered.get(1)))
			{
				rowToAdd.set(2,lastRowEntered.get(2)+"&CHAR(10)&\""+rowToAdd.get(2)+"\"");
				merged.set(merged.size()-1,rowToAdd);
				lastRowEntered = rowToAdd;
			}
			else
			{
				rowToAdd.set(2,"=\""+rowToAdd.get(2)+"\"");
				merged.add(rowToAdd);
			}
			count++;
		}
		return merged;
	}
	
	//TODO: method below not being used since commented out
	
	/**
	 * Method mergeRows4Columns.
	 * @param tabData ArrayList<ArrayList<String>>
	 * @return ArrayList<ArrayList<String>>
	 */
	public ArrayList<ArrayList<String>> mergeRows4Columns(ArrayList<ArrayList<String>> tabData)
	{
		ArrayList<ArrayList<String>> merged = new ArrayList<ArrayList<String>>();
		if(tabData.size()<1)
			return merged;

		ArrayList<String> rowToAdd = tabData.get(0); //row being added
		rowToAdd.set(3,"=\""+rowToAdd.get(3)+"\"");
		ArrayList<String> lastRowEntered = tabData.get(0); //last row that has been entered into the returned value, used for comparison to next row
		merged.add(rowToAdd);
		int count=1;
		while(count<tabData.size())	
		{
			rowToAdd=tabData.get(count);
			if(rowToAdd.get(0).equals(lastRowEntered.get(0))&&rowToAdd.get(1).equals(lastRowEntered.get(1))&&rowToAdd.get(2).equals(lastRowEntered.get(2)))
			{
				rowToAdd.set(3,lastRowEntered.get(3)+"&CHAR(10)&\""+rowToAdd.get(3)+"\"");
				merged.set(merged.size()-1,rowToAdd);
				lastRowEntered = rowToAdd;
			}
			else
			{
				rowToAdd.set(3,"=\""+rowToAdd.get(3)+"\"");
				lastRowEntered = rowToAdd;
				merged.add(rowToAdd);
			}
			count++;
		}
		return merged;
	}
	
	/**
	 * Makes a copy of an ArrayList<ArrayList<String>>
	 * @param oldArray 		ArrayList<ArrayList<String>> containing the information to reproduce
	 * @return newArray		ArrayList<ArrayList<String>> containing the information that was copied
	 */
	private ArrayList<ArrayList<String>> deepCopy(ArrayList<ArrayList<String>> oldArray)
	{
		ArrayList<ArrayList<String>> newArray = new ArrayList<ArrayList<String>>();
		
		for(int i=0;i<oldArray.size();i++)
		{
			ArrayList<String> oldArray2 = oldArray.get(i);
			ArrayList<String> newArray2 = new ArrayList<String>();
			for(int i2=0;i2<oldArray2.size();i2++)
			{
				newArray2.add(oldArray2.get(i2));
			}
			newArray.add(newArray2);
		}
		return newArray;
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}

}
