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
package prerna.ui.components.specific.tap;

import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.NewScrollBarUI;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ServiceICDCostAnalyzer {
	ArrayList<String> systems;
	Hashtable<String, Double> dataSerICDCountHash = new Hashtable<String, Double>();
	Hashtable<String, Double> serGenericCostHash = new Hashtable<String, Double>();
	Hashtable<String, Double> dataSerCostHash = new Hashtable<String, Double>();
	Hashtable<String, Double> dataSerCount = new Hashtable<String, Double>();
	GridFilterData gfd = new GridFilterData();
	double hourlyRate = 150;
	double interfaceAnnualCost = 100000;
	double sustainmentPer = 0.18;
	
	//indices for the final table
	private static int serIdx = 0;
	private static int dataIdx = 1;
	private static int fullICDIdx = 2;
	private static int partialIdx = 3;
	private static int costIdx = 4;
	private static int susCostIdx = 5;
	private static int icdSavingsIdx = 6;
	
	
	private static String healthMilDataURI = "http://health.mil/ontologies/Concept/System";
	
	//queries
	private String serCostQuery = "SELECT DISTINCT ?data ?ser (SUM(?loe) AS ?cost) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase>} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass}{?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start}  {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser }{?data <http://semoss.org/ontologies/Relation/Input> ?GLitem}} GROUP BY ?data ?ser BINDINGS ?sys {@SYSBINDINGS@}";
	
	private String serGenericCostQuery = "SELECT DISTINCT ?ser (SUM(?loe) AS ?loeSUM) WHERE {  {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND( <http://semoss.org/ontologies/Relation/TaggedBy> AS ?tagged) {?GLitem ?tagged ?gltag;} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start ;} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase> ;} BIND(<http://semoss.org/ontologies/Relation/BelongsTo> AS ?belongs) {?GLitem ?belongs ?phase ;} BIND( <http://semoss.org/ontologies/Relation/Input> AS ?input) {?inputElement ?input ?GLitem} BIND( <http://health.mil/ontologies/Concept/GLTag/Generic> AS ?gltag). BIND( <http://semoss.org/ontologies/Relation/Output> AS ?output) {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?GLitem ?output ?ser ;}  {?ser <http://www.w3.org/2000/01/rdf-schema#label> ?name;} BIND(\"Generic\" as ?sys)} GROUP BY  ?ser";
	
	private String  icdCountQueryUpstream = "SELECT (SAMPLE(?data) AS ?Data) (SAMPLE(?ser) AS ?Ser) (COUNT(DISTINCT(?icd)) AS ?icdCount) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND( <http://semoss.org/ontologies/Relation/Exposes> AS ?exp) {?ser ?exp ?data;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}  {?pay <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd ?pay ?data}  {?pay <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"} BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?provide) {?sys ?provide ?icd} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?consume)  {?icd ?consume ?sys2} BIND(URI(CONCAT(STR(?data),STR(?ser))) AS ?dataSer)} GROUP BY ?dataSer BINDINGS ?sys {@SYSBINDINGS@}";
	
	private String  icdCountQueryDownstream = "SELECT (SAMPLE(?data) AS ?Data) (SAMPLE(?ser) AS ?Ser) (COUNT(DISTINCT(?icd)) AS ?icdCount) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;} BIND( <http://semoss.org/ontologies/Relation/Exposes> AS ?exp) {?ser ?exp ?data;} {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}  {?pay <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?icd ?pay ?data}  {?pay <http://semoss.org/ontologies/Relation/Contains/Type> \"TBD\"} BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?provide) {?sys ?provide ?icd} BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?consume)  {?icd ?consume ?sys2} BIND(URI(CONCAT(STR(?data),STR(?ser))) AS ?dataSer)} GROUP BY ?dataSer BINDINGS ?sys2 {@SYSBINDINGS@}";
	
	private static String dataSerCountQuery = "SELECT DISTINCT ?data (COUNT(DISTINCT(?service)) AS ?serCount)  WHERE {  {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>} {?service <http://semoss.org/ontologies/Relation/Exposes> ?data}} GROUP BY ?data";
	
	public ServiceICDCostAnalyzer (ArrayList<String> systems)
	{
		this.systems = systems;
	}
	
	private void addBindingToQueries(ArrayList<String> sysBindings)
	{
		String bindingString ="";
		for (String sys : sysBindings)
		{
			bindingString = bindingString+"(<" + healthMilDataURI + "/"+sys + ">)";
		}
		serCostQuery=serCostQuery.replace("@SYSBINDINGS@", bindingString);
		icdCountQueryUpstream=icdCountQueryUpstream.replace("@SYSBINDINGS@", bindingString);
		icdCountQueryDownstream=icdCountQueryDownstream.replace("@SYSBINDINGS@", bindingString);
	}
	
	public void runServiceResults()
	{
		addBindingToQueries(systems);
		
		//there are upstream icd counts and downstream icd counts next two query processes combines both
		ISelectWrapper sjswQuery = processQuery(icdCountQueryUpstream, (IEngine)DIHelper.getInstance().getLocalProp("HR_Core"));
		processDataSerICD(sjswQuery);
		sjswQuery = processQuery(icdCountQueryDownstream, (IEngine)DIHelper.getInstance().getLocalProp("HR_Core"));
		processDataSerICD(sjswQuery);
		
		//organizes dataobjects and services
		sjswQuery = processQuery(dataSerCountQuery, (IEngine)DIHelper.getInstance().getLocalProp("HR_Core"));
		processSerForDataCount(sjswQuery);
		
		//get generic service costs
		sjswQuery = processQuery(serGenericCostQuery, (IEngine)DIHelper.getInstance().getLocalProp("TAP_Cost_Data"));
		processGenericSerCost(sjswQuery);
		
		//get system specific service costs
		sjswQuery = processQuery(serCostQuery, (IEngine)DIHelper.getInstance().getLocalProp("TAP_Cost_Data"));
		processDataSerCost(sjswQuery);
		
		ArrayList<Object[]> outputList = processData();
		createGrid(outputList);
	}
	
	private ArrayList<Object[]> processData()
	{
		ArrayList<Object[]> outputList = new ArrayList<Object[]>();
		for(String dataService : dataSerICDCountHash.keySet())
		{
			Object[] listElement = new Object[7];
			double icdCount = dataSerICDCountHash.get(dataService);
			double dataSerCost= 0.0;
			if(dataSerCostHash.containsKey(dataService))
				dataSerCost = dataSerCostHash.get(dataService);
			
			String[] serDataSplit = dataService.split("&&");
			String service = serDataSplit[1];
			String data = serDataSplit[0];
			double serForDataCount = dataSerCount.get(data);
			listElement[serIdx] = service;
			listElement[dataIdx] = data;
			
			//if ser for data = 1, that means this service replaces this data 1 for 1, therefore, you can just multiple icdCount by annual cost savings
			if(serForDataCount == 1)
			{
				listElement[fullICDIdx] = icdCount;
				listElement[partialIdx] = 0;
				listElement[icdSavingsIdx] = Math.round(icdCount * interfaceAnnualCost);
			}
			//if more than one ser is needed for a data, need to divide up the annual cost savings
			else
			{
				listElement[partialIdx] = icdCount;
				listElement[fullICDIdx] = 0;
				listElement[icdSavingsIdx] = Math.round(icdCount * interfaceAnnualCost/serForDataCount);
			}
			listElement[costIdx] = dataSerCost*hourlyRate;
			listElement[susCostIdx] = dataSerCost *0.18*hourlyRate;
			
			outputList.add(listElement);
			
			
		}
		
		
		return outputList;
		
	}
	

	
	private void processDataSerICD(ISelectWrapper sjswQuery)
	{
		
		String[] vars = sjswQuery.getVariables();
		while(sjswQuery.hasNext())
		{
			ISelectStatement sjss = sjswQuery.next();
			String data = sjss.getVar(vars[0]).toString();
			String service = sjss.getVar(vars[1]).toString();
			double count = (Double) sjss.getVar(vars[2]);
			String key = data+"&&"+service;
			if (dataSerICDCountHash.containsKey(key))
			{
				dataSerICDCountHash.put(key,  dataSerICDCountHash.get(key)+count);
			}
			else
			{
				dataSerICDCountHash.put(key,  count);
			}
		}
	}
	private void processGenericSerCost(ISelectWrapper sjswQuery)
	{
		
		String[] vars = sjswQuery.getVariables();
		while(sjswQuery.hasNext())
		{
			ISelectStatement sjss = sjswQuery.next();
			String service = sjss.getVar(vars[0]).toString();
			double cost = (Double) sjss.getVar(vars[1]);
			serGenericCostHash.put(service,  cost);
		}
	}
	
	private void processDataSerCost(ISelectWrapper sjswQuery)
	{
		
		String[] vars = sjswQuery.getVariables();
		while(sjswQuery.hasNext())
		{
			ISelectStatement sjss = sjswQuery.next();
			String data = sjss.getVar(vars[0]).toString();
			String service = sjss.getVar(vars[1]).toString();
			double cost = (Double) sjss.getVar(vars[2]);
			double serGenericCost = 0.0;
			if (serGenericCostHash.containsKey(service))
			{
				serGenericCost = serGenericCostHash.get(service);
			}
			String key = data+"&&"+service;
			dataSerCostHash.put(key,  cost+serGenericCost);
		}
	}
	
	private void processSerForDataCount(ISelectWrapper sjswQuery)
	{
		
		String[] vars = sjswQuery.getVariables();
		while(sjswQuery.hasNext())
		{
			ISelectStatement sjss = sjswQuery.next();
			String data = sjss.getVar(vars[0]).toString();
			double count = (Double) sjss.getVar(vars[1]);
			dataSerCount.put(data,  count);
		}
	}
	
	
	private ISelectWrapper processQuery(String query, IEngine engine){
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();		
		sjsw.getVariables();
		System.out.println(query);
		return sjsw;
		*/
		return wrapper;
	}
	
	
	//create the final grid
	public void createGrid(ArrayList<Object[]> outputList)
	{
		String[] columnNames = new String[7];
		columnNames[serIdx] = "Service Name";
		columnNames[dataIdx] = "Data Name";
		columnNames[fullICDIdx] = "Count of Full ICDs Replaced";
		columnNames[partialIdx] = "Count of Partial ICDs Replaced";
		columnNames[costIdx] = "Service Build Cost";
		columnNames[susCostIdx] = "Annual Service Sustainment Cost";
		columnNames[icdSavingsIdx] = "Annual ICD Savings";
		gfd.setColumnNames(columnNames);
		gfd.setDataList(outputList);
		JTable table = new JTable();
		
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setAutoCreateRowSorter(true);
		JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);
		JInternalFrame sysDecommissionSheet = new JInternalFrame();
		sysDecommissionSheet.setContentPane(scrollPane);
		pane.add(sysDecommissionSheet);
		sysDecommissionSheet.setClosable(true);
		sysDecommissionSheet.setMaximizable(true);
		sysDecommissionSheet.setIconifiable(true);
		sysDecommissionSheet.setTitle("ICD Service Cost Analysis");
		sysDecommissionSheet.setResizable(true);
		sysDecommissionSheet.pack();
		sysDecommissionSheet.setVisible(true);
		try {
			sysDecommissionSheet.setSelected(false);
			sysDecommissionSheet.setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	public void setConstants(double hourlyRate, double interfaceAnnualCost, double sustainmentPer){
		this.hourlyRate = hourlyRate;
		this.interfaceAnnualCost  = interfaceAnnualCost;
		this.sustainmentPer  = sustainmentPer;

	}

}
