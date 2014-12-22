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

import java.io.File;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.InterfaceReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the System Info Report
 * Used in conjunction with SystemInfoGenListener
 */
public class InterfaceReportProcessor {
	static final Logger logger = LogManager.getLogger(InterfaceReportProcessor.class.getName());
	String tapCoreEngine = "TAP_Core_Data";
	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	Hashtable<String,Hashtable> masterHash;
	ArrayList<ArrayList<String>> allInterfaces;
	ArrayList<String> interfaceHeaders;
	ArrayList<String> sheetsList;
	/**
	 * Runs a query on a specific database and puts the results in the masterHash for a system
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runQuery(String engineName, String query) {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		try{
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		while (wrapper.hasNext()) {
			SesameJenaSelectStatement sjss = wrapper.next();
			String interfaceName = (String)sjss.getVar(names[0]);
			ArrayList<String> interfaceRow = new ArrayList<String>();
			interfaceRow.add(interfaceName);
			for(int i=1;i<names.length-2;i++)
			{
				String varType = names[i];
				String var = (String)sjss.getVar(names[i]);
				Hashtable<String,Integer> hash = masterHash.get(varType);
				int count = 1;
				if(hash.containsKey(var))
				{
					count+=hash.get(var);
				}
				hash.put(var,count);
				interfaceRow.add(var);
			}
			interfaceRow.add((String)sjss.getVar(names[names.length-2]));
			interfaceRow.add((String)sjss.getVar(names[names.length-1]));
			allInterfaces.add(interfaceRow);
		}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engineName);
		}
	}
	
	/**
	 * Processes and stores the system info queries and calls the system info writer to output the system info report
	 */
	public void generateInterfaceReport() {
		masterHash = new Hashtable<String,Hashtable>();
		allInterfaces = new ArrayList<ArrayList<String>>();
		interfaceHeaders = new ArrayList<String>();
		sheetsList = new ArrayList<String>();
		
		//checking to see if databases are loaded
		String engineName = tapCoreEngine;
		try
		{
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			if(engine==null)
				throw new NullPointerException();
			
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engineName);
			return;
		}
		
		processQueries();
		
		
		
		
		InterfaceReportWriter writer = new InterfaceReportWriter();
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports" + System.getProperty("file.separator");
		String writeFileName = "Interface_Report_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		logger.info(fileLoc);	
		
		String templateFileName = "Interface_Visual_Template.xlsx";
		String templateLoc = workingDir + folder + templateFileName;

		writer.exportInterfaceReport(fileLoc,templateLoc,sheetsList,masterHash,interfaceHeaders,allInterfaces);
		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\"+writeFileName);
	}

	/**
	 * Identifies and runs all the queries required for the system info report.
	 * Stores values in masterHash 
	 */
	public void processQueries() {

		sheetsList.add("Data");
		sheetsList.add("Format");
		sheetsList.add("Frequency");
		sheetsList.add("Protocol");
		
		masterHash.put("Data", new Hashtable<String,Integer>());
		masterHash.put("Format", new Hashtable<String,Integer>());
		masterHash.put("Frequency", new Hashtable<String,Integer>());
		masterHash.put("Protocol", new Hashtable<String,Integer>());
		
		//System Names
		String interfaceQuery = "SELECT DISTINCT ?Interface ?Data ?Format ?Frequency ?Protocol ?UpstreamSystem ?DownstreamSystem WHERE {{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?hasFormat <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?Format <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DForm>;}{?hasFreq <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?Frequency <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DFreq>;}{?hasProtocol <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;} {?Protocol <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DProt>;}{?UpstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?DownstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Interface ?carries ?Data;}{?UpstreamSystem ?Upstream ?Interface ;}{?Interface ?Downstream ?DownstreamSystem ;}{?Interface ?hasFormat ?Format}{?Interface ?hasFreq ?Frequency}{?Interface ?hasProtocol ?Protocol}}";
		
		//run all queries and store them in the masterHash
		runQuery(tapCoreEngine, interfaceQuery);

		interfaceHeaders.add("Interface");
		interfaceHeaders.add("Data");
		interfaceHeaders.add("Format");
		interfaceHeaders.add("Frequency");
		interfaceHeaders.add("Protocol");
		interfaceHeaders.add("UpstreamSystem");
		interfaceHeaders.add("DownstreamSystem");

	}

}