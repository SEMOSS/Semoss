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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.BasicReportWriter;
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
public class DHMSMSystemSORAccessTypeReportProcessor {
	static final Logger logger = LogManager.getLogger(DHMSMSystemSORAccessTypeReportProcessor.class.getName());
	Hashtable<String,String> dataLatencyTypeHash = new Hashtable<String,String>();
	Hashtable<String,String> dataAccessTypeHash = new Hashtable<String,String>();
	String hrCoreEngine = "HR_Core";
	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	Hashtable<String,Hashtable<String,Object>> masterHash;
	ArrayList<String> sysList;
	ArrayList<String> headersList;
	
	public void setDataLatencyTypeHash(Hashtable<String,String> dataLatencyTypeHash)
	{
		this.dataLatencyTypeHash = dataLatencyTypeHash;
	}
	
	public void setDataAccessTypeHash(Hashtable<String,String> dataAccessTypeHash)
	{
		this.dataAccessTypeHash = dataAccessTypeHash;
	}
	
	/**
	 * Runs a query on a specific database and puts the results in the masterHash for a system
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runQuery(String engineName, String query) {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		for(String head : names)
			if(!headersList.contains(head))
				headersList.add(head);
		try {
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				String sys = (String)sjss.getVar(names[0]);
				if(masterHash.containsKey(sys))
				{
					Hashtable<String, Object> sysHash = (Hashtable<String,Object>) masterHash.get(sys);
					for (int colIndex = 1; colIndex < names.length; colIndex++) {
						String varName = names[colIndex];
						Object val = sjss.getVar(names[colIndex]);
						if (val != null) {
							if (val instanceof Double) {
								sysHash.put(varName, val);
							}
							else
							{
								if(sysHash.containsKey(varName))
									val = (String) sysHash.get(varName) +", " + (String) val;
								sysHash.put(varName, val);
							}
						}
					}
				}
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runSystemListQuery(String engineName, String query) {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();
		try {
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				Hashtable<String,Object> sysHash = new Hashtable<String,Object>();
				masterHash.put((String) sjss.getVar(names[0]),sysHash);
				sysList.add((String) sjss.getVar(names[0]));
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Processes and stores the system info queries and calls the system info writer to output the system info report
	 */
	public void runReport() {
		masterHash = new Hashtable<String,Hashtable<String,Object>>();
		sysList = new ArrayList<String>();
		headersList = new ArrayList<String>();
		
		processQueries();
		
		BasicReportWriter writer = new BasicReportWriter();
		String folder = "\\export\\Reports\\";
		String writeFileName = "System_SOR_Access_Type_Report_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		writer.makeWorkbook(fileLoc);
		logger.info(fileLoc);
		
		ArrayList<Object[]> masterList = writer.makeListFromHash(headersList, sysList, masterHash);

		writer.writeListSheet("System Info",headersList,masterList);
		writer.writeWorkbook();
				
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\"+writeFileName);
	}

	/**
	 * Identifies and runs all the queries required for the system info report.
	 * Stores values in masterHash 
	 */
	public void processQueries() {

		//System Names
		String sysNameQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?Owner}}ORDER BY ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)(<http://health.mil/ontologies/Concept/SystemOwner/Central>)}";
		
		runSystemListQuery(hrCoreEngine, sysNameQuery);

		DHMSMHelper dhelp = new DHMSMHelper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine);
		dhelp.setUseDHMSMOnly(false);
		dhelp.runData(engine);

		headersList.add("System");
		headersList.add("Integrated_Data_Objects_System_Is_Record_Of");
		headersList.add("Hybrid_Data_Objects_System_Is_Record_Of");
		headersList.add("Manual_Data_Objects_System_Is_Record_Of");
		headersList.add("Realtime_Data_Objects_System_Is_Record_Of");
		headersList.add("Near_Realtime_Data_Objects_System_Is_Record_Of");
		headersList.add("Archived_Data_Objects_System_Is_Record_Of");
		headersList.add("Ignored_Data_Objects_System_Is_Record_Of");

		for(String system : sysList)
		{
			Hashtable<String, Object> sysHash = masterHash.get(system);
			ArrayList<String> dataObjectList = dhelp.getAllDataFromSys(system, "C");
			String integrated="";
			String hybrid="";
			String manual="";
			String real="";
			String near="";
			String archive="";
			String ignore="";
			for(String data : dataObjectList)
			{
				String dataAccessType = dataAccessTypeHash.get(data);
				if(dataAccessType!=null)
				{
					if(dataAccessType.equals("Integrated"))
						integrated+=data+", ";
					else if(dataAccessType.equals("Hybrid"))
						hybrid+=data+", ";
					else if(dataAccessType.equals("Manual"))
						manual+=data+", ";
				}
				String dataLatencyType = dataLatencyTypeHash.get(data);
				if(dataLatencyType!=null)
				{
					if(dataLatencyType.equals("Real"))
						real+=data+", ";
					else if(dataLatencyType.equals("NearReal"))
						near+=data+", ";
					else if(dataLatencyType.equals("Archive"))
						archive+=data+", ";
					else if(dataLatencyType.equals("Ignore"))
						ignore+=data+", ";
				}
			}
			if(integrated.length()>=2)
				integrated=integrated.substring(0,integrated.length()-2);
			if(hybrid.length()>=2)
				hybrid=hybrid.substring(0,hybrid.length()-2);
			if(manual.length()>=2)
				manual=manual.substring(0,manual.length()-2);
			if(real.length()>=2)
				real=real.substring(0,real.length()-2);
			if(near.length()>=2)
				near=near.substring(0,near.length()-2);
			if(archive.length()>=2)
				archive=archive.substring(0,archive.length()-2);
			if(ignore.length()>=2)
				ignore=ignore.substring(0,ignore.length()-2);
			sysHash.put("Integrated_Data_Objects_System_Is_Record_Of",integrated);
			sysHash.put("Hybrid_Data_Objects_System_Is_Record_Of",hybrid);
			sysHash.put("Manual_Data_Objects_System_Is_Record_Of",manual);
			sysHash.put("Realtime_Data_Objects_System_Is_Record_Of",real);
			sysHash.put("Near_Realtime_Data_Objects_System_Is_Record_Of",near);
			sysHash.put("Archived_Data_Objects_System_Is_Record_Of",archive);
			sysHash.put("Ignored_Data_Objects_System_Is_Record_Of",ignore);
			masterHash.put(system,sysHash);
		}		

	}

}