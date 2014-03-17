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

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.JList;

import org.apache.log4j.Logger;

import prerna.poi.specific.SystemInfoGenWriter;
import prerna.poi.specific.TaskerGenerationWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class contains the queries and query processing required to gather the information needed to generate the System Info Report
 * Used in conjunction with SystemInfoGenListener
 */
public class DHMSMSystemSORAccessTypeReportProcessor {
	Logger logger = Logger.getLogger(getClass());
	Hashtable<String,String> dataAccessTypeHash = new Hashtable<String,String>();
	String hrCoreEngine = "HR_Core";
	String workingDir = System.getProperty("user.dir");
	Hashtable<String,Hashtable> masterHash;
	ArrayList<String> sysList;
	ArrayList<String> headersList;
	
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
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
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
					Hashtable sysHash = masterHash.get(sys);
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runSystemListQuery(String engineName, String query) {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Processes and stores the system info queries and calls the system info writer to output the system info report
	 */
	public void runReport() {
		masterHash = new Hashtable<String,Hashtable>();
		sysList = new ArrayList<String>();
		headersList = new ArrayList<String>();
		
		processQueries();
		
		SystemInfoGenWriter writer = new SystemInfoGenWriter();
		String folder = "\\export\\Reports\\";
		String writeFileName = "System_SOR_Access_Type_Report_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		logger.info(fileLoc);	

		writer.exportSystemInfoReport(fileLoc,sysList,headersList,masterHash);
		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\"+writeFileName);
	}

	/**
	 * Identifies and runs all the queries required for the system info report.
	 * Stores values in masterHash 
	 */
	public void processQueries() {

		//System Names
		String sysNameQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?Owner}}ORDER BY ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";
		
		runSystemListQuery(hrCoreEngine, sysNameQuery);

		DHMSMHelper dhelp = new DHMSMHelper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine);
		dhelp.setUseDHMSMOnly(false);
		dhelp.runData(engine);

		headersList.add("System");
		headersList.add("Num_Of_Realtime_Data_Objects_System_Is_Record_Of");
		headersList.add("Num_Of_Near_Realtime_Data_Objects_System_Is_Record_Of");
		headersList.add("Num_Of_Archived_Data_Objects_System_Is_Record_Of");
		headersList.add("Num_Of_Ignored_Data_Objects_System_Is_Record_Of");
		for(String system : sysList)
		{
			Hashtable sysHash = masterHash.get(system);
			ArrayList<String> dataObjectList = dhelp.getAllDataFromSys(system, "C");
			int realCount = 0;
			int nearCount = 0;
			int archivedCount = 0;
			int ignoredCount = 0;
			for(String data : dataObjectList)
			{
				String dataAccessType = dataAccessTypeHash.get(data);
				if(dataAccessType!=null)
				{
				if(dataAccessType.equals("Real"))
					realCount++;
				else if(dataAccessType.equals("NearReal"))
					nearCount++;
				else if(dataAccessType.equals("Archive"))
					archivedCount++;
				else if(dataAccessType.equals("Ignore"))
					ignoredCount++;
				}
			}
			sysHash.put("Num_Of_Realtime_Data_Objects_System_Is_Record_Of",realCount);
			sysHash.put("Num_Of_Near_Realtime_Data_Objects_System_Is_Record_Of",nearCount);
			sysHash.put("Num_Of_Archived_Data_Objects_System_Is_Record_Of",archivedCount);
			sysHash.put("Num_Of_Ignored_Data_Objects_System_Is_Record_Of",ignoredCount);
			masterHash.put(system,sysHash);
		}		

	}

}