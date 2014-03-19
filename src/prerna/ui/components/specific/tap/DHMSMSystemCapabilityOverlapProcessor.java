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
public class DHMSMSystemCapabilityOverlapProcessor {
	
	Logger logger = Logger.getLogger(getClass());
	String hrCoreEngine = "HR_Core";
	Hashtable<String,String> capToFunctionalAreaHash = new Hashtable<String,String> ();
	Hashtable<String,Hashtable> masterHash=  new Hashtable<String,Hashtable>();
	ArrayList<String> headersList = new ArrayList<String>();
	ArrayList<String> capList;

	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);	
	
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public void runCapabilityListQuery() {
		try {
		JList repoList = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[]) repoList.getSelectedValues();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine);
		
		String query = "SELECT DISTINCT ?Capability ?CapabilityFunctionalArea WHERE {BIND(<http://health.mil/ontologies/Concept/DHMSM/DHMSM> as ?dhmsm) {?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?dhmsm <http://semoss.org/ontologies/Relation/TaggedBy> ?cap}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;} {?CapabilityGroup ?ConsistsOfCapability ?Capability;}}";

		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();

		String[] names = wrapper.getVariables();

			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				capToFunctionalAreaHash.put((String) sjss.getVar(names[0]),(String) sjss.getVar(names[1]));
			}
		} catch (Exception e) {
			Utility.showError("Cannot find engine: "+hrCoreEngine);
		}
	}
	
	
	/**
	 * Processes and stores the system info queries and calls the system info writer to output the system info report
	 */
	public void generateSystemCapabilityOverlapReport() {
		
		DHMSMHelper dhelp = new DHMSMHelper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreEngine);
		dhelp.runData(engine);
		
		runCapabilityListQuery();
		
		headersList.add("Capability");
		headersList.add("Functional_Area");
		headersList.add("Capability_Creates_Or_Reads_Data_Object");
		headersList.add("Data_Object");
		headersList.add("Systems_that_are_Source_of_Record_of_Data_Object");
		headersList.add("Systems_that_Read_Data_Object");
		
		for(String capability : capToFunctionalAreaHash.keySet())
		{
			
			ArrayList<String> dataObjectsCreatedByCapability = dhelp.getAllDataFromCap(capability, "C");
			ArrayList<ArrayList<String>> sysAndDataObjectsCapCreatesSystemSOR= dhelp.getSysAndData(capability, "C", "C");
			ArrayList<ArrayList<String>> sysAndDataObjectsCapCreatesSystemReads= dhelp.getSysAndData(capability, "C", "R");
			
			for(String data : dataObjectsCreatedByCapability)
			{
				Hashtable<String,Object> capEntry = new Hashtable<String,Object>();
				capEntry.put("Functional_Area",capToFunctionalAreaHash.get(capability));
				capEntry.put("Capability_Creates_Or_Reads_Data_Object", "Creates");
				capEntry.put("Data_Object", data);
				capEntry.put("Systems_that_are_Source_of_Record_of_Data_Object", makeConcat(sysAndDataObjectsCapCreatesSystemSOR,data));
				capEntry.put("Systems_that_Read_Data_Object", makeConcat(sysAndDataObjectsCapCreatesSystemReads,data));
				masterHash.put(capability+"$$$"+data,capEntry);
			}
			
			ArrayList<String> dataObjectsReadByCapability = dhelp.getAllDataFromCap(capability, "R");
			ArrayList<ArrayList<String>> sysAndDataObjectsCapReadsSystemSOR= dhelp.getSysAndData(capability, "R", "C");
			ArrayList<ArrayList<String>> sysAndDataObjectsCapReadsSystemReads= dhelp.getSysAndData(capability, "R", "R");
			
			for(String data : dataObjectsReadByCapability)
			{
				if(!masterHash.containsKey(capability+"$$$"+data))
				{
					Hashtable<String,Object> capEntry = new Hashtable<String,Object>();
					capEntry.put("Functional_Area",capToFunctionalAreaHash.get(capability));
					capEntry.put("Capability_Creates_Or_Reads_Data_Object", "Reads");
					capEntry.put("Data_Object", data);
					capEntry.put("Systems_that_are_Source_of_Record_of_Data_Object", makeConcat(sysAndDataObjectsCapReadsSystemSOR,data));
					capEntry.put("Systems_that_Read_Data_Object", makeConcat(sysAndDataObjectsCapReadsSystemReads,data));
					masterHash.put(capability+"$$$"+data,capEntry);
				}
			}
		}
		
		
		SystemInfoGenWriter writer = new SystemInfoGenWriter();
		String folder = "\\export\\Reports\\";
		String writeFileName = "Capability_System_Overlap_"+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_") + ".xlsx";

		String fileLoc = workingDir + folder + writeFileName;
		logger.info(fileLoc);	

		writer.exportSystemInfoReport(fileLoc, new ArrayList<String>(masterHash.keySet()),headersList,masterHash);
		
		Utility.showMessage("Report generation successful! \n\nExport Location: " + workingDir + "\\export\\Reports\\"+writeFileName);
	}
	
	public String makeConcat(ArrayList<ArrayList<String>> sysAndDataList,String data)
	{
		String retVal = "";
		if(sysAndDataList==null||sysAndDataList.size()==0)
			return retVal;
		for(int i=0;i<sysAndDataList.size();i++)
		{
			if(sysAndDataList.get(i).get(1).equals(data))
			{
				if(retVal.length()==0)
					retVal = sysAndDataList.get(i).get(0);
				else
					retVal += ", "+ sysAndDataList.get(i).get(0);
			}
		}
		return retVal;
	}


}