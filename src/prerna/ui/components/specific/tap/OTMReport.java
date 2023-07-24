/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
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
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/

package prerna.ui.components.specific.tap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;

import prerna.engine.api.IDatabase;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.specific.OTMReportWriter;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

/** 
 * This class is responsible for generating the data from a series of queries for the OTMReport. The OTM report details LPI systems, interfaces,
 *  waves, and data sources. The output is into excel over a series of worksheets under the workbook name of OTMReport.
 *  
 *  Author: Joseph Vidalis  
 *  Email:  jvidalis@deloitte.com
 */

@SuppressWarnings("serial")
public class OTMReport extends TablePlaySheet {
	//Define Query
	private String sysInfoQuery = "SELECT DISTINCT ?System ?Description ?POC ?Central ?AvalActual ?AvalRequired ?SystemOwner  WHERE   { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI' ;} {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N' ;} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description ;} OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/POC> ?POC ;} OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> ?Central ;} OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?AvalActual ;} OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Availability-Required> ?AvalRequired ;} OPTIONAL {?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner> ;} {?System <http://semoss.org/ontologies/Relation/OwnedBy> ?SystemOwner ;} }ORDER BY ?System ";
	private String currentDownstreamQuery =  "SELECT DISTINCT ?System ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?UpstreamSystem ?DownstreamSystem  WHERE { {?UpstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI'} {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;}   	OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} MINUS { BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?carries)} {?UpstreamSystem <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;} BIND(?System AS ?DownstreamSystem) BIND('Upstream' AS ?type) }ORDER BY ?System";//TODO why is there a minus here?
	private String currentUpstreamQuery = "SELECT DISTINCT ?System ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?UpstreamSystem ?DownstreamSystem WHERE { {?DownstreamSystem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI'} {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) {?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?Interface ?carries ?Data;} OPTIONAL {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;}MINUS { BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?carries)} {?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;} {?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSystem ;} BIND(?System AS ?UpstreamSystem) BIND('Downstream' AS ?type)   }ORDER BY ?System";//TODO why is there a minus here?
	private String futureUpstreamQuery = "SELECT DISTINCT   ?System ?Interface ?Data (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?UpstreamSystem ?DownstreamSystem  WHERE {  	BIND(<http://health.mil/ontologies/Concept/System/MHS_GENESIS> AS ?DownstreamSystem)  	{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}   	{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}   	{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}   	{?Interface ?carries ?Data;}   	OPTIONAL {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} MINUS { BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?carries)}	  	{?System <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}   	{?Interface <http://semoss.org/ontologies/Relation/Consume> ?DownstreamSystem ;}   	BIND(?System AS ?UpstreamSystem)  	BIND('Upstream' AS ?type)   } ";//TODO why is there a minus here?
	private String futureDownstreamQuery = "SELECT DISTINCT   ?System ?Interface ?Data  (COALESCE(?format,'') AS ?Format) (COALESCE(?frequency,'') AS ?Frequency) (COALESCE(?protocol,'') AS ?Protocol) ?UpstreamSystem ?DownstreamSystem  WHERE {  	BIND(<http://health.mil/ontologies/Concept/System/MHS_GENESIS> AS ?UpstreamSystem)  	{?Interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface> ;}   	{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}   	{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}   	OPTIONAL {?carries <http://semoss.org/ontologies/Relation/Contains/Format> ?format ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Frequency> ?frequency ;} OPTIONAL{?carries <http://semoss.org/ontologies/Relation/Contains/Protocol> ?protocol ;} MINUS { BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?carries)}	  	{?UpstreamSystem <http://semoss.org/ontologies/Relation/Provide> ?Interface ;}   	{?Interface <http://semoss.org/ontologies/Relation/Consume> ?System ;}  	{?Interface ?carries ?Data;}   	BIND(?System AS ?DownstreamSystem)  	BIND('Upstream' AS ?type)   }ORDER BY ?System"; //TODO why is there a minus here?
	private String waveQuery = "SELECT DISTINCT ?System ?Wave ?Quarter ?Year  WHERE{    	{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}  	{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>}  	{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}  	{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>}  	{?YearQuarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}  	{?Year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}  	{?Quarter <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Quarter>}  	{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite}  	{?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite}  	{?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite}  	{?Wave <http://semoss.org/ontologies/Relation/BeginsOn> ?YearQuarter}  	{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Year}  	{?YearQuarter <http://semoss.org/ontologies/Relation/has> ?Quarter}  }ORDER BY ?System";
	private String forwardQuery = "SELECT DISTINCT ?System1 ?System2 (COUNT(?ICD1) AS ?ICDCOUNT) WHERE  {  	BIND(<http://health.mil/ontologies/Concept/System/MHS_GENESIS> AS ?System1)  	{?System1 <http://semoss.org/ontologies/Relation/Provide> ?ICD1}  	{?ICD1 <http://semoss.org/ontologies/Relation/Consume> ?System2}  }GROUP BY ?System1 ?System2 ORDER BY ?System2";
	private String reverseQuery = "SELECT DISTINCT ?System2 ?System1 (COUNT(?ICD1) AS ?ICDCOUNT) WHERE  {  	BIND(<http://health.mil/ontologies/Concept/System/MHS_GENESIS> AS ?System2)  	{?System1 <http://semoss.org/ontologies/Relation/Provide> ?ICD1}  	{?ICD1 <http://semoss.org/ontologies/Relation/Consume> ?System2}  }GROUP BY ?System1 ?System2 ORDER BY ?System2";
	private String dataSourceQuery = "SELECT DISTINCT ?System ?Data  WHERE { BIND(<http://health.mil/ontologies/Concept/SourceType/MigrationReference> AS ?sourceType ) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;} {?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI'} {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N' ;} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) {?DOS <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObjectSource> ;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?System <http://semoss.org/ontologies/Relation/Designated> ?DOS ;} {?DOS <http://semoss.org/ontologies/Relation/Delivers> ?Data ;}{?DOS <http://semoss.org/ontologies/Relation/LabeledAs> ?sourceType ;} } ";

	
	//Declare database engines
	private IDatabase TAP_Core_Data;
	private IDatabase FutureDB;
	private IDatabase TAP_Site_Data;
	
	private boolean showMessages = true;

	//Set Tap Core Database
	public void setTAP_Core_Data(IDatabase TAP_Core_Data) {
		this.TAP_Core_Data = TAP_Core_Data;
	}
	
	//Set Future Database
	public void setFutureDB(IDatabase FutureDB) {
		this.FutureDB = FutureDB;
	}
	//Set Future Database
	public void setTAP_Site_Data(IDatabase TAP_Site_Data) {
		this.TAP_Site_Data = TAP_Site_Data;
	}
	
	//Function called by the question once its set up. This is where to set up your queries
	public void createData() {
		//Initiate Tap_Core_Data database engine
		try{
			TAP_Core_Data = (IDatabase) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
			if(TAP_Core_Data==null)
				throw new IOException("Database not found");
		} catch(IOException e) {
			Utility.showError("Could not find necessary database: TAP_Core_Data. Cannot generate report. Updated");
			return;
		}
		
		try{
			FutureDB = (IDatabase) DIHelper.getInstance().getLocalProp("FutureDB");
			if(FutureDB==null)
				throw new IOException("Database not found");
		} catch(IOException e) {
			Utility.showError("Could not find necessary database: FutureDB. Cannot generate report. Updated");
			return;
		}
		
		try{
			TAP_Site_Data = (IDatabase) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
			if(TAP_Site_Data==null)
				throw new IOException("Database not found");
		} catch(IOException e) {
			Utility.showError("Could not find necessary database: TAP_Site_Data. Cannot generate report. Updated");
			return;
		}
		
		
		//Utility.showMessage("JV Getting Query \n"+sysInfoQuery);
		System.out.println("Running System Info Query");
		HashMap<String, Object> sysInfoData = getQueryDataWithHeaders(TAP_Core_Data, sysInfoQuery);
		
		System.out.println("Running Current Upstream Query");
		HashMap<String, Object> currentUpstreamData = getQueryDataWithHeaders(TAP_Core_Data, currentUpstreamQuery);
		
		System.out.println("Running Current Downstream Query");
		HashMap<String, Object> currentDownstreamData = getQueryDataWithHeaders(TAP_Core_Data, currentDownstreamQuery);
		
		System.out.println("Running Future Upstream Query to MHS GENESIS");
		HashMap<String, Object> futureUpstreamData = getQueryDataWithHeaders(FutureDB, futureUpstreamQuery);
		
		System.out.println("Running Future DownStream Query to MHS GENESIS");
		HashMap<String, Object> futureDownstreamData = getQueryDataWithHeaders(FutureDB, futureDownstreamQuery);
		
		System.out.println("Running Wave Query");
		HashMap<String, Object> waveData = getQueryDataWithHeaders(TAP_Site_Data, waveQuery);
		
		System.out.println("Querys Finished");
		HashMap<String, Object> dataSourceData = getQueryDataWithHeaders(TAP_Core_Data, dataSourceQuery);
		
		System.out.println("Running Forward ICD Count Query");
		HashMap<String, Object> forwardData = getQueryDataWithHeaders(FutureDB, forwardQuery);
		
		System.out.println("Running Forward ICD Count Query");
		HashMap<String, Object> reverseData = getQueryDataWithHeaders(FutureDB, reverseQuery);
		
		System.out.println("Writing Report");
		boolean success = writeReport(sysInfoData, currentUpstreamData, currentDownstreamData, futureUpstreamData, futureDownstreamData,  waveData, forwardData, reverseData, dataSourceData);
		
		//Display error message if write failed. Display location of file if it succeeded.
		if(showMessages)
		{
			if(success){
				Utility.showMessage("System Export Finished! File located in:\n" + OTMReportWriter.getFileLoc() );
			} else {
				Utility.showError("Error Creating Report!");
			}
		}
	}
	
	// This function gets the hashmap of a grid with data headers of a query to a specific engine
	private HashMap<String, Object> getQueryDataWithHeaders(IDatabase engine, String query){
		HashMap<String, Object> dataHash = new HashMap<String, Object>();

		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		String[] names = sjsw.getVariables();
		dataHash.put(DHMSMTransitionUtility.HEADER_KEY, names);

		ArrayList<Object[]> dataToAddArr = new ArrayList<Object[]>();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			Object[] dataRow = new Object[names.length];
			for(int i = 0; i < names.length; i++)
			{
				Object dataElem = sjss.getVar(names[i]);
				if(dataElem.toString().startsWith("\"") && dataElem.toString().endsWith("\""))
				{
					dataElem = dataElem.toString().substring(1, dataElem.toString().length()-1); // remove annoying quotes
				}
				dataRow[i] = dataElem;
			}
			dataToAddArr.add(dataRow);
		}

		dataHash.put(DHMSMTransitionUtility.DATA_KEY, dataToAddArr);

		return dataHash;
	}
	
	/**
	 * Writes an excel workbook from hash tables. The report is saved as OTMReport.xlsx and is generated from the template OTMTemplate.xlsx
	 *   
	 * @param sysInfoHash			Hashtable containing {System, Description, Owner}
	 * @param currentUpstreamHash	Hashtable containing {System, type, Interface, Data, Format, Frequency, Protocol, Upstream System, Downstream System}
	 * @param currentDownstreamHash	Hashtable containing {System, type, Interface, Data, Format, Frequency, Protocol, Upstream System, Downstream System}
	 * @param futureUpstreamHash	Hashtable containing {System, type, Interface, Data, Format, Frequency, Protocol, Upstream System, Downstream System}
	 * @param futureDownstreamHash	Hashtable containing {System, type, Interface, Data, Format, Frequency, Protocol, Upstream System, Downstream System}
	 * @param waveHash				Hashtable containing {System, Wave, Quarter, Year}
	 */
	private boolean writeReport(
			HashMap<String, Object> sysInfoHash, 
			HashMap<String, Object> currentUpstreamHash,
			HashMap<String, Object> currentDownstreamHash,
			HashMap<String, Object> futureUpstreamHash,
			HashMap<String, Object> futureDownstreamHash,
			HashMap<String, Object> waveHash,
			HashMap<String, Object> forwardHash,
			HashMap<String, Object> reverseHash,
			HashMap<String, Object> dataSourceHash)
	{
		Date d = new Date();
		String[] dateSplit = d.toString().split("\\s");
		String sysName = "OTMReport_" + dateSplit[1] + "_"+ dateSplit[2] + "_"+ dateSplit[5] +".xlsx";
		//Declare writer
		OTMReportWriter writer = new OTMReportWriter();
		//Create Workbook from Template
		writer.makeWorkbook(sysName, "OTMTemplate.xlsx");
		
		//Write System Data
		System.out.println("Writing System Information");
		writer.writeSystemDataSheet("1. System Information", sysInfoHash);
		
		//Write Current Upstream Data
		System.out.println("Writing Directionality");
		writer.writeICDCountSheet("6. Directionality", forwardHash, reverseHash);
		
		//Write Current Upstream Data
		System.out.println("Writing Current Upstream");
		writer.writeListSheet("2. Current Upstream", currentUpstreamHash);
		
		//Write Current Upstream Data
		System.out.println("Writing Current Downstream");
		writer.writeListSheet("3. Current Downstream", currentDownstreamHash);
		
		//Write Current Upstream Data
		System.out.println("Writing Future Upstream from MHS GENESIS");
		writer.writeListSheet("4. Future Upstream", futureUpstreamHash);
		
		//Write Current Upstream Data
		System.out.println("Writing Future Downstream to MHS GENESIS");
		writer.writeListSheet("5. Future Downstream", futureDownstreamHash);
		
		//Write Current Upstream Data
		System.out.println("Writing When Needed");
		writer.writeWaveSheet("7. When Needed", waveHash, sysInfoHash);
		
		System.out.println("Writing Data Sources");
		writer.writeListSheet("8. Data Sources", dataSourceHash, 0);
		
		//Save workbook and return
		return writer.writeWorkbook();
	}
	
	@Override
	public void refineView() {
	}

	@Override
	public void overlayView() {
	}

	@Override
	public void runAnalytics() {
	}

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		return null;
	}

	@Override
	public void processQueryData() {
		// TODO Auto-generated method stub
		
	}
}