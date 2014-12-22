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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.NewScrollBarUI;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class DHMSMSysDecommissionReport {
	static final Logger logger = LogManager.getLogger(DHMSMSysDecommissionReport.class.getName());

	GridFilterData gfd = new GridFilterData();
	//hashtable storing all the data objects and the time they are needed: real, near-real, or archive
	Hashtable<String,String> dataLatencyTypeHash = new Hashtable<String,String>();
	
	//hashtable storing all the data objects and the time they are needed: manual, hybrid, or integrated
	Hashtable<String,String> dataAccessTypeHash = new Hashtable<String,String>();

	
	//arrayList storing prioritizied site list
	ArrayList<String> sitePriorityList = new ArrayList<String>();
	
	//hastable of years and the sites at that year
	Hashtable<Integer, ArrayList<String>> yearToSiteHash = new Hashtable<Integer, ArrayList<String>>();
	
	//hashtable storing all the sites and the systems they have
	//and the reverse, hashtable storing all the systems and their sites
	Hashtable<String, ArrayList<String>> siteToSysHash = new Hashtable<String, ArrayList<String>>();
	Hashtable<String, ArrayList<String>> sysToSiteHash = new Hashtable<String, ArrayList<String>>();
	
	//hashtable storing all the systems, their data objects, and their costs
	Hashtable<String, Hashtable<String,Double>> sysToDataToCostHash = new Hashtable<String, Hashtable<String,Double>>();

	//hashtable storing all the systems, and the data objects they read
	Hashtable<String, Hashtable<String,String>>  sysToDataReadHash = new Hashtable<String, Hashtable<String,String>> ();
	
	//hashtable storing all the systems, and the data objects they read
	Hashtable<String, Hashtable<String,Double>>  siteLatLongHash = new Hashtable<String, Hashtable<String,Double>> ();
	
	//hashtable storing all results by site, system, and then specific piece
	Hashtable<String,Hashtable<String,Hashtable<String,Hashtable<String,Object>>>> masterHash = new Hashtable<String,Hashtable<String,Hashtable<String,Hashtable<String,Object>>>> ();
	
	//hashtable store system recommendation
	Hashtable<String, String> sysToRecHash = new Hashtable<String, String>();
	
	//starting year
	Date startingDate;
	public static final int startYear = 2015;
	public static final int startMonth = 8;
	public static final int startDay = 1;
	public static final int workHoursInDay = 8;
	
	public static final String dataKey = "Data";
	public static final String loeKey = "LOE";
	public static final String startKey = "StartDate";
	public static final String endKey = "EndDate";
	public static final String resourceKey = "Resource";
	public static final String accessTypeKey = "AccessType";
	public static final String latencyTypeKey = "LatencyType";
	public static final String pilotKey = "Pilot";
	public static final String sunset = "Sunset";
	public static final String interim = "Interim";
	public static final String modernize = "Modernize";
	
	public static final String real = "Real";
	public static final String nearReal = "NearReal";
	public static final String archive = "Archive";
	public static final String integrated = "Integrated";
	public static final String hybrid = "Hybrid";
	public static final String manual = "Manual";
	
	public static final String deployment = "Deployment$Deployment";
	
	double realDeployOfPilotPer = .25;
	double nearDeployOfPilotPer = .25;
	double archiveDeployOfPilotPer = .20;
	double archivePer = .3;
	double nearRealPer = .6;
	double hybridPer = 1.0;
	double manualPer = 0.0;
	
	
	
	public DHMSMSysDecommissionReport()
	{
	}
	
	private ArrayList<String> getDataObjectList()
	{
		ArrayList<String> dataList = new ArrayList<String>();
		for(String data : dataLatencyTypeHash.keySet())
		{
			String accessType = dataLatencyTypeHash.get(data);
			if (!accessType.equals("Ignore"))
			{
				dataList.add(data);
			}
		}
		return dataList;
	}
	
	
	public void setDataLatencyTypeHash(Hashtable<String,String> dataLatencyTypeHash)
	{
		this.dataLatencyTypeHash = dataLatencyTypeHash;
	}
	
	public void setDataAccessTypeHash(Hashtable<String,String> dataAccessTypeHash)
	{
		this.dataAccessTypeHash = dataAccessTypeHash;
	}
	
	
	
	public void runCalculation()
	{
		ArrayList<String> dataList= getDataObjectList();
		SystemTransitionOrganizer sysTransOrganizer = new SystemTransitionOrganizer(dataList);
		siteToSysHash = sysTransOrganizer.getSiteToSysHash();
		sysToSiteHash = sysTransOrganizer.getSysToSiteHash();
		sysToDataToCostHash = sysTransOrganizer.getSysDataLOEHash();
		sysToDataReadHash = sysTransOrganizer.getSysReadDataHash();
		siteLatLongHash = sysTransOrganizer.getSiteLatLongHash();
		
		Set<String> siteList = siteToSysHash.keySet();
		Vector <String> siteListV = new Vector<String>(siteList);
		Collections.sort(siteListV);
		sitePriorityList = new ArrayList<String>(siteListV);
		
		Calendar c = Calendar.getInstance();
		c.set(startYear, startMonth, startDay);
		startingDate = c.getTime();
		//ultimately to be removed.
		//we expect data to be chunked with prioritization
		int siteChunk = 0;
		while(siteChunk*20<sitePriorityList.size())
		{
			ArrayList<String> currListSites = new ArrayList<String>();
			for(int i=siteChunk*20;i<siteChunk*20+20&&i<sitePriorityList.size();i++)
			{
				currListSites.add(sitePriorityList.get(i));
			}
			yearToSiteHash.put(siteChunk,currListSites);
			siteChunk++;
		}
		
		int max = 0;
		for(int key : yearToSiteHash.keySet())
			if(key>max)
				max = key;
		for(int i=0;i<=max;i++)
		{
			if(yearToSiteHash.containsKey(i))
			{
				ArrayList<String> siteChunkToProcess = yearToSiteHash.get(i);
 				processSiteChunk(siteChunkToProcess,i);
			}
		}
		ArrayList<Object[]> outputArray = convertToArrayList();
		//convertToArrayListSmaller();
		
		createGrid(outputArray);
	}
	

	private void processSiteChunk(ArrayList<String> siteChunkToProcess,int chunkIndex)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(startingDate);
		cal.add(Calendar.YEAR, chunkIndex);
		Date chunkStartDate = cal.getTime();
		
		for(String site : siteChunkToProcess)
		{
			ArrayList<String> sysAtSiteList = siteToSysHash.get(site);
			if(sysAtSiteList!=null)
			{
				for(String system : sysAtSiteList)
				{
					Hashtable<String,Double> dataAndLOEForSys = sysToDataToCostHash.get(system);
					if(dataAndLOEForSys!=null && !sysToRecHash.containsKey(system))
					{
						//getting the total LOE and longest LOE for the given site-system combination				
						double dataSum=0.0;
						double dataMax=0.0;
						double deployDataSum = 0.0;
						//assume archive unless proven other wise
						String sysLatencyType = archive;
						String sysAccessType = manual;
						String dataMaxType="";
						Hashtable<String,Double> pilotLOEHash = new Hashtable<String,Double>();
	
						for(String dataService : dataAndLOEForSys.keySet())
						{
							double loe = dataAndLOEForSys.get(dataService);
							String data = dataService.substring(0, dataService.indexOf("$"));
							String latencyType = dataLatencyTypeHash.get(data);
							String accessType = dataAccessTypeHash.get(data);
							double pilotLOE = getPilotLOEForDataFromLatency(latencyType,loe);
							pilotLOE = getPilotLOEForDataFromAccess(accessType,pilotLOE);
							deployDataSum += getDeploymentLOEForData(latencyType,pilotLOE);
							dataSum +=pilotLOE;
							if(dataMax<pilotLOE)
							{
								dataMax = pilotLOE;
								dataMaxType = latencyType;
							}
							pilotLOEHash.put(dataService,pilotLOE);
							sysAccessType = updateCurrentAccessType(sysAccessType, accessType);
							sysLatencyType = updateCurrentLatencyType(sysLatencyType, latencyType);
						}
						
						if(sysLatencyType.equals(real))
							sysToRecHash.put(system,  modernize);
						else if (sysLatencyType.equals(nearReal))
							sysToRecHash.put(system, interim);
						else if (sysLatencyType.equals(archive))
							sysToRecHash.put(system,  sunset);
						
						if(sysAccessType.equals(real))
							sysToRecHash.put(system,  modernize);
						else if (sysLatencyType.equals(nearReal))
							sysToRecHash.put(system, interim);
						else if (sysLatencyType.equals(archive))
							sysToRecHash.put(system,  sunset);
						
						int resources = (int) Math.ceil(dataSum/dataMax);
						Date systemPilotStartDate = chunkStartDate; //start of the chunk for system
						Date systemPilotEndDate = getNewEndDate(systemPilotStartDate,dataMax); //when the whole system ends the pilot

	
						for(String dataService : pilotLOEHash.keySet())
						{
							String data = dataService.substring(0, dataService.indexOf("$"));
							String latencyType = dataLatencyTypeHash.get(data);
							String accessType = dataAccessTypeHash.get(data);
							double pilotLOE = pilotLOEHash.get(dataService);
							//adding in the pilot LOE first, then will add a separate element for each specific site this system is deployed at
							
							addDataToMasterHash(site,system,dataService,pilotLOE,systemPilotStartDate,systemPilotEndDate,resources,accessType, latencyType, true);
						}
						//person hours for 4 months
						double deployFixedSum = resources * 16* 5*8;
						//total LOE is either what was calculated, or maximum fixed sum
						double deployTotalLOE = Math.min(deployDataSum,  deployFixedSum);
						
						//gets the loe for the max data object
						double deployDataMax = getDeploymentLOEForData(dataMaxType,dataMax);
						
						Date systemDeployStartDate = systemPilotEndDate;
						ArrayList<String> orderedSitesForSystem = orderSitesForSystem(system);	
						orderedSitesForSystem.remove(site);
						for(String siteForSystem : orderedSitesForSystem)
						{
							Date dataDeployEndDate;
							Date siteStartDate = getStartDateForSite(siteForSystem);
							Date siteEndDate;
							if(deployDataSum<deployFixedSum)
							{
								dataDeployEndDate = getNewEndDate(systemDeployStartDate, deployDataMax);
								siteEndDate = getNewEndDate(siteStartDate, deployDataMax);
							}
							else
							{
								dataDeployEndDate = addDaysToDate(systemDeployStartDate, 122); 
								siteEndDate = addDaysToDate(siteStartDate, 122);
							}

							if(siteStartDate.before(systemDeployStartDate)) //site has already started
								addDataToMasterHash(siteForSystem,system,deployment,deployTotalLOE,siteStartDate,siteEndDate,resources,sysAccessType, sysLatencyType, false);
							else
								addDataToMasterHash(siteForSystem,system,deployment,deployTotalLOE,systemDeployStartDate,dataDeployEndDate,resources, sysAccessType, sysLatencyType, false);
							systemDeployStartDate = dataDeployEndDate;
						}
						
						//remove from site and sys lists so we dont have to process again
						for(String siteForSystem : orderedSitesForSystem)
						{
							ArrayList<String> sysList = siteToSysHash.get(siteForSystem);
							sysList.remove(system);
						}
				
						sysToSiteHash.remove(system);
						sysToSiteHash.put(system,new ArrayList<String>());
					}
				}
			}
		}
	}
	//add site-system-data object to masterhash
	private void addDataToMasterHash(String site,String system,String data,double loe,Date startDate,Date endDate,int resources,String accessType, String latencyType, boolean pilot)
	{
		Hashtable<String, Hashtable<String, Hashtable<String,Object>>> systemHash;
		if(masterHash.containsKey(site))
			systemHash = masterHash.get(site);
		else
			systemHash = new Hashtable<String, Hashtable<String, Hashtable<String,Object>>>();
		
		Hashtable<String, Hashtable<String,Object>> dataHash;
		if(systemHash.containsKey(system))
			dataHash = systemHash.get(system);
		else
			dataHash = new Hashtable<String, Hashtable<String,Object>>();
			
		Hashtable<String,Object> propHash;
		if(dataHash.containsKey(data))
			propHash=dataHash.get(data);
		else
			propHash = new Hashtable<String,Object> ();
			
		propHash.put(loeKey, loe);
		propHash.put(startKey, startDate);
		propHash.put(endKey, endDate);
		propHash.put(resourceKey,resources);
		propHash.put(accessTypeKey,accessType);
		propHash.put(latencyTypeKey, latencyType);
		propHash.put(pilotKey,pilot);
		
		dataHash.put(data,propHash);
		systemHash.put(system,dataHash);
		masterHash.put(site,systemHash);
		
	}
	
	private Date getNewEndDate(Date date, double loe)
	{
		int days = ((Double)Math.ceil(loe/workHoursInDay / 5 * 7)).intValue();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, days);
		Date retDate = cal.getTime();
		return retDate;
	}
	
	private Date addDaysToDate(Date date, int days)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, days);
		Date retDate = cal.getTime();
		return retDate;
	}
	//these calculations are arbitrary and based off of data federation estimates for now.
	private double getPilotLOEForDataFromLatency(String latencyType, double loe)
	{
		if(latencyType.equals(real))
			return loe;
		else if(latencyType.equals(nearReal))
			loe = loe*nearRealPer;
		else if(latencyType.equals(archive))
			loe = loe*archivePer;
		else
			logger.info("Didn't find an latency type");
		return loe;

	}
	
	private double getPilotLOEForDataFromAccess(String accessType, double loe)
	{
		if(accessType.equals(integrated))
			return loe;
		else if(accessType.equals(hybrid))
			loe = loe*hybridPer;
		else if(accessType.equals(manual))
			loe = loe*manualPer;
		else
			logger.info("Didn't find an access type");
		return loe;

	}
	
	private String updateCurrentLatencyType(String sysLatencyType, String latencyType)
	{
		if(sysLatencyType.equals(integrated))
			return sysLatencyType;
		else if(sysLatencyType.equals(hybrid))
		{
			if (latencyType.equals(integrated))
				return integrated;
		}
		else if(sysLatencyType.equals(manual))
		{
			if (latencyType.equals(integrated))
				return integrated;
			else if (latencyType.equals(hybrid))
				return hybrid;
		}
		
		return sysLatencyType;
	}
	
	private String updateCurrentAccessType(String sysAccessType, String accessType)
	{
		if(sysAccessType.equals(real))
			return sysAccessType;
		else if(sysAccessType.equals(nearReal))
		{
			if (accessType.equals(real))
				return real;
		}
		else if(sysAccessType.equals(archive))
		{
			if (accessType.equals(real))
				return real;
			else if (accessType.equals(nearReal))
				return nearReal;
		}
		
		return sysAccessType;
	}
	
	//these calculations are arbitrary and based off of data federation estimates for now.
	private double getDeploymentLOEForData(String accessType, double pilotLOE)
	{
		if(accessType.equals(real))
			pilotLOE = realDeployOfPilotPer*pilotLOE;
		else if(accessType.equals(nearReal))
			pilotLOE = nearDeployOfPilotPer*pilotLOE;
		else if(accessType.equals(archive))
			pilotLOE = archiveDeployOfPilotPer*pilotLOE;
		else
			logger.info("Didn't find an access type");
		return pilotLOE;
	}
	
	private ArrayList<String> orderSitesForSystem(String system)
	{
		ArrayList<String> sitesForSystem = sysToSiteHash.get(system);
		if(sitesForSystem == null)
			return sitesForSystem;
		ArrayList<String> orderedSitesForSystem = new ArrayList<String>();
		int max = 0;
		for(int key : yearToSiteHash.keySet())
			if(key>max)
				max = key;
		
		for(int i=0;i<=max;i++)
		{
			if(yearToSiteHash.containsKey(i))
			{
				
				ArrayList<String> sitesForYear = yearToSiteHash.get(i);
				for(String site: sitesForYear)
				{
					if(sitesForSystem.contains(site))
					{
						orderedSitesForSystem.add(site);
						sitesForSystem.remove(site);
					}
				}

			}
		}
		
		orderedSitesForSystem.addAll(sitesForSystem);
		return orderedSitesForSystem;
		
	}
	private Date getStartDateForSite(String siteForSystem)
	{
		int yearToStart=0;
		for(int year : yearToSiteHash.keySet())
		{
			ArrayList<String> sites = yearToSiteHash.get(year);
			if(sites.contains(siteForSystem))
			{
				yearToStart=year;
				break;
			}
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(startingDate);
		cal.add(Calendar.YEAR, yearToStart);
		Date retStartDate = cal.getTime();
		return retStartDate;
	}

	private ArrayList<Object[]> convertToArrayList()
	{
		ArrayList<Object[]> masterList = new ArrayList<Object[]>();
		for(String site : masterHash.keySet())
		{
			Hashtable<String, Hashtable<String, Hashtable<String,Object>>> systemHash = masterHash.get(site);
			for(String system : systemHash.keySet())
			{
				Hashtable<String, Hashtable<String,Object>> dataHash = systemHash.get(system);
				Date startDate = new Date();
				Date latestEndDate = new Date();
				int resources=0;
				double loe = 0.0;

				for(String data : dataHash.keySet())
				{
					Hashtable<String,Object> propHash = dataHash.get(data);
					startDate = ((Date)propHash.get(startKey));
					Date endDate = ((Date)propHash.get(endKey)); 
					if(latestEndDate == null || latestEndDate.before(endDate))
						latestEndDate = endDate;
					if(resources == 0)
						resources = (Integer)propHash.get(resourceKey);
					loe+=(Double)propHash.get(loeKey);
				}
				

				DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	//			Date startDate = ((Date)propHash.get(startKey));
				String startDateString = df.format(startDate);
	//			Date endDate = ((Date)propHash.get(endKey));      
				String endDateString = df.format(latestEndDate);
				long systemDateDiff = Math.abs(latestEndDate.getTime() - startDate.getTime());
				long systemDateDiffDays = systemDateDiff / (24 * 60 * 60 * 1000);
				String sysToRec = sysToRecHash.get(system);
				String siteStartDateString = df.format(getStartDateForSite(site));
				
				Object[] systemSiteDataPropRow = new Object[10];
				systemSiteDataPropRow[0] = system;
				systemSiteDataPropRow[1] = site;
				systemSiteDataPropRow[2] = sysToRec;
				systemSiteDataPropRow[3] = siteStartDateString;
				systemSiteDataPropRow[4] = startDateString;
				systemSiteDataPropRow[5] = endDateString;
				systemSiteDataPropRow[6] = systemDateDiffDays;
				systemSiteDataPropRow[7] = loe;
				systemSiteDataPropRow[8] = resources;
				systemSiteDataPropRow[9] = loe*150;
				masterList.add(systemSiteDataPropRow);
				
			}
		}
		return masterList;
//		System.out.println(masterList);
	}
	
	/**
	 * Creates grid of result data.
	 * 
	 * @param outputList ArrayList<Object[]>	List of data to be set
	 */
	public void createGrid(ArrayList<Object[]> outputList)
	{
		String [] names = new String[]{"System", "Site", "TAP Recommendation","Site Start Date", "System Start Date", "System End Date","Date Elapsed", "LOE", "Resources", "Total Cost"};
		String title = "DHMSM System Decomission Report";
		gfd.setColumnNames(names);
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
		sysDecommissionSheet.setTitle(title);
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
	
	
//	private void convertToArrayListSmaller()
//	{
//		for(String site : masterHash.keySet())
//		{
//			Hashtable<String, Hashtable<String, Hashtable<String,Object>>> systemHash = masterHash.get(site);
//			for(String system : systemHash.keySet())
//			{
//				Hashtable<String, Hashtable<String,Object>> dataHash = systemHash.get(system);
//				Date startDate = null;
//				Date latestEndDate = null;
//				double totalLOE = 0.0;
//				int resources = 0;
//				for(String data : dataHash.keySet())
//				{
//					Hashtable<String,Object> propHash = dataHash.get(data);
//					Date currDate = (Date) propHash.get(endKey);
//					if(latestEndDate==null ||latestEndDate.before(currDate))
//					{
//						startDate = (Date) propHash.get(startKey);
//						latestEndDate = currDate;
//					}
//					totalLOE = totalLOE + (Double)propHash.get(loeKey);
//					resources = (Integer) propHash.get(resourceKey);
//				}
//
//				DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
//				String startDateString = df.format(startDate);
//				Date endDate = latestEndDate;      
//				String endDateString = df.format(endDate);
//				String sysToRec = sysToRecHash.get(system);
//				String siteStartDateString = df.format(getStartDateForSite(site));
//				
//			//	System.out.println(system + "$" + site + "$" + sysToRec + "$" + siteStartDateString + "$" + startDateString + "$" + endDateString + "$" + totalLOE + "$" + resources);
//
//			}
//		}
//	}
	
	public Hashtable<String, Hashtable<String, Double>> getSiteLatLongHash() 
	{
		return siteLatLongHash;
	}
}
