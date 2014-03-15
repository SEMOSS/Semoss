package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

public class DHMSMSysDecommissionReport {
	Logger logger = Logger.getLogger(getClass());
	//hashtable storing all the data objects and the time they are needed: real, near-real, or archive
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
	
	//hashtable storing all results by site, system, and then specific piece
	Hashtable<String,Hashtable<String,Hashtable<String,Hashtable<String,Object>>>> masterHash = new Hashtable<String,Hashtable<String,Hashtable<String,Hashtable<String,Object>>>> ();
	
	//starting year
	Date startingDate;
	public static final int startYear = 2015;
	public static final int startMonth = 8;
	public static final int startDay = 1;
	public static final int workHoursInDay = 8;
	
	public static String dataKey = "Data";
	public static String loeKey = "LOE";
	public static String startKey = "StartDate";
	public static String endKey = "EndDate";
	public static String resourceKey = "Resource";
	public static String accessTypeKey = "AccessType";
	public static String pilotKey = "Pilot";
	
	public static final double realDeployOfPilotPer = .50;
	public static final double nearDeployOfPilotPer = .33;
	public static final double archiveDeployOfPilotPer = .67;
	
	public DHMSMSysDecommissionReport()
	{
		SystemTransitionOrganizer sysTransOrganizer = new SystemTransitionOrganizer();
		siteToSysHash = sysTransOrganizer.getSiteToSysHash();
		sysToSiteHash = sysTransOrganizer.getSysToSiteHash();
		sysToDataToCostHash = sysTransOrganizer.getSysDataLOEHash();
		sysToDataReadHash = sysTransOrganizer.getSysReadDataHash();
		
		Set<String> siteList = siteToSysHash.keySet();
		Vector <String> siteListV = new Vector(siteList);
		Collections.sort(siteListV);
		sitePriorityList = new ArrayList<String>(siteListV);
		
		Calendar c = Calendar.getInstance();
		c.set(startYear, startMonth, startDay);
		startingDate = c.getTime();
	}
	public void setDataAccessTypeHash(Hashtable<String,String> dataAccessTypeHash)
	{
		this.dataAccessTypeHash = dataAccessTypeHash;
	}
	
	public void runCalculation()
	{
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
		convertToArrayList();
	//	convertToArrayListSmaller();
		
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
					if(dataAndLOEForSys!=null)
					{
						//getting the total LOE and longest LOE for the given site-system combination				
						double dataSum=0.0;
						double dataMax=0.0;
						String dataMaxType="";
						Hashtable<String,Double> pilotLOEHash = new Hashtable<String,Double>();
	
						for(String data : dataAndLOEForSys.keySet())
						{
							double loe = dataAndLOEForSys.get(data);
							String accessType = dataAccessTypeHash.get(data);
							double pilotLOE = getPilotLOEForData(accessType,loe);
							dataSum +=pilotLOE;
							if(dataMax<pilotLOE)
							{
								dataMax = pilotLOE;
								dataMaxType = accessType;
							}
							pilotLOEHash.put(data,pilotLOE);
						}
						int resources = (int) Math.ceil(dataSum/dataMax);
						Date systemPilotStartDate = chunkStartDate; //start of the chunk for system
						Date systemPilotEndDate = getNewEndDate(systemPilotStartDate,dataMax,resources); //when the whole system ends the pilot
						double deployDataMax = getDeploymentLOEForData(dataMaxType,dataMax); //gets the loe for the max data object

						ArrayList<String> orderedSitesForSystem = orderSitesForSystem(system);		
						for(String data : pilotLOEHash.keySet())
						{
							String accessType = dataAccessTypeHash.get(data);
							double pilotLOE = pilotLOEHash.get(data);
							double deployLOE = getDeploymentLOEForData(accessType,pilotLOE);
							//adding in the pilot LOE first, then will add a separate element for each specific site this system is deployed at
							
							Date systemDeployStartDate = systemPilotEndDate;
							Date dataDeployEndDate = getNewEndDate(systemDeployStartDate, deployLOE,resources);
							
							addDataToMasterHash(site,system,data,pilotLOE+deployLOE,systemPilotStartDate,dataDeployEndDate,resources,accessType, true);
							systemDeployStartDate = getNewEndDate(systemDeployStartDate,deployDataMax,resources);
							dataDeployEndDate = getNewEndDate(systemDeployStartDate, deployLOE,resources);

							orderedSitesForSystem.remove(site);
							for(String siteForSystem : orderedSitesForSystem)
							{
								
								systemDeployStartDate = getNewEndDate(systemDeployStartDate,deployDataMax,resources);
								dataDeployEndDate = getNewEndDate(systemDeployStartDate, deployLOE,resources);

								Date siteStartDate = getStartDateForSite(siteForSystem);
								Date siteEndDate = getNewEndDate(siteStartDate, deployLOE,resources);
								if(siteStartDate.before(systemDeployStartDate)) //site has already started
									addDataToMasterHash(siteForSystem,system,data,deployLOE,siteStartDate,siteEndDate,resources,accessType, false);
								else
									addDataToMasterHash(siteForSystem,system,data,deployLOE,systemDeployStartDate,dataDeployEndDate,resources,accessType, false);

							}
							
						}
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
	private void addDataToMasterHash(String site,String system,String data,double loe,Date startDate,Date endDate,int resources,String accessType,boolean pilot)
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
		propHash.put(pilotKey,pilot);
		
		dataHash.put(data,propHash);
		systemHash.put(system,dataHash);
		masterHash.put(site,systemHash);
		
	}
	
	private Date getNewEndDate(Date date, double loe, int resources)
	{
		int days = ((Double)Math.ceil(loe/workHoursInDay / 5 * 7/resources)).intValue();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, days);
		Date retDate = cal.getTime();
		return retDate;
	}
	//these calculations are arbitrary and based off of data federation estimates for now.
	private double getPilotLOEForData(String accessType, double loe)
	{
		if(accessType.equals("Real"))
			return loe;
		else if(accessType.equals("NearReal"))
			loe = loe*.6;
		else if(accessType.equals("Archive"))
			loe = loe*.3;
		else
			logger.info("Didn't find an access type");
		return loe;

	}
	//these calculations are arbitrary and based off of data federation estimates for now.
	private double getDeploymentLOEForData(String accessType, double pilotLOE)
	{
		if(accessType.equals("Real"))
			pilotLOE = realDeployOfPilotPer*pilotLOE;
		else if(accessType.equals("NearReal"))
			pilotLOE = nearDeployOfPilotPer*pilotLOE;
		else if(accessType.equals("Archive"))
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

	private void convertToArrayList()
	{
		ArrayList<ArrayList<Object>> masterList = new ArrayList<ArrayList<Object>>();
		for(String site : masterHash.keySet())
		{
			Hashtable<String, Hashtable<String, Hashtable<String,Object>>> systemHash = masterHash.get(site);
			for(String system : systemHash.keySet())
			{
				Hashtable<String, Hashtable<String,Object>> dataHash = systemHash.get(system);
				for(String data : dataHash.keySet())
				{
					Hashtable<String,Object> propHash = dataHash.get(data);
					ArrayList<Object> siteSystemDataPropRow = new ArrayList<Object>();
					siteSystemDataPropRow.add(site);
					siteSystemDataPropRow.add(system);
					siteSystemDataPropRow.add(data);
					siteSystemDataPropRow.add(propHash.get(loeKey));
					siteSystemDataPropRow.add(propHash.get(startKey));
					siteSystemDataPropRow.add(propHash.get(endKey));
					siteSystemDataPropRow.add(propHash.get(resourceKey));
					siteSystemDataPropRow.add(propHash.get(accessTypeKey));
					siteSystemDataPropRow.add(propHash.get(pilotKey));
					
					DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
					// Get the date today using Calendar object.
					Date startDate = ((Date)propHash.get(startKey));      
					String startDateString = df.format(startDate);
					Date endDate = ((Date)propHash.get(endKey));      
					String endDateString = df.format(endDate);
					String siteStartDateString = df.format(getStartDateForSite(site));
					System.out.println(site + "$" + system + "$" + data + "$" + propHash.get(loeKey) + "$" + siteStartDateString + "$" + startDateString + "$" + endDateString + "$" + propHash.get(resourceKey) + "$" + propHash.get(accessTypeKey) + "$" + propHash.get(pilotKey));
					masterList.add(siteSystemDataPropRow);
				}
			}
		}
//		System.out.println(masterList);
	}
	
	
	private void convertToArrayListSmaller()
	{
		ArrayList<ArrayList<Object>> masterList = new ArrayList<ArrayList<Object>>();
		for(String site : masterHash.keySet())
		{
			Hashtable<String, Hashtable<String, Hashtable<String,Object>>> systemHash = masterHash.get(site);
			for(String system : systemHash.keySet())
			{
				Hashtable<String, Hashtable<String,Object>> dataHash = systemHash.get(system);
				Date startDate = null;
				Date latestEndDate = null;
				for(String data : dataHash.keySet())
				{
					Hashtable<String,Object> propHash = dataHash.get(data);
					Date currDate = (Date) propHash.get(endKey);
					if(latestEndDate==null ||latestEndDate.before(currDate))
					{
						startDate = (Date) propHash.get(startKey);
						latestEndDate = currDate;
					}
				}

				DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
				String startDateString = df.format(startDate);
				Date endDate = latestEndDate;      
				String endDateString = df.format(endDate);
				
				String siteStartDateString = df.format(getStartDateForSite(site));
				
				System.out.println(site + "$" + system + "$" + startDateString + "$" + endDateString + "$" + siteStartDateString);

			}
		}
//		System.out.println(masterList);
	}
}
