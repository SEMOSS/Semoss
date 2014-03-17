package prerna.ui.components.specific.tap;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

public class SystemSiteVisualizationProcessing {

	private Hashtable<String, Hashtable<String, Hashtable<String, Hashtable<String, Object>>>> allData = new Hashtable<String, Hashtable<String, Hashtable<String, Hashtable<String, Object>>>>();
	public void setAllData(Hashtable<String, Hashtable<String, Hashtable<String, Hashtable<String, Object>>>> allData) {
		this.allData = allData;
	}

	private DHMSMSysDecommissionReport dataSource;
	public void setDataSource(DHMSMSysDecommissionReport dataSource) {
		this.dataSource = dataSource;
	}

	private Hashtable<String, ArrayList<String>> systemsForSite = new Hashtable<String, ArrayList<String>>();
	private Hashtable<String, Hashtable<String, Date>> startDateForSiteSystem = new Hashtable<String, Hashtable<String, Date>>();
	private Hashtable<String, Hashtable<String, Date>> endDateForSiteSystem = new Hashtable<String, Hashtable<String, Date>>();
	private Hashtable<String, Hashtable<String, Integer>> resourceForSiteSystem = new Hashtable<String, Hashtable<String, Integer>>();
	private Hashtable<String, Hashtable<String, Double>> loeForSiteSystem = new Hashtable<String, Hashtable<String, Double>>();
	private Hashtable<String, Hashtable<String, String>> accessTypeForSiteSystem = new Hashtable<String, Hashtable<String, String>>();
	private Hashtable<String, Hashtable<String, Boolean>> pilotForSiteSystem = new Hashtable<String, Hashtable<String, Boolean>>();
	//	private Hashtable<String, Double> fhpForSystem = new Hashtable<String, Double>();
	//	private Hashtable<String, Double> hssForSystem = new Hashtable<String, Double>();
	//	private Hashtable<String, Double> hsdForSystem = new Hashtable<String, Double>();

	private ArrayList<String> listOfSystems = new ArrayList<String>();
	private ArrayList<String> listOfSites = new ArrayList<String>();

	private Hashtable<String, Double> systemCost = new Hashtable<String, Double>();
	private Hashtable<String, Double> siteCost = new Hashtable<String, Double>();
	private Hashtable<String, String> globalStatusForSys = new Hashtable<String, String>();

	public void constructHash()
	{
		Hashtable<Integer, Object> output = new Hashtable<Integer, Object>();
		Integer time = 2015;
		Date endingDate = getLatestDate();
		Integer endTime = endingDate.getYear() + 1900 + 1;
		while( time < endTime)
		{
			DateFormat d = new SimpleDateFormat("yyyy-MM-dd");
			Date yearEnd = null;
			Date yearStart = null;
			try {
				yearEnd = d.parse(time + "-12-31");
				yearStart = d.parse(time + "-01-01");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			output.put(time, new Hashtable<String, Object>());
			Hashtable<String, Object> divergence = (Hashtable<String, Object>) output.get(time);
			divergence.put("system", new Hashtable<String, Object>());
			divergence.put("site", new Hashtable<String, Object>());

			Hashtable<String, Object> siteHashList = (Hashtable<String, Object>) divergence.get("site");
			for(String site : listOfSites)
			{
				siteHashList.put(site, new Hashtable<String, Object>());
				Hashtable<String, Object> propHash = (Hashtable<String, Object>) siteHashList.get(site);
				propHash.put("TCostSite", siteCost.get(site));
				//TODO: ADD LAT/LONG
				propHash.put("Lat", "x");
				propHash.put("Long", "x");
				propHash.put("SystemForSite", new Hashtable<String, Object>());
				Hashtable<String, Object> sysAtSite = (Hashtable<String, Object>) propHash.get("SystemForSite");
				for (String sys : systemsForSite.get(site))
				{
					sysAtSite.put(sys, new Hashtable<String, Object>());
					Hashtable<String, Object> sysAtSitePropHash = (Hashtable<String, Object>) sysAtSite.get(sys);
					sysAtSitePropHash.put("Cost", loeForSiteSystem.get(site).get(sys));
					sysAtSitePropHash.put("Rescources", resourceForSiteSystem.get(site).get(sys));
					sysAtSitePropHash.put("Pilot", pilotForSiteSystem.get(site).get(sys));
					sysAtSitePropHash.put("AccessType", accessTypeForSiteSystem.get(site).get(sys));
					// logic to determine status of system at given site
					String status = "";
					if(startDateForSiteSystem.get(site).get(sys).before(yearStart))
					{
						status = "Not Started";
					}
					else if(startDateForSiteSystem.get(site).get(sys).after(yearStart) || startDateForSiteSystem.get(site).get(sys).before(yearEnd) )
					{
						status = "In Progress";
					}
					if(!endDateForSiteSystem.get(site).get(sys).after(yearStart))
					{
						status = "Decommissioned";
					}
					sysAtSitePropHash.put("Status", status);

					// logic to determine global status of system
					if(!globalStatusForSys.contains(sys))
					{
						globalStatusForSys.put(sys, status);
					}
					else
					{
						String curStatus = globalStatusForSys.get(sys);
						if(!status.equals(curStatus))
						{
							globalStatusForSys.put(sys, "In Progress");
						}
					}
				}
			}

			Hashtable<String, Object> sysHashList = (Hashtable<String, Object>) divergence.get("system");
			for( String sys : listOfSystems)
			{

				sysHashList.put(sys, new Hashtable<String, Object>());
				Hashtable<String, Object> propHash = (Hashtable<String, Object>) sysHashList.get(sys);
				propHash.put("TCostSystem", systemCost.get(sys));
				propHash.put("AggregatedStatus", globalStatusForSys.get(sys));
			}

			time++;
		}
	}

	private Date getLatestDate()
	{
		Date endDate = null;
		Date latestDate = determineLatestDate();
		int endingYear = 1900 + latestDate.getYear();
		DateFormat df = new SimpleDateFormat("mm/dd/yyyy");
		try {
			endDate = df.parse("12/31/" + String.valueOf(endingYear));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return endDate; 
	}

	private Date determineLatestDate()
	{
		Date date = null;
		for(String site : endDateForSiteSystem.keySet())
		{
			Hashtable<String, Date> innerHash = endDateForSiteSystem.get(site);
			for (String sys : innerHash.keySet())
			{
				if(date == null || date.before(innerHash.get(sys)))
				{
					date = innerHash.get(sys);
				}
			}
		}
		return date;
	}

	public void decomposeData()
	{
		// iterate across each site
		for( String site : allData.keySet() )
		{
			Hashtable<String, Hashtable<String, Hashtable<String, Object>>> siteHash = allData.get(site);
			listOfSites.add(site);

			// iterate across each system at a given site
			for ( String system : siteHash.keySet())
			{
				listOfSystems.add(system);
				// STORE SYSTEMS FOR ALL SITES
				if(!systemsForSite.containsKey(site))
				{
					ArrayList<String> sys = new ArrayList<String>();
					sys.add(system);
					systemsForSite.put(site, sys);
				}
				else if(!systemsForSite.get(site).contains(system))
				{
					systemsForSite.get(site).add(system);
				}

				Hashtable<String, Hashtable<String, Object>> dataObjectHash = siteHash.get(system);

				// iterate across each data object
				for(String data : dataObjectHash.keySet())
				{
					Hashtable<String, Object> propHash = dataObjectHash.get(data);

					// iterate across all properties for a given data object and aggregate at the site-sys lvl
					for (String prop : propHash.keySet())
					{
						if(prop.equals(dataSource.resourceKey))
						{
							if(!resourceForSiteSystem.containsKey(site))
							{
								Hashtable<String, Integer> innerHash = new Hashtable<String, Integer>();
								innerHash.put(system, (Integer) propHash.get(prop));
								resourceForSiteSystem.put(site, innerHash);
							}
							else if(!resourceForSiteSystem.get(site).containsKey(system))
							{
								Hashtable<String, Integer> innerHash = resourceForSiteSystem.get(site);
								innerHash.put(system, (Integer) propHash.get(prop));
							}
						}
						else if(prop.equals(dataSource.pilotKey))
						{
							if(!pilotForSiteSystem.containsKey(site))
							{
								Hashtable<String, Boolean> innerHash = new Hashtable<String, Boolean>();
								innerHash.put(system, (Boolean) propHash.get(prop));
								pilotForSiteSystem.put(site, innerHash);
							}
							else if(!pilotForSiteSystem.get(site).containsKey(system))
							{
								Hashtable<String, Boolean> innerHash = pilotForSiteSystem.get(site);
								innerHash.put(system, (Boolean) propHash.get(prop));
							}
						}
						else if(prop.equals(dataSource.accessTypeKey))
						{
							if(!accessTypeForSiteSystem.containsKey(site))
							{
								Hashtable<String, String> innerHash = new Hashtable<String, String>();
								innerHash.put(system, (String) propHash.get(prop));
								accessTypeForSiteSystem.put(site, innerHash);
							}
							else if(!accessTypeForSiteSystem.get(site).containsKey(system))
							{
								Hashtable<String, String> innerHash = accessTypeForSiteSystem.get(site);
								innerHash.put(system, (String) propHash.get(prop));
							}
						}
						else if(prop.equals(dataSource.startKey))
						{
							if(!startDateForSiteSystem.containsKey(site))
							{
								Hashtable<String, Date> innerHash = new Hashtable<String, Date>();
								innerHash.put(system, (Date) propHash.get(prop));
								startDateForSiteSystem.put(site, innerHash);
							}
							else if(!startDateForSiteSystem.get(site).containsKey(system))
							{
								Hashtable<String, Date> innerHash = startDateForSiteSystem.get(site);
								innerHash.put(system, (Date) propHash.get(prop));
							}
						}
						else if(prop.equals(dataSource.endKey))
						{
							if(!endDateForSiteSystem.containsKey(site))
							{
								Hashtable<String, Date> innerHash = new Hashtable<String, Date>();
								innerHash.put(system, (Date) propHash.get(prop));
								endDateForSiteSystem.put(site, innerHash);
							}
							else if(!endDateForSiteSystem.get(site).containsKey(system))
							{
								Hashtable<String, Date> innerHash = endDateForSiteSystem.get(site);
								innerHash.put(system, (Date) propHash.get(prop));
							}
							else
							{
								Hashtable<String, Date> innerHash = endDateForSiteSystem.get(site);
								Date oldEndDate = innerHash.get(system);
								Date newEndDate = (Date) propHash.get(dataSource.endKey);
								if(newEndDate.after(oldEndDate))
								{
									innerHash.put(system, newEndDate);
								}
							}
						}
						else if(prop.equals(dataSource.loeKey))
						{
							if(!loeForSiteSystem.containsKey(site))
							{
								Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
								innerHash.put(system, (Double) propHash.get(prop));
								loeForSiteSystem.put(site, innerHash);
							}
							else if(!loeForSiteSystem.get(site).containsKey(system))
							{
								Hashtable<String, Double> innerHash = loeForSiteSystem.get(site);
								innerHash.put(system, (Double) propHash.get(prop));
							}
							else
							{
								Hashtable<String, Double> innerHash = loeForSiteSystem.get(site);
								Double oldLOE = innerHash.get(system);
								Double addLOE = (Double) propHash.get(dataSource.loeKey);
								innerHash.put(system, oldLOE + addLOE);
							}

							// determine overall system cost
							if(!systemCost.contains(system))
							{
								systemCost.put(system, (Double) propHash.get(dataSource.loeKey));
							}
							else
							{
								Double oldLOW = systemCost.get(system);
								Double addLOE = (Double) propHash.get(dataSource.loeKey);
								systemCost.put(system, oldLOW + addLOE);
							}

							// determine overall site cost
							if(!siteCost.contains(site))
							{
								siteCost.put(site, (Double) propHash.get(dataSource.loeKey));
							}
							else
							{
								Double oldLOW = siteCost.get(system);
								Double addLOE = (Double) propHash.get(dataSource.loeKey);
								siteCost.put(site, oldLOW + addLOE);
							}
						}
					}
				}
			}
		}
	}



}
