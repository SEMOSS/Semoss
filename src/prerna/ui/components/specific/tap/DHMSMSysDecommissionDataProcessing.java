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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

public class DHMSMSysDecommissionDataProcessing {

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

	private Set<String> listOfSystems = new HashSet<String>();
	private Set<String> listOfSites = new HashSet<String>();

	//private Hashtable<String, Double> systemCost = new Hashtable<String, Double>();
	private Hashtable<String, Double> siteCost = new Hashtable<String, Double>();

	private Double costPerHr = 150.0;

	public Hashtable<Integer, Object> constructHash()
	{
		Hashtable<String, Hashtable<String, Double>> siteLatLongHash = dataSource.getSiteLatLongHash();

		Hashtable<String, Double> cumSysCost = new Hashtable<String, Double>();

		for( String s : listOfSystems)
		{
			cumSysCost.put(s, (double) 0);
		}


		Hashtable<Integer, Object> output = new Hashtable<Integer, Object>();
		Integer time = 2014;
		Date endingDate = getLatestDate();
		Integer endTime = endingDate.getYear() + 1900 + 1;
		while( time <= endTime)
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

			Hashtable<String, ArrayList<Double>> currentDeployedSystemsCost = new Hashtable<String, ArrayList<Double>>();

			output.put(time, new Hashtable<String, Object>());
			Hashtable<String, Object> divergence = (Hashtable<String, Object>) output.get(time);
			divergence.put("system", new Hashtable<String, Object>());
			divergence.put("site", new Hashtable<String, Object>());

			Hashtable<String, Object> siteHashList = (Hashtable<String, Object>) divergence.get("site");
			Hashtable<String, ArrayList<String>> globalStatusForSys = new Hashtable<String, ArrayList<String>>();
			for(String site : listOfSites)
			{
				siteHashList.put(site, new Hashtable<String, Object>());
				Hashtable<String, Object> propHash = (Hashtable<String, Object>) siteHashList.get(site);
				propHash.put("TCostSite", siteCost.get(site)*costPerHr);
				if(siteLatLongHash.get(site) != null)
				{
					propHash.put("Lat", siteLatLongHash.get(site).get("LAT"));
					propHash.put("Long", siteLatLongHash.get(site).get("LONG"));
				}
				propHash.put("SystemForSite", new Hashtable<String, Object>());
				Hashtable<String, Object> sysAtSite = (Hashtable<String, Object>) propHash.get("SystemForSite");
				for (String sys : systemsForSite.get(site))
				{
					sysAtSite.put(sys, new Hashtable<String, Object>());
					Hashtable<String, Object> sysAtSitePropHash = (Hashtable<String, Object>) sysAtSite.get(sys);
					sysAtSitePropHash.put("Cost", loeForSiteSystem.get(site).get(sys)*costPerHr);
					sysAtSitePropHash.put("Resources", resourceForSiteSystem.get(site).get(sys));
					sysAtSitePropHash.put("Pilot", pilotForSiteSystem.get(site).get(sys));
					sysAtSitePropHash.put("AccessType", accessTypeForSiteSystem.get(site).get(sys));

					// logic to determine status of system at given site
					String status = "";
					if(!startDateForSiteSystem.get(site).get(sys).before(yearEnd))
					{
						status = "Not Started";
					}
					else if(endDateForSiteSystem.get(site).get(sys).before(yearStart))
					{
						status = "Decommissioned";
					}
					else
					{
						status = "In Progress";
						if(!currentDeployedSystemsCost.containsKey(sys))
						{
							ArrayList<Double> sysCostList = new ArrayList<Double>();
							sysCostList.add(loeForSiteSystem.get(site).get(sys));
							currentDeployedSystemsCost.put(sys, sysCostList);
						}
						else
						{
							ArrayList<Double> sysCostList = currentDeployedSystemsCost.get(sys);
							sysCostList.add(loeForSiteSystem.get(site).get(sys));
						}

					}
					
					sysAtSitePropHash.put("Status", status);

					// logic to determine global status of system
					if(!globalStatusForSys.containsKey(sys))
					{
						ArrayList<String> statArr = new ArrayList<String>();
						statArr.add(status);
						globalStatusForSys.put(sys, statArr);
					}
					else
					{
						ArrayList<String> statArr = globalStatusForSys.get(sys);
						statArr.add(status);
					}
				}
			}

			Hashtable<String, Object> sysHashList = (Hashtable<String, Object>) divergence.get("system");
			for( String sys : listOfSystems)
			{
				sysHashList.put(sys, new Hashtable<String, Object>());
				Hashtable<String, Object> propHash = (Hashtable<String, Object>) sysHashList.get(sys);

				// total sys cost calculation
				Double yearCostForSys = (double) 0;
				ArrayList<Double> deploymentCostArr = currentDeployedSystemsCost.get(sys);
				if(deploymentCostArr != null)
				{
					for(int i = 0; i < deploymentCostArr.size(); i++)
					{
						yearCostForSys += deploymentCostArr.get(i);
					}
				}
				yearCostForSys *= costPerHr;
				Double pastCostForSys = cumSysCost.get(sys);
				cumSysCost.put(sys, pastCostForSys + yearCostForSys);
				propHash.put("TCostSystem", cumSysCost.get(sys));

				// status determination
				ArrayList<String> statArr = globalStatusForSys.get(sys);
				if(statArr.contains("Not Started") && !statArr.contains("In Progress") && !statArr.contains("Decommissioned"))
				{
					propHash.put("AggregatedStatus", "Not Started");
				}
				else if(!statArr.contains("Not Started") && !statArr.contains("In Progress") && statArr.contains("Decommissioned"))
				{
					propHash.put("AggregatedStatus", "Decommissioned");
				}
				else
				{
					propHash.put("AggregatedStatus", "In Progress");
				}
			}
			time++;
		}

		// round all cost values
		for( Integer years : output.keySet() )
		{
			Hashtable<String, Object> siteSystem = (Hashtable<String, Object>) ((Hashtable<String, Object>) output.get(years)).get("site");
			Hashtable<String, Object> aggregatedSystems = (Hashtable<String, Object>) ((Hashtable<String, Object>) output.get(years)).get("system");
			for ( String site : siteSystem.keySet() )
			{
				Double TCostSite = (Double) ((Hashtable<String, Object>) siteSystem.get(site)).get("TCostSite");
				TCostSite = (double) Math.round(TCostSite);
				Hashtable<String, Object> systemsAtSite = (Hashtable<String, Object>) ((Hashtable<String, Object>) siteSystem.get(site)).get("SystemForSite");
				for ( String sys : systemsAtSite.keySet())
				{
					Hashtable<String, Object> sysProp = (Hashtable<String, Object>) systemsAtSite.get(sys);
					Double loe = (Double) sysProp.get("Cost");
					loe = (double) Math.round(loe);
				}
			}
			for( String sys : aggregatedSystems.keySet() )
			{
				Hashtable<String, Object> sysProp = (Hashtable<String, Object>) aggregatedSystems.get(sys);
				Double TCostSystem = (Double) sysProp.get("TCostSystem");
				TCostSystem = (double) Math.round(TCostSystem);
			}
		}
		
		return output;
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
						if(prop.equals(DHMSMSysDecommissionReport.resourceKey))
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
						else if(prop.equals(DHMSMSysDecommissionReport.pilotKey))
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
						else if(prop.equals(DHMSMSysDecommissionReport.accessTypeKey))
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
						else if(prop.equals(DHMSMSysDecommissionReport.startKey))
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
						else if(prop.equals(DHMSMSysDecommissionReport.endKey))
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
								Date newEndDate = (Date) propHash.get(DHMSMSysDecommissionReport.endKey);
								if(newEndDate.after(oldEndDate))
								{
									innerHash.put(system, newEndDate);
								}
							}
						}
						else if(prop.equals(DHMSMSysDecommissionReport.loeKey))
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
								Double addLOE = (Double) propHash.get(DHMSMSysDecommissionReport.loeKey);
								innerHash.put(system, oldLOE + addLOE);
							}

							//							// determine overall system cost
							//							if(!systemCost.containsKey(system))
							//							{
							//								systemCost.put(system, (Double) propHash.get(dataSource.loeKey));
							//							}
							//							else
							//							{
							//								Double oldLOW = systemCost.get(system);
							//								Double addLOE = (Double) propHash.get(dataSource.loeKey);
							//								systemCost.put(system, oldLOW + addLOE);
							//							}

							// determine overall site cost
							if(!siteCost.containsKey(site))
							{
								siteCost.put(site, (Double) propHash.get(DHMSMSysDecommissionReport.loeKey));
							}
							else
							{
								Double oldLOW = siteCost.get(site);
								Double addLOE = (Double) propHash.get(DHMSMSysDecommissionReport.loeKey);
								siteCost.put(site, oldLOW + addLOE);
							}
						}
					}
				}
			}
		}
	}



}
