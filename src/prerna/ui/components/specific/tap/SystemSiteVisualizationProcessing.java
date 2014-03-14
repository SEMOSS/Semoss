package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

public class SystemSiteVisualizationProcessing {

	private Hashtable<String, Hashtable<String, Hashtable<String, Object>>> allData = new Hashtable<String, Hashtable<String, Hashtable<String, Object>>>();

	private Hashtable<String, Date> startDateForSystem = new Hashtable<String, Date>();
	private Hashtable<String, Date> endDateForSystem = new Hashtable<String, Date>();
	private Hashtable<String, Double> fhpForSystem = new Hashtable<String, Double>();
	private Hashtable<String, Double> hssForSystem = new Hashtable<String, Double>();
	private Hashtable<String, Double> hsdForSystem = new Hashtable<String, Double>();
	private Hashtable<String, ArrayList<String>> systemsForSite = new Hashtable<String, ArrayList<String>>();

	public void setAllData(Hashtable<String, Hashtable<String, Hashtable<String, Object>>> data)
	{
		this.allData = data;
	}

	public void constructHash()
	{
		for( String site : allData.keySet() )
		{
			Hashtable<String, Hashtable<String, Object>> siteHash = allData.get(site);

			for ( String system : siteHash.keySet())
			{
				// DETERMINE SYSTEMS FOR ALL SITES
				if(!systemsForSite.containsKey(site))
				{
					ArrayList<String> sys = new ArrayList<String>();
					systemsForSite.put(site, sys);
				}
				else
				{
					systemsForSite.get(site).add(system);
				}
				

				Hashtable<String, Object> dataObjectHash = siteHash.get(system);

				// DETERMINE START DATE FOR ALL SYSTEMS
				if(!startDateForSystem.containsKey(system))
				{
					startDateForSystem.put(system, (Date) dataObjectHash.get("PUT KEY FOR DATE"));
				}
				else
				{
					Date oldStartDate = startDateForSystem.get(system);
					Date newStartDate = (Date) dataObjectHash.get("PUT KEY FOR DATE");
					if(newStartDate.before(oldStartDate))
					{
						startDateForSystem.put(system, newStartDate);
					}
				}

				// DETERMINE END DATE FOR ALL SYSTEMS
				if(!endDateForSystem.containsKey(system))
				{
					endDateForSystem.put(system, (Date) dataObjectHash.get("PUT KEY FOR DATE"));
				}
				else
				{
					Date oldEndDate = endDateForSystem.get(system);
					Date newEndDate = (Date) dataObjectHash.get("PUT KEY FOR DATE");
					if(newEndDate.before(oldEndDate))
					{
						endDateForSystem.put(system, newEndDate);
					}
				}

				
				
				
				
				
				
				//TODO: fix and correct this once final thing is being sent
				// DETERMINE FHP FOR ALL SYSTEMS
				if(!fhpForSystem.containsKey(system))
				{
					//fhpForSystem.put(system, (Double) dataObjectHash.get("PUT KEY FOR DATE"));
					fhpForSystem.put(system, (double) 0);
				}
				// DETERMINE HSS FOR ALL SYSTEMS
				if(!hssForSystem.containsKey(system))
				{
					hssForSystem.put(system, (double) 0);
				}
				// DETERMINE HSD FOR ALL SYSTEMS
				if(!hsdForSystem.containsKey(system))
				{
					hsdForSystem.put(system, (double) 0);
				}



			}


		}



	}


}
