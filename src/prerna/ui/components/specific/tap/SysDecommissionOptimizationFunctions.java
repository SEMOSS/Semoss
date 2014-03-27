package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

public class SysDecommissionOptimizationFunctions {
	

	protected Logger logger = Logger.getLogger(getClass());
	
	//stores list of systems and then hashtable of systems to their min time and work volumes
	private Hashtable<String, Double> sysToMinTimeHashPerSite;
	private Hashtable<String, Double> sysToWorkVolHashPerSite;
	private Hashtable<String, Double> sysToMinTimeHashAllSites;
	private Hashtable<String, Double> sysToWorkVolHashAllSites;
	
	private Hashtable<String, Double> sysToResourceAllocationHash;
	private Hashtable<String, Double> sysToNumSimultaneousTransformHash;
	
    //hashtable storing all the systems, their data objects, and their loes
    Hashtable<String, Hashtable<String,Double>> sysToDataToLOEHash;
    //hashtable storing all the systems and their sites.
    Hashtable<String, ArrayList<String>> sysToSiteHash;
    
    private ArrayList<String> systemsWithNoSite;
    private Vector<String> sortedSysList;

	ArrayList <Object []> outputList;

	private static String costDB = "TAP_Cost_Data";
	private static String systemCostQuery = "SELECT DISTINCT ?sys ?data ?ser (SUM(?loe) AS ?cost) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase>} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass}{?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start}  {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser }{?data <http://semoss.org/ontologies/Relation/Input> ?GLitem}} GROUP BY ?sys ?data ?ser";

	private static String siteDB = "TAP_Site_Data";
	private static String systemSiteQuery = "SELECT DISTINCT ?System ?DCSite WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;}  {?SystemDCSite ?DeployedAt ?DCSite;}{?System ?DeployedAt1 ?SystemDCSite;} }";

	private double percentOfPilot;
	private double costPerHour;	
	private static final int workHoursInDay = 8;

	public int resourcesConstraint;
	private double resourcesPossible;

	public double timeConstraint;

	private double minNecessaryTimeAllSystems;
	private double minPossibleTimeAllSystems;
	private double workVolAllSysAllSites;
	private double costAllSysAllSites;

	public SysDecommissionOptimizationFunctions()
	{
		resourcesConstraint = 1000;
		timeConstraint = 9.09*365;
	}
	
	
	public void optimizeTime()
	{

		instantiate();
	
		//if necessary is greater than possible, we have more resources than we need so will recalculate R to be only necessary.
		if(minNecessaryTimeAllSystems>minPossibleTimeAllSystems)
		{
			minPossibleTimeAllSystems = minNecessaryTimeAllSystems;
			recalculateResourcesPossible();
		}
		else
			resourcesPossible = resourcesConstraint;
		
		calculateResourceAndOutput();

		System.out.println("Resources constraint: "+Math.ceil(resourcesConstraint));
		System.out.println("Resources used: "+Math.ceil(resourcesPossible));
		System.out.println("Time used in years: "+minPossibleTimeAllSystems / 365.0);
		System.out.println("Total cost: "+costAllSysAllSites);

	}
	public void optimizeResource()
	{
		instantiate();
		
		minPossibleTimeAllSystems = Math.max(minNecessaryTimeAllSystems,timeConstraint);
		recalculateResourcesPossible();
		
		calculateResourceAndOutput();
		
		System.out.println("Time constraint:"+timeConstraint / 365.0);
		System.out.println("Time actually used in years:"+minPossibleTimeAllSystems / 365.0);
		System.out.println("Resources used: "+Math.ceil(resourcesPossible));
		System.out.println("Total cost: "+costAllSysAllSites);
	}
	
	public void instantiate()
	{
		sysToMinTimeHashPerSite = new Hashtable<String, Double>();
		sysToWorkVolHashPerSite = new Hashtable<String, Double>();
		sysToMinTimeHashAllSites = new Hashtable<String, Double>();
		sysToWorkVolHashAllSites = new Hashtable<String, Double>();
		sysToDataToLOEHash = new Hashtable<String, Hashtable<String,Double>>();
		sysToSiteHash = new Hashtable<String, ArrayList<String>>();
		sysToResourceAllocationHash = new Hashtable<String, Double>();
		sysToNumSimultaneousTransformHash = new Hashtable<String, Double>();
		outputList = new ArrayList<Object[]>();
		systemsWithNoSite = new ArrayList<String>();
		sortedSysList = new Vector<String>();

		percentOfPilot = .2;
		costPerHour = 150.0;
		
		resourcesPossible = 0.0;
		minNecessaryTimeAllSystems = 0.0;
		minPossibleTimeAllSystems = 0.0;
		workVolAllSysAllSites = 0.0;
		costAllSysAllSites = 0.0;
		
		calculateMinTimeAndWorkVolPerSystemPerSite();
		calculateMinTimeAndWorkVolPerSystemAllSites();
		removeSystemsWithNoSite();
		sortSysList();
		calculateMinTimeAllSystemsAllSites();
		calculateWorkVolAllSystemsAllSites();
	}
	
	public void calculateResourceAndOutput()
	{
		calculateResourceAllocationPerSystem();
		calculateNumSysSimultaneousTransform();
		calculateTotalCost();
	
		makeArrayList();
	}
	
	/**
	 * Calculating the minimum time and work volume for each system.
	 * Also determining the shortest necessary time to transform all systems
	 * and storing in minNecessaryTimeAllSystems.
	 */
	public void calculateMinTimeAndWorkVolPerSystemPerSite()
	{
		ArrayList <Object []> systemDataCostList = createData(costDB,systemCostQuery);
		processSystemDataLOE(systemDataCostList);
		
		for(String sys : sysToDataToLOEHash.keySet())
		{
			Hashtable<String,Double> dataAndLOEForSys = sysToDataToLOEHash.get(sys);
			
			double loeSum=0.0;
			double loeMax=0.0;
			for(String data : dataAndLOEForSys.keySet())
			{
				double loeForData = convertLoeHoursToDays(dataAndLOEForSys.get(data));
				loeSum+=loeForData;
				if(loeForData>loeMax)
					loeMax = loeForData;
			}
			sysToMinTimeHashPerSite.put(sys,loeMax);
			sysToWorkVolHashPerSite.put(sys,loeSum);
		}
	}
	/**
	 * Multiplying the min time and work volume for each system by the number of sites.
	 * Also calculating the minNecessary and min possible time based on resources constraint for transforming all systems
	 */
	
	public void calculateMinTimeAndWorkVolPerSystemAllSites()
	{
		ArrayList <Object []> systemSiteList = createData(siteDB,systemSiteQuery);
		processSystemSiteHashTables(systemSiteList);
		
		for(String sys : sysToMinTimeHashPerSite.keySet())
		{
			//only using systems that are in the decomission list.
			//if we dont have site data, we are assuming the system is only deployed at one site.
			int numOfSites = 1;
			if(sysToSiteHash.containsKey(sys))
			{
				numOfSites = sysToSiteHash.get(sys).size();
			
				double minNecessaryTimePerSysPerSite = sysToMinTimeHashPerSite.get(sys);
				double minNecessaryTimePerSysAllSites = numOfSites*minNecessaryTimePerSysPerSite;
				
				double workVolPerSysPerSite = sysToWorkVolHashPerSite.get(sys);
				double workVolPerSysAllSites = numOfSites*workVolPerSysPerSite;
				
				sysToMinTimeHashAllSites.put(sys,minNecessaryTimePerSysAllSites);
				sysToWorkVolHashAllSites.put(sys,workVolPerSysAllSites);
			}
			else
			{
				systemsWithNoSite.add(sys);
			}
			
		}

	}
	
	public void removeSystemsWithNoSite()
	{
		for(String sys : systemsWithNoSite)
		{
			sysToMinTimeHashPerSite.remove(sys);
			sysToWorkVolHashPerSite.remove(sys);
			logger.info(sys+"...removing because no site data");
		}
	}
	
	public void calculateMinTimeAllSystemsAllSites()
	{
		for(String sys : sysToMinTimeHashAllSites.keySet())
		{
			double minNecessaryTimePerSysPerSites = sysToMinTimeHashPerSite.get(sys);
			
			if(minNecessaryTimePerSysPerSites>minNecessaryTimeAllSystems)
				minNecessaryTimeAllSystems=minNecessaryTimePerSysPerSites;
			
			double workVolPerSysAllSites = sysToWorkVolHashAllSites.get(sys);
			minPossibleTimeAllSystems+=workVolPerSysAllSites;
		}
		minPossibleTimeAllSystems = minPossibleTimeAllSystems / resourcesConstraint;
	}
	
	public void calculateWorkVolAllSystemsAllSites()
	{
		for(String sys : sysToWorkVolHashAllSites.keySet())
		{
			double workVolPerSysAllSites = sysToWorkVolHashAllSites.get(sys);
			workVolAllSysAllSites+=workVolPerSysAllSites;
		}
	}
	
	public void recalculateResourcesPossible()
	{
		for(String sys : sysToMinTimeHashAllSites.keySet())
		{
			double workVolPerSysAllSites = sysToWorkVolHashAllSites.get(sys);
			resourcesPossible+=workVolPerSysAllSites;
		}
		resourcesPossible = resourcesPossible / minPossibleTimeAllSystems;
	}
	
	public void calculateResourceAllocationPerSystem()
	{
		for(String sys : sysToWorkVolHashAllSites.keySet())
		{
			double resourceForSysAllSites = resourcesPossible*sysToWorkVolHashAllSites.get(sys) / workVolAllSysAllSites;
			sysToResourceAllocationHash.put(sys, resourceForSysAllSites);
		}
		
	}
	
	public void calculateNumSysSimultaneousTransform()
	{
		for(String sys : sysToResourceAllocationHash.keySet())
		{
			double numSimultaneousTransform = sysToResourceAllocationHash.get(sys)*sysToMinTimeHashPerSite.get(sys)/sysToWorkVolHashPerSite.get(sys);		
			sysToNumSimultaneousTransformHash.put(sys, numSimultaneousTransform);
		}
	}
	
	public void calculateTotalCost()
	{
		for(String sys : sysToWorkVolHashPerSite.keySet())
		{
			costAllSysAllSites += sysToWorkVolHashPerSite.get(sys)/7*5*workHoursInDay * sysToSiteHash.get(sys).size() * costPerHour;//"still working on...."; // should be total work vol in hours * site * 150;
		}

	}
	
	public void printTest()
	{
		for(String sys : sysToMinTimeHashPerSite.keySet())
		{
			System.out.print(sys + "$" + sysToMinTimeHashPerSite.get(sys) / 365.0 + "$" + sysToWorkVolHashPerSite.get(sys) / 365.0);
			if(sysToSiteHash.containsKey(sys))
				System.out.print("$" + sysToSiteHash.get(sys).size());
			else
				System.out.print("$" + "not found");
			System.out.print("$" + sysToResourceAllocationHash.get(sys) + "$" + sysToNumSimultaneousTransformHash.get(sys));
			System.out.println();
		}
		
		System.out.println("resources used: "+resourcesPossible);
		System.out.println("min time with unlimited resources: "+minNecessaryTimeAllSystems / 365.0);
		System.out.println("min time with resources used: "+minPossibleTimeAllSystems / 365.0);		
		System.out.println();
	}
	
	public void makeArrayList()
	{
		
		//output is system, number of sites, resource allocation, number of sites at same time, total cost for system
		//old version included min time at one site and work volume at one site
		for(String sys : sortedSysList)
		{
			Object[] element = new Object[6];
			element[0] = sys;
			double time1 = sysToWorkVolHashAllSites.get(sys) / 365.0 / Math.ceil(sysToResourceAllocationHash.get(sys));
			element[1] = Math.max(time1, sysToMinTimeHashPerSite.get(sys) / 365.0);
	//		element[1] = sysToMinTimeHashPerSite.get(sys) / 365.0;
	//		element[2] = sysToWorkVolHashPerSite.get(sys) / 365.0;

			element[2] = sysToSiteHash.get(sys).size();
			element[3] = Math.ceil(sysToResourceAllocationHash.get(sys));
//			element[2] = sysToResourceAllocationHash.get(sys);

			element[4] = sysToNumSimultaneousTransformHash.get(sys);
			element[5] = sysToWorkVolHashPerSite.get(sys)/7*5*workHoursInDay * sysToSiteHash.get(sys).size() * costPerHour;//"still working on...."; // should be total work vol in hours * site * 150

			outputList.add(element);
		}
	}
	
	
	/**
	 * Converts an loe in hours to loe in days, assuming workHoursInDay and 5 day work weeks.
	 * @param loeInHours
	 * @return
	 */
	public double convertLoeHoursToDays(double loeInHours)
	{
		return loeInHours/workHoursInDay / 5 * 7 * percentOfPilot;
	}
		
	public ArrayList <Object []> createData(String engineName, String query) {
		
		ArrayList <Object []> list = new ArrayList<Object[]>();
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.setEngineType(IEngine.ENGINE_TYPE.SESAME);
		try{
			wrapper.executeQuery();	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}		

		// get the bindings from it
		String[] names = wrapper.getVariables();
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();

				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = getVariable(names[colIndex], sjss);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
		return list;
	}
	
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}
	
	private void processSystemDataLOE(ArrayList <Object []> list)
	{
		logger.info("Processing query:"+systemCostQuery);
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String data = (String) elementArray[1];
			String ser = (String) elementArray[2];
			Double loe = (Double) elementArray[3];
			if(sysToDataToLOEHash.containsKey(system))
			{
				Hashtable<String, Double> dataCostHash = sysToDataToLOEHash.get(system);
				dataCostHash.put(data+"$"+ser, loe);
			}
			else
			{
				Hashtable<String, Double> dataCostHash = new Hashtable<String, Double> ();
				dataCostHash.put(data+"$"+ser,  loe);
				sysToDataToLOEHash.put(system, dataCostHash);
			}
		}
	}
	private void processSystemSiteHashTables(ArrayList <Object []> list)
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String site = (String) elementArray[1];
			if(sysToSiteHash.containsKey(system))
			{
				ArrayList<String> siteList = sysToSiteHash.get(system);
				siteList.add(site);
			}
			else
			{
				ArrayList<String> siteList = new ArrayList<String>();
				siteList.add(site);
				sysToSiteHash.put(system, siteList);
			}
		}
	}
	private void sortSysList()
	{
		sortedSysList = new Vector<String>(sysToMinTimeHashPerSite.keySet());
		Collections.sort(sortedSysList);
	}
}
