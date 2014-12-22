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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

public class SysDecommissionOptimizationFunctions {
	

	protected static final Logger logger = LogManager.getLogger(SysDecommissionOptimizationFunctions.class.getName());
	
	//stores list of systems and then hashtable of systems to their min time and work volumes
	private Hashtable<String, Double> sysToMinTimeHashPerSite;
	private Hashtable<String, Double> sysToWorkVolHashPerSite;
	private Hashtable<String, Double> sysToWorkVolHashAllSites;
	
	private Hashtable<String, Double> sysToResourceAllocationFirstSiteHash;
	private Hashtable<String, Double> sysToResourceAllocationOtherSitesHash;
	private Hashtable<String, Double> sysToPossibleResourceAllocationHash;
	private Hashtable<String, Double> sysToNumSimultaneousTransformHash;
	
    //hashtable storing all the systems, their data objects, and their loes
    Hashtable<String, Hashtable<String,Double>> sysToDataToLOEHash;
    //hashtable storing all the systems and their sites.
    Hashtable<String, ArrayList<String>> sysToSiteHash;
    //hashtable storing all the systems and their probabilities
    Hashtable<String, String> sysToProbHash;
    private Hashtable<String, String> sysToOwnerHash;
//    public Hashtable<String, Double> sysToSustainmentCost;
    public Hashtable<String, Integer> sysToSiteCountHash;
    
    private ArrayList<String> systemsWithNoSite;
    private Vector<String> sortedSysList;
    public ArrayList<String> sysList;
    public ArrayList<String> dataList;
    boolean givenSysList = false;
    boolean givenDataList = false;

	ArrayList <Object []> outputList;

	private static String costDB = "TAP_Cost_Data";
	private static String systemCostQuery = "SELECT DISTINCT ?sys ?data ?ser (SUM(?loe) AS ?cost) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase>} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass}{?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start}  {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser }{?data <http://semoss.org/ontologies/Relation/Input> ?GLitem}} GROUP BY ?sys ?data ?ser";

	private static String siteDB = "TAP_Site_Data";
	private static String systemSiteQuery = "SELECT DISTINCT ?System ?DCSite WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;}  {?SystemDCSite ?DeployedAt ?DCSite;}{?System ?DeployedAt1 ?SystemDCSite;} }";
	
//	private static String systemSustainmentCostQuery = "SELECT DISTINCT ?System ?SustainmentBudget WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBudget}}";
	
	private static String coreDB = "HR_Core";
	private static String systemProbQuery = "SELECT DISTINCT ?System ?Prob WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}}";
	private static String systemArchiveQuery = "SELECT DISTINCT ?System ?Archive WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}OPTIONAL{?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> ?Archive}}";
	private static String systemOwnerQuery = "SELECT DISTINCT ?System (GROUP_CONCAT(?OwnerName ; SEPARATOR = ', ') AS ?Owner) WHERE { SELECT DISTINCT ?System (COALESCE(SUBSTR(STR(?Own),50),'') AS ?OwnerName) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{?System <http://semoss.org/ontologies/Relation/OwnedBy> ?Own} }}  GROUP BY ?System";
	
	private boolean includeFirstSite=true;
	private boolean includePilot = false;
	private boolean storeWorkVolInDays = true;
	private boolean includeArchive=false;
	private double percentOfPilot=0.2;
	private double archivePercent = 0.3;
	private double hourlyCost=150.0;
	private static final int workHoursInDay = 8;
	private static final int workHoursInYear = 40*52;

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
	public void setPilotBoolean(boolean includePilot)
	{
		this.includePilot = includePilot;
	}
	public void setFirstSiteBoolean(boolean includeFirstSite)
	{
		this.includeFirstSite = includeFirstSite;
	}
	public void setIncludeArchiveBoolean(boolean includeArchive)
	{
		this.includeArchive = includeArchive;
	}
	public void setstoreWorkVolInDays(boolean storeWorkVolInDays)
	{
		this.storeWorkVolInDays = storeWorkVolInDays;
	}
	public void setHourlyCost(double hourlyCost)
	{
		this.hourlyCost = hourlyCost;
	}
	public void setDataList(ArrayList<String> dataList)
	{
		this.dataList = dataList;
		this.givenDataList = true;
	}
	public void setSysList(ArrayList<String> sysList)
	{
		this.sysList = sysList;
		this.givenSysList = true;
	}
	public Hashtable<String, Double> getSysToWorkVolHashPerSite()
	{
		return sysToWorkVolHashPerSite;
	}
	public Hashtable<String,String> getSysToOwnerHash() {
		return sysToOwnerHash;
	}
	public void optimizeTime()
	{

		instantiate();
		calculateForAllSystems();
		
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
		calculateForAllSystems();
		
		minPossibleTimeAllSystems = Math.max(minNecessaryTimeAllSystems,timeConstraint);
		recalculateResourcesPossible();
		
		calculateResourceAndOutput();
		
		System.out.println("Time constraint:"+timeConstraint / 365.0);
		System.out.println("Time actually used in years:"+minPossibleTimeAllSystems / 365.0);
		System.out.println("Resources used: "+Math.ceil(resourcesPossible));
		System.out.println("Total cost: "+costAllSysAllSites);
	}
	public void optimize(double budget, double minYears)
	{
		instantiate();
		calculateForAllSystems();
		
		minPossibleTimeAllSystems = Math.max(minNecessaryTimeAllSystems,minYears);
	//	resourcesPossible = budget / (costPerHour
		
		//timeConstraint = minYears;
		
		//minNecessaryTimeAllSystems
		
	}
	/**Adjusts the minimum time it takes to transform the systems since there is some
	 * minimum time each system takes based upon its data objects and resources.
	 * 
	 * @param budget
	 * @param hourlyCost
	 * @param years
	 */
	public double adjustTimeToTransform(double budget, double years)
	{
		sysToPossibleResourceAllocationHash = new Hashtable<String, Double>();
		minPossibleTimeAllSystems = 0.0;
		//instantiate();
		resourcesPossible = budget / (workHoursInYear*hourlyCost);
		calculatePossibleResourceAllocationPerSystem();//sysToResourceAllocationHash
//		calculateMaxResourceAllocationPerSystem();
		calculateResourceAllocationPerSystem();
//		calculateTimeToTransformPerSystem(years);
		calculateMinTimeToTransform();
		System.out.println("Years: "+years + " and Adjusted Years"+minPossibleTimeAllSystems / 365.0);
		return Math.max(minPossibleTimeAllSystems / 365.0,years);
		
	}
	public void instantiate()
	{
		sysToMinTimeHashPerSite = new Hashtable<String, Double>();
		sysToWorkVolHashPerSite = new Hashtable<String, Double>();
//		sysToMinTimeHashAllSites = new Hashtable<String, Double>();
		sysToWorkVolHashAllSites = new Hashtable<String, Double>();
		sysToDataToLOEHash = new Hashtable<String, Hashtable<String,Double>>();
		sysToSiteHash = new Hashtable<String, ArrayList<String>>();
		sysToProbHash = new Hashtable<String, String>();
		sysToOwnerHash = new Hashtable<String, String>();
//		sysToSustainmentCost = new Hashtable<String,Double>();
		sysToSiteCountHash = new Hashtable<String, Integer>();
		sysToPossibleResourceAllocationHash = new Hashtable<String, Double>();
		sysToResourceAllocationFirstSiteHash = new Hashtable<String, Double>();
		sysToResourceAllocationOtherSitesHash = new Hashtable<String, Double>();
		sysToNumSimultaneousTransformHash = new Hashtable<String, Double>();
		outputList = new ArrayList<Object[]>();
		systemsWithNoSite = new ArrayList<String>();
		sortedSysList = new Vector<String>();
		
		resourcesPossible = 0.0;
		minNecessaryTimeAllSystems = 0.0;
		minPossibleTimeAllSystems = 0.0;
		workVolAllSysAllSites = 0.0;
		costAllSysAllSites = 0.0;
		
		calculateMinTimeAndWorkVolPerSystemPerSite();
		calculateMinTimeAndWorkVolPerSystemAllSites();
//		removeSystemsWithNoSite();
		sortSysList();
		
		ArrayList <Object []> systemProbList = createData(coreDB,systemProbQuery);
		processSystemHash(systemProbList,sysToProbHash);
//		ArrayList <Object []> systemSustainmentCostList = createData(coreDB,systemSustainmentCostQuery);
//		processSystemSustainmentCostHash(systemSustainmentCostList);
		if(includeArchive) {
			ArrayList <Object []> systemArchiveList = createData(coreDB,systemArchiveQuery);
			updateWorkVolWithArchive(systemArchiveList);
		}
		ArrayList <Object []> systemOwnerList = createData(coreDB,systemOwnerQuery);
		processSystemHash(systemOwnerList,sysToOwnerHash);
	}
	
	
	public void calculateForAllSystems()
	{

		calculateMinTimeAllSystemsAllSites();
		calculateWorkVolAllSystemsAllSites();
	}
	
	public void calculateResourceAndOutput()
	{
		calculatePossibleResourceAllocationPerSystem();
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
				double loeForData = 0.0;
				if(storeWorkVolInDays)
					loeForData = convertLoeHoursToDays(dataAndLOEForSys.get(data));
				else
					loeForData = convertLoeHoursToCost(dataAndLOEForSys.get(data));
				if(!includePilot)
				{
					loeForData = loeForData * percentOfPilot;
				}
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
		for(String sys : sysToSiteHash.keySet())
		{
			sysToSiteCountHash.put(sys, sysToSiteHash.get(sys).size());
		}
		
		for(String sys : sysToMinTimeHashPerSite.keySet())
		{
			//only using systems that are in the decomission list.
			//if we dont have site data, we are assuming the system is only deployed at one site.
			int numOfSites = 1;
			if(sysToSiteHash.containsKey(sys))
			{
				numOfSites = sysToSiteHash.get(sys).size();
			}
			else
			{
				systemsWithNoSite.add(sys);
			}
	//		double minNecessaryTimePerSysPerSite = sysToMinTimeHashPerSite.get(sys);
	//		double minNecessaryTimePerSysAllSites = numOfSites*minNecessaryTimePerSysPerSite;
			
			double workVolPerSysPerSite = sysToWorkVolHashPerSite.get(sys);
			double workVolPerSysAllSites = numOfSites*workVolPerSysPerSite;
			if(!includeFirstSite&&numOfSites>1)
				workVolPerSysAllSites = (numOfSites-1)*workVolPerSysPerSite*percentOfPilot;
			else if(!includeFirstSite)
				workVolPerSysAllSites = 0;
			else if(includePilot&&includeFirstSite&&numOfSites>1)
				workVolPerSysAllSites = workVolPerSysPerSite+(numOfSites-1)*workVolPerSysPerSite*percentOfPilot;
	//		sysToMinTimeHashAllSites.put(sys,minNecessaryTimePerSysAllSites);
			sysToWorkVolHashAllSites.put(sys,workVolPerSysAllSites);
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
//		for(String sys : sysToMinTimeHashAllSites.keySet())
		for(String sys : sysToMinTimeHashPerSite.keySet())
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
//		for(String sys : sysToMinTimeHashAllSites.keySet())
		for(String sys : sysToMinTimeHashPerSite.keySet())
		{
			double workVolPerSysAllSites = sysToWorkVolHashAllSites.get(sys);
			resourcesPossible+=workVolPerSysAllSites;
		}
		resourcesPossible = resourcesPossible / minPossibleTimeAllSystems;
	}
	
	public void calculatePossibleResourceAllocationPerSystem()
	{
		for(String sys : sysToWorkVolHashAllSites.keySet())
		{
			double possibleResourceForSysAllSites = resourcesPossible*sysToWorkVolHashAllSites.get(sys) / workVolAllSysAllSites;
			sysToPossibleResourceAllocationHash.put(sys, possibleResourceForSysAllSites);
		}
		
	}

	public void calculateResourceAllocationPerSystem()
	{
//		Hashtable<String,Double> sysToMaxResourceAllocationForFirstSiteHash = new Hashtable<String,Double>();
//		Hashtable<String,Double> sysToMaxResourceAllocationForOtherSitesHash = new Hashtable<String,Double>();
		for(String sys : sysToPossibleResourceAllocationHash.keySet())
		{
			double pilotWorkVol = sysToWorkVolHashPerSite.get(sys);
			double remainingWorkVol = sysToWorkVolHashAllSites.get(sys) - sysToWorkVolHashPerSite.get(sys);
			
			double maxResourceForSysFirstSite = pilotWorkVol / sysToMinTimeHashPerSite.get(sys);
			double maxResourceForSysOtherSites = remainingWorkVol / (sysToMinTimeHashPerSite.get(sys)*percentOfPilot);
			
			double possibleResourceForSysAllSites = sysToPossibleResourceAllocationHash.get(sys);
			
			sysToResourceAllocationFirstSiteHash.put(sys, Math.min(possibleResourceForSysAllSites,maxResourceForSysFirstSite));
			sysToResourceAllocationOtherSitesHash.put(sys, Math.min(possibleResourceForSysAllSites,maxResourceForSysOtherSites));
			
		}
	}
	public void calculateMinTimeToTransform()
	{
		for(String sys : sysToResourceAllocationFirstSiteHash.keySet())
		{
			double pilotWorkVol = sysToWorkVolHashPerSite.get(sys);
			double remainingWorkVol = sysToWorkVolHashAllSites.get(sys) - sysToWorkVolHashPerSite.get(sys);

			double timeForSys = pilotWorkVol / sysToResourceAllocationFirstSiteHash.get(sys);
			timeForSys+= remainingWorkVol / sysToResourceAllocationOtherSitesHash.get(sys);
		
			if(minPossibleTimeAllSystems<timeForSys)
				minPossibleTimeAllSystems = timeForSys;
		}
		
	}
	
	public void calculateNumSysSimultaneousTransform()
	{
		for(String sys : sysToPossibleResourceAllocationHash.keySet())
		{
			double numSimultaneousTransform = Math.ceil(sysToPossibleResourceAllocationHash.get(sys))*sysToMinTimeHashPerSite.get(sys)/sysToWorkVolHashPerSite.get(sys);		
			sysToNumSimultaneousTransformHash.put(sys, numSimultaneousTransform);
		}
	}
	
	public void calculateTotalCost()
	{
		for(String sys : sysToWorkVolHashPerSite.keySet())
		{
			int numSites= 1;
			if(sysToSiteHash.containsKey(sys))
				numSites = sysToSiteHash.get(sys).size();
			costAllSysAllSites += sysToWorkVolHashPerSite.get(sys)/7*5*workHoursInDay * numSites * hourlyCost;
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
			System.out.print("$" + sysToPossibleResourceAllocationHash.get(sys) + "$" + sysToNumSimultaneousTransformHash.get(sys));
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
			Object[] element = new Object[9];
			element[0] = sys;
			element[1] = sysToProbHash.get(sys);
			element[2] = sysToMinTimeHashPerSite.get(sys) / 365.0;
			
			//add a minimum time for system
			double time1 = sysToWorkVolHashAllSites.get(sys) / 365.0 / Math.ceil(sysToPossibleResourceAllocationHash.get(sys));
			element[3] = Math.max(time1, sysToMinTimeHashPerSite.get(sys) / 365.0);
			element[4] = sysToWorkVolHashPerSite.get(sys) / 365.0;
	//		element[1] = sysToMinTimeHashPerSite.get(sys) / 365.0;
	//		element[2] = sysToWorkVolHashPerSite.get(sys) / 365.0;
			int numSites= 1;
			if(sysToSiteHash.containsKey(sys))
				numSites = sysToSiteHash.get(sys).size();
			element[5] = numSites;
			element[6] = Math.ceil(sysToPossibleResourceAllocationHash.get(sys));
//			element[2] = sysToResourceAllocationHash.get(sys);

			element[7] = Math.ceil(sysToNumSimultaneousTransformHash.get(sys));
			element[8] = sysToWorkVolHashPerSite.get(sys)/7*5*workHoursInDay * numSites * hourlyCost;// should be total work vol in hours * site * 150

			outputList.add(element);
		}
		
		for(String sys : sysList)
		{
			//adding in any systems that we were missing information for
			if(!sortedSysList.contains(sys))
			{
				Object[] element = new Object[9];
				element[0] = sys;
				element[1] = sysToProbHash.get(sys);
				element[2] = -1;
				element[3] = -1;
				element[4] = -1;
				int numSites= 1;
				if(sysToSiteHash.containsKey(sys))
					numSites = sysToSiteHash.get(sys).size();
				element[5] = numSites;
				element[6] = -1;
				element[7] = -1;
				element[8] = -1;
				outputList.add(element);
			}
				
		}
	}
	
	
	/**
	 * Converts an loe in hours to loe in days, assuming workHoursInDay and 5 day work weeks.
	 * @param loeInHours
	 * @return
	 */
	public double convertLoeHoursToDays(double loeInHours)
	{
		return loeInHours/workHoursInDay / 5 * 7;
	}
	
	public double convertLoeHoursToCost(double loeInHours)
	{
		return loeInHours*hourlyCost;
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
		catch (RuntimeException e)
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
		} catch (RuntimeException e) {
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
			//if given a sysList, make sure that the system is in that list
			if(!givenSysList||(givenSysList&&sysList.contains(system)))
			{
				if(!givenDataList||(givenDataList&&dataList.contains(data)))
				{
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
		}
	}
	private void processSystemHash(ArrayList <Object []> list,Hashtable<String,String> sysHash)
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String prob = (String) elementArray[1];
			sysHash.put(system,prob);
		}
	}
	private void updateWorkVolWithArchive(ArrayList <Object []> list)
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String archive = (String) elementArray[1];
			if(archive.toUpperCase().contains("Y")&&sysToWorkVolHashPerSite.containsKey(system)) {
				double workVol = sysToWorkVolHashPerSite.get(system);
				sysToWorkVolHashPerSite.put(system,workVol*archivePercent);
			}
		}
	}
//	private void processSystemSustainmentCostHash(ArrayList <Object []> list)
//	{
//		for (int i=0; i<list.size(); i++)
//		{
//			Object[] elementArray= list.get(i);
//			String system = (String) elementArray[0];
//			if(elementArray[1] instanceof Double)
//			{
//				Double cost = (Double) elementArray[1];
//				sysToSustainmentCost.put(system,cost);
//			}
//		}
//	}
	private void processSystemSiteHashTables(ArrayList <Object []> list)
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String site = (String) elementArray[1];
			if(!givenSysList||(givenSysList&&sysList.contains(system)))
			{
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
	}
	private void sortSysList()
	{
		sortedSysList = new Vector<String>(sysToMinTimeHashPerSite.keySet());
		Collections.sort(sortedSysList);
	}
}
