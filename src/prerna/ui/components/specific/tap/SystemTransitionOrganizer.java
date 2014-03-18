package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

public class SystemTransitionOrganizer {
	protected Logger logger = Logger.getLogger(getClass());
	private static String healthMilDataURI = "http://health.mil/ontologies/Concept/DataObject";
	private static String siteDB = "TAP_Site_Data";
	private static String costDB = "TAP_Cost_Data";
	private static String coreDB = "TAP_Core_Data";
	private static String hrDB = "HR_Core";
	private static String systemSiteQuery = "SELECT DISTINCT ?System ?DCSite WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;}  {?SystemDCSite ?DeployedAt ?DCSite;}{?System ?DeployedAt1 ?SystemDCSite;} }";
	private static String systemCostQuery = "SELECT DISTINCT ?sys ?data ?ser (SUM(?loe) AS ?cost) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase>} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass}{?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start}  {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser }{?data <http://semoss.org/ontologies/Relation/Input> ?GLitem}} GROUP BY ?sys ?data ?ser BINDINGS ?data {@DATABINDINGS@}";
	private static String systemDataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .} }BINDINGS ?Data {@DATABINDINGS@}";
	private static String capDataQuery = "SELECT DISTINCT ?Capability ?Data ?Crm WHERE {{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} {?Capability <http://semoss.org/ontologies/Relation/Contains/Source> \"HSD\"}}";
    private static String siteLocationQuery = "SELECT DISTINCT ?dcSite ?lat ?long WHERE { {?dcSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?dcSite <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat }  {?dcSite <http://semoss.org/ontologies/Relation/Contains/LONG> ?long } }";
    
    //hashtable storing all the sites and the systems they have
    //and the reverse, hashtable storing all the systems and their sites
    Hashtable<String, ArrayList<String>> siteToSysHash = new Hashtable<String, ArrayList<String>>();
    Hashtable<String, ArrayList<String>> sysToSiteHash = new Hashtable<String, ArrayList<String>>();
    
    //hashtable storing all the systems, their data objects, and their costs
    Hashtable<String, Hashtable<String,Double>> sysToDataToCostHash = new Hashtable<String, Hashtable<String,Double>>();
    
    //hashtable storing all the systems, their data objects they read, and whether they claimed to be C or R
    Hashtable<String, Hashtable<String,String>> sysReadDataHash = new Hashtable<String, Hashtable<String,String>>();
	
    //hashtable storing lat and long for each site
    Hashtable<String, Hashtable<String, Double>> siteLatLongHash = new Hashtable<String, Hashtable<String, Double>>();
	
	public SystemTransitionOrganizer(ArrayList<String> dataBindings)
	{
		addBindingToQueries(dataBindings);
		createAllData();
	}
	
	private void addBindingToQueries(ArrayList<String> dataBindings)
	{
		String bindingString ="";
		for (String data : dataBindings)
		{
			bindingString = bindingString+"(<" + healthMilDataURI + "/"+data + ">)";
		}
		systemCostQuery=systemCostQuery.replace("@DATABINDINGS@", bindingString);
		systemDataQuery=systemDataQuery.replace("@DATABINDINGS@", bindingString);
	}
	
	private void createAllData()
	{
		ArrayList <Object []> list = createData(siteDB, systemSiteQuery);
		processSystemSiteHashTables(list);
		list = createData(costDB, systemCostQuery);
		processSystemDataLOE(list);
		list = createData(coreDB, systemDataQuery);
		processSystemReadData(list);
		list = createData(siteDB, siteLocationQuery);
		processSiteLocationData(list);
	}
	
	private void processSiteLocationData(ArrayList<Object[]> list) 
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray = list.get(i);
			String site = (String) elementArray[0];
			Double lat = (Double) elementArray[1];
			Double lon = (Double) elementArray[2];
			Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
			innerHash.put("LAT", lat);
			innerHash.put("LONG", lon);
			siteLatLongHash.put(site, innerHash);
		}
	}

	private void processSystemSiteHashTables(ArrayList <Object []> list)
	{
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String site = (String) elementArray[1];
			if(siteToSysHash.containsKey(site))
			{
				ArrayList<String> sysList = siteToSysHash.get(site);
				sysList.add(system);
			}
			else
			{
				ArrayList<String> sysList = new ArrayList<String>();
				sysList.add(system);
				siteToSysHash.put(site, sysList);
			}
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
	
	private void processSystemDataLOE(ArrayList <Object []> list)
	{
		System.out.println(systemCostQuery);
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String data = (String) elementArray[1];
			String ser = (String) elementArray[2];
			Double loe = (Double) elementArray[3];
			if(sysToDataToCostHash.containsKey(system))
			{
				Hashtable<String, Double> dataCostHash = sysToDataToCostHash.get(system);
				dataCostHash.put(data+"$"+ser, loe);
			}
			else
			{
				Hashtable<String, Double> dataCostHash = new Hashtable<String, Double> ();
				dataCostHash.put(data+"$"+ser,  loe);
				sysToDataToCostHash.put(system, dataCostHash);
			}
		}
	}
	
	private void processSystemReadData(ArrayList <Object []> list)
	{
		System.out.println(systemDataQuery);
		for (int i=0; i<list.size(); i++)
		{
			Object[] elementArray= list.get(i);
			String system = (String) elementArray[0];
			String data = (String) elementArray[1];
			String crm = (String) elementArray[2];
			if(sysReadDataHash.containsKey(system))
			{
				Hashtable<String, String> dataReadHash = sysReadDataHash.get(system);
				if(sysToDataToCostHash.get(system)!=null&&sysToDataToCostHash.get(system).containsKey(data))
				{
					dataReadHash.put(data, crm);
				}
			}
			else
			{
				Hashtable<String, String> dataReadHash = new Hashtable<String, String>();
				if(sysToDataToCostHash.get(system)!=null&&sysToDataToCostHash.get(system).containsKey(data))
				{
					dataReadHash.put(data, crm);
					sysReadDataHash.put(system,dataReadHash);
				}
			}
		}
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
	
	public Hashtable<String, ArrayList<String>> getSiteToSysHash()
	{
		return siteToSysHash;
	}
	
	public Hashtable<String, ArrayList<String>> getSysToSiteHash()
	{
		return sysToSiteHash;
	}
	
	public Hashtable<String, Hashtable<String,Double>> getSysDataLOEHash()
	{
		return sysToDataToCostHash;
	}
	
	public Hashtable<String, Hashtable<String,String>> getSysReadDataHash()
	{
		return sysReadDataHash;
	}
	
	public Hashtable<String, Hashtable<String, Double>> getSiteLatLongHash()
	{
		return siteLatLongHash;
	}

}
