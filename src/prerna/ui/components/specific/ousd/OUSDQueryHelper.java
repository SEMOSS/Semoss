package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class OUSDQueryHelper {

	protected static final Logger LOGGER = LogManager.getLogger(OUSDQueryHelper.class.getName());
	
	/**
	 * @param dbName
	 * @param owners
	 * @return
	 */
	public static List<String> getSystemsByOwners(IEngine db, List<String> owners){
		
		List<String> addedOwners = new ArrayList<String>();
		String ownerBindings = "";
		String systemQuery = db.getProperty(OUSDConstants.SYSTEMS_BY_OWNERS_QUERY);
		
		for(String owner: owners){
			if(!addedOwners.contains(owner)){
				ownerBindings = ownerBindings + "(<http://semoss.org/ontologies/Concept/SystemOwner/" + owner + ">)";
				addedOwners.add(owner);
			}
		}

		systemQuery = systemQuery.replaceAll("!OWNERS!", ownerBindings);
		
		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + systemQuery);
		List<String> systems =  new ArrayList<String>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(db, systemQuery);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			systems.add((String) iss.getVar(wNames[0]));
		}
		LOGGER.info("RETRIEVED SYSTEM DATA::::: " + systems.toString());

		return systems;
	}
	
	/**
	 * @param costDbName
	 * @param budgetQuery
	 * @return
	 */
	private static Map<String, Double> getBudgetData(String costDbName, String budgetQuery){
		IEngine costDb = null;

		LOGGER.info("Cost db " + costDbName);

		//costDbName = OUSDPlaysheetHelper.getCostDatabase(costDbName);
		String id = MasterDatabaseUtility.testEngineIdIfAlias(costDbName + "_V4b");
		costDb = Utility.getEngine(id);

		boolean costDbExists = costDb!=null;
		LOGGER.info("Cost db " + costDbName + " exists ::::  " + costDbExists);
		if(!costDbExists){
			return null;
		}

		LOGGER.info("RUNNING COST QUERY::::::: db is " + costDbName + " and query is " + budgetQuery);
		Map<String, Double> sysBudgets =  new HashMap<String, Double>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(costDb, budgetQuery);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			sysBudgets.put((String) iss.getVar(wNames[0]), (Double) iss.getVar(wNames[1]));
		}
		LOGGER.info("Got cost data :: " + sysBudgets.toString());

		return sysBudgets;
	}
	
	/**
	 * @param sysList
	 * @return
	 */
	public static Map<String, Double> getBudgetData(IEngine engine, String[] sysList){
		String costEngineName = engine.getProperty(OUSDConstants.COST_ENGINE_NAME);
		String budgetQuery = engine.getProperty(OUSDConstants.SYSTEM_SUSTAINMENT_BUDGET_QUERY);
		LOGGER.info("COST DB NAME ::::::"+costEngineName);

		//retrieve the budget numbers from the helper class
		Map<String, Double> budgetMap = OUSDQueryHelper.getBudgetData(costEngineName, budgetQuery);
		Map<String, Double> updatedBudgetMap = new HashMap<String, Double>();

		if(sysList != null){
			for(String system: sysList){
				if(budgetMap.containsKey(system)){
					updatedBudgetMap.put(system, budgetMap.get(system));
				}else{
					System.err.println("MISSING BUDGET FOR SYSTEM :::::::: " + system + " . SETTING TO 1");
					updatedBudgetMap.put(system, new Double(1));
				}
			}
		}
		else{
			return budgetMap;
		}
		return updatedBudgetMap;
	}
	
	public static List<String> getEnduringSystems(IEngine engine){
		LOGGER.info("Cost db " + engine);

		String enduringQuery = engine.getProperty(OUSDConstants.ENDURING_SYSTEM_QUERY);
		LOGGER.info("RUNNING ENDURING SYSTEMS QUERY::::::: db is " + engine.getEngineName() + " and query is " + enduringQuery);
		List<String> sysBudgets =  new ArrayList<String>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(engine, enduringQuery);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			sysBudgets.add((String) iss.getVar(wNames[0]));
		}
		LOGGER.info("Got enduring data :: " + sysBudgets.toString());
		return sysBudgets;
	}
	
	/**
	 * @param dbName
	 * @param systemToSystemDataQuery
	 * @return
	 */
	public static Map<String, List<List<String>>> getSystemToSystemData(IEngine db){

		String systemToSystemDataQuery = db.getProperty(OUSDConstants.SYSTEM_TO_SYSTEM_DATA_QUERY);

		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + systemToSystemDataQuery);
		
		Map<String, List<List<String>>> systemsToSystemData =  new HashMap<String, List<List<String>>>();
		
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(db, systemToSystemDataQuery);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			if(systemsToSystemData.keySet().contains(iss.getVar(wNames[0]).toString())){
				List<String> systemData = new ArrayList<String>();
				systemData.add(iss.getVar(wNames[1]).toString());
				systemData.add(iss.getVar(wNames[2]).toString());
				systemsToSystemData.get(iss.getVar(wNames[0]).toString()).add(systemData);
			}else{
				List<List<String>> systemDataList = new ArrayList<List<String>>();
				List<String> systemData = new ArrayList<String>();
				systemData.add(iss.getVar(wNames[1]).toString());
				systemData.add(iss.getVar(wNames[2]).toString());
				systemDataList.add(systemData);
				systemsToSystemData.put(iss.getVar(wNames[0]).toString(), systemDataList);
			}
		}
		LOGGER.info("RETRIEVED SYSTEM DATA::::: " + systemsToSystemData.toString());

		return systemsToSystemData;
	}
	
	/**
	 * @param dbName
	 * @param systemToSystemDataQuery
	 * @return
	 */
	public static Map<String, List<String>> getSystemToSystemDataWithSystemBind(IEngine db, String systemBindingString){

		String systemToSystemDataQuery = db.getProperty(OUSDConstants.SYSTEM_TO_SYSTEM_DATA_QUERY_WTIH_SYSTEM_BIND);
		
		systemToSystemDataQuery = systemToSystemDataQuery.replaceAll("!SYSTEMS!", systemBindingString);

		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + systemToSystemDataQuery);

		return getSingleLevelMap(db, systemToSystemDataQuery);
	}
	
	/**
	 * @param dbName
	 * @param bluSystemQuery
	 * @param systemBindingsString
	 * @return
	 */
	public static Map<String, List<String>> getBLUtoSystem(IEngine db, String systemBindingsString){
		String bluSystemQuery = db.getProperty(OUSDConstants.BLU_SYSTEM_QUERY);
		bluSystemQuery = bluSystemQuery.replace("!SYSTEMS!", systemBindingsString);
		System.out.println(systemBindingsString);

		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + bluSystemQuery);
		
		return getSingleLevelMap(db, bluSystemQuery);
	}


	/**
	 * @param db
	 * @param systemBindingsString
	 * @return
	 */
	public static Map<String, List<String>> getSystemToTarget(IEngine db, String systemBindingsString){
		String systemTargetQuery = db.getProperty(OUSDConstants.SYSTEM_TO_TARGET_QUERY);
		systemTargetQuery = systemTargetQuery.replace("!SYSTEMS!", systemBindingsString);
		System.out.println(systemBindingsString);

		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + systemTargetQuery);
		
		return getSingleLevelMap(db, systemTargetQuery);
	}
	
	/**
	 * @param dbName
	 * @param dataBLUQuery
	 * @param bluBindingsString
	 * @return
	 */
	public static List<Object[]> getDataConsumedByBLU(IEngine db, String bluBindingsString){
		String dataBLUQuery = db.getProperty(OUSDConstants.DATA_CONSUMED_BY_BLU_QUERY);
		dataBLUQuery = dataBLUQuery.replace("!BLU!", bluBindingsString);
		System.out.println(bluBindingsString);
		
		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + dataBLUQuery);
		
		List<Object[]> bluData =  new ArrayList<Object[]>();
		
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(db, dataBLUQuery);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			Object[] row = new Object[2];
			row[0] = iss.getVar(wNames[0]).toString();
			row[1] = iss.getVar(wNames[1]).toString();
			bluData.add(row);
		}
		LOGGER.info("RETRIEVED SYSTEM DATA::::: " + bluData.toString());
		
		return bluData;
	}
	
	public static Map<String, List<String>> getDataCreatedBySystem(IEngine db, String systemBindingsString){
		String dataSystemQuery = db.getProperty(OUSDConstants.DATA_CREATED_BY_SYSTEM_QUERY);
		dataSystemQuery = dataSystemQuery.replace("!SYSTEMS!", systemBindingsString);
		System.out.println(systemBindingsString);

		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + dataSystemQuery);
		
		return getSingleLevelMap(db, dataSystemQuery);
	}
	
	public static Map<String, List<String>> getSystemsByRetirementType(IEngine db, String systemBindingsString){
		String retirementTypeQuery = db.getProperty(OUSDConstants.RETIREMENT_TYPE_QUERY);
		retirementTypeQuery = retirementTypeQuery.replace("!SYSTEMS!", systemBindingsString);
		System.out.println(systemBindingsString);

		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + retirementTypeQuery);
		
		return getSingleLevelMap(db, retirementTypeQuery);
	}
	
	public static Map<String, Map<String, List<String>>> getActivityBluSystemMap(IEngine db, String systemBindingsString){

		String activityBluSystemQuery = db.getProperty(OUSDConstants.ACTIVITY_BLU_SYSTEM_QUERY);
		
		activityBluSystemQuery = activityBluSystemQuery.replace("!SYSTEMS!", systemBindingsString);
		System.out.println(systemBindingsString);
		
		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + activityBluSystemQuery);
		
		return getDualLevelMap(db, activityBluSystemQuery);
	}
	
	public static Map<String, Map<String, List<String>>> getActivityGranularBluSystemMap(IEngine db, String systemBindingsString){

		String activityBluSystemQuery = db.getProperty(OUSDConstants.ACTIVITY_GRANULAR_BLU);
		
		activityBluSystemQuery = activityBluSystemQuery.replace("!SYSTEMS!", systemBindingsString);
		System.out.println(systemBindingsString);
		
		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + activityBluSystemQuery);
		
		return getDualLevelMap(db, activityBluSystemQuery);
	}
	
	public static Map<String, Map<String, List<String>>> getActivityDataSystemMap(IEngine db, String systemBindingsString){

		String activityDataSystemQuery = db.getProperty(OUSDConstants.ACTIVITY_DATA_SYSTEM_QUERY);
		
		activityDataSystemQuery = activityDataSystemQuery.replace("!SYSTEMS!", systemBindingsString);
		System.out.println(systemBindingsString);
		
		LOGGER.info("QUERYING FOR ALL SYSTEMS::::::: db is " + db.getEngineName() + " and query is " + activityDataSystemQuery);
		
		return getDualLevelMap(db, activityDataSystemQuery);
	}
	
	private static Map<String, List<String>> getSingleLevelMap(IEngine db, String query){
		Map<String, List<String>> retirementTypeToSystemMap =  new HashMap<String, List<String>>();
		
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(db, query);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			if(retirementTypeToSystemMap.keySet().contains(iss.getVar(wNames[1]).toString())){
				retirementTypeToSystemMap.get(iss.getVar(wNames[1]).toString()).add(iss.getVar(wNames[0]).toString());
			}else{
				List<String> systemData = new ArrayList<String>();
				systemData.add(iss.getVar(wNames[0]).toString());
				retirementTypeToSystemMap.put(iss.getVar(wNames[1]).toString(), systemData);
			}
		}
		LOGGER.info("RETRIEVED SYSTEM DATA::::: " + retirementTypeToSystemMap.toString());
		
		return retirementTypeToSystemMap;
	}
	
	private static Map<String, Map<String, List<String>>> getDualLevelMap(IEngine db, String query){
		Map<String, Map<String, List<String>>> activityBluSystem =  new HashMap<String, Map<String, List<String>>>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(db, query);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();
			String blu = "";
			//check to see if we need to combine blu and data
			if(wNames.length==4){
				blu = iss.getVar(wNames[1]).toString() + "||" + iss.getVar(wNames[2]).toString();
			}else{
				blu = iss.getVar(wNames[1]).toString();
				//contains activity?
			}
			if(activityBluSystem.keySet().contains(iss.getVar(wNames[0]).toString())){

				//contains BLU?
				if(activityBluSystem.get(iss.getVar(wNames[0]).toString()).keySet().contains(blu)){
					//blu map contains system?
					if(activityBluSystem.get(iss.getVar(wNames[0]).toString()).get(blu).contains(iss.getVar(wNames[wNames.length-1]).toString())){
						//do nothing if contains system
						continue;
					}else{
						//add system if not contained in blu map
						activityBluSystem.get(iss.getVar(wNames[0]).toString()).get(blu).add(iss.getVar(wNames[wNames.length-1]).toString());
					}
				//put BLU new list
				}else{
					List<String> newList = new ArrayList<String>();
					newList.add(iss.getVar(wNames[wNames.length-1]).toString());
					activityBluSystem.get(iss.getVar(wNames[0]).toString()).put(blu, newList);
				}
			//add activity with new map
			}else{
				Map<String, List<String>> newBluMap = new HashMap<String, List<String>>();
				List<String> newList = new ArrayList<String>();
				newList.add(iss.getVar(wNames[wNames.length-1]).toString());
				newBluMap.put(blu, newList);
				activityBluSystem.put(iss.getVar(wNames[0]).toString(), newBluMap);
			}
		}
	
		return activityBluSystem;
	}
	
}
