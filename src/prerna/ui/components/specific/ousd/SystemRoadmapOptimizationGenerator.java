//package prerna.ui.components.specific.ousd;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import lpsolve.LpSolveException;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.engine.api.IEngine;
//import prerna.engine.api.ISelectStatement;
//
//public class SystemRoadmapOptimizationGenerator implements ITimelineGenerator{
//
//	protected static final Logger LOGGER = LogManager.getLogger(BLUSystemOptimizationPlaySheet.class.getName());
//
//	boolean limit = true;
//	
//	OUSDTimeline timeline = new OUSDTimeline();
//	String sysOwner;
//	IEngine roadmapEngine;
//	
//	//optimization values
//	int year = 1;
//	int totalYears = 5;
//	double interfaceCost = 350000.0;
//	double interfaceSustainmentPercent = 0.18;
//	List<List<String>> previousValues = new ArrayList<List<String>>();
//	Map<String, Double> systemCostPenalty = new HashMap<String, Double>();
//
//	//lists for tracking systems
//	String[] sysList;
//	List<String> decommissionedSystems = new ArrayList<String>();
//
//	//maps to get values
//	Map<String, Double> sysBudget;
//	Map<String, List<String>> bluMap = new HashMap<String, List<String>>(); //blu -> list of systems that support this blu.
//	Map<String, List<String>> dataSystemMap = new HashMap<String, List<String>>(); //data object -> list of systems that support this data object
//	Map<String, List<String>> granularBLUMap = new HashMap<String, List<String>>();
//	Map<String, List<String>> retirementMap = new HashMap<String, List<String>>();
//	Map<String, List<String>> interfaceCountMap = new HashMap<String, List<String>>(); //system -> list of systems where an interface exists between the two systems
//	Map<String, List<List<String>>> sdsMap; //systems -> list of data obj that are provided
//
//	List<Map<String, Double>> investmentMap = new ArrayList<Map<String, Double>>();
//	List<Map<String, Double>> sustainmentMap = new ArrayList<Map<String, Double>>();
//	
//	//for creating a table. will be removed once we have the new class to take the timeline into the roadmap
//	ArrayList<Object[]> outputList = new ArrayList<Object[]>();
//
//	@Override
//	public void createTimeline(IEngine engine, String owner){
//		roadmapEngine = engine;
//		sysOwner = owner;
//		runOptimization(limit);
//	}
//	
//	@Override
//	public OUSDTimeline getTimeline(){
//		return timeline;
//	}
//
//	public void runOptimization(boolean limit){
//		List<String> owners = new ArrayList<String>();
//		owners.add(sysOwner);
//		List<String> osystems = OUSDQueryHelper.getSystemsByOwners(roadmapEngine, owners);
//
//		Object[] systemReturn = OUSDPlaysheetHelper.createSysLists(osystems);
//		String[] sysList = (String[]) systemReturn[0];
//		String sysBindingsString = (String) systemReturn[1];
//		this.sysList = sysList;
//		int totalSystemCount = osystems.size();
//
//		sysBudget = OUSDQueryHelper.getBudgetData(roadmapEngine, sysList);
//
//		interfaceCountMap = OUSDQueryHelper.getSystemToSystemDataWithSystemBind(roadmapEngine, sysBindingsString);
//
//		dataSystemMap = OUSDQueryHelper.getDataCreatedBySystem(roadmapEngine, sysBindingsString);
//		retirementMap = OUSDQueryHelper.getSystemsByRetirementType(roadmapEngine, sysBindingsString);	
//		sdsMap = OUSDQueryHelper.getSystemToSystemData(roadmapEngine);
//
//		bluMap = OUSDQueryHelper.getBLUtoSystem(roadmapEngine, sysBindingsString);
//		String bluBindings = OUSDPlaysheetHelper.bluBindingStringMaker(bluMap);		
//		List<Object[]> bluDataList = OUSDQueryHelper.getDataConsumedByBLU(roadmapEngine, bluBindings);
//		granularBLUMap = OUSDPlaysheetHelper.systemToGranularBLU(bluDataList, dataSystemMap, bluMap);
//
//		timeline.setRetirementMap(retirementMap);
//		timeline.setDataSystemMap(dataSystemMap);
//		timeline.setBudgetMap(sysBudget);
//		timeline.setGranularBLUMap(granularBLUMap);
//		timeline.setSystemDownstream(sdsMap);
//
//		for(String system: sysList){
//			systemCostPenalty.put(system, new Double(0.0));
//		}
//		
//		while(year<=totalYears){
//
//			timeline.insertFy(year);
//
//			System.out.println("RUNNING FOR YEAR "+year);
//
//			if(year>1){
//				sysList = this.sysList;
//			}
//
//			List<String> keptSystems = new ArrayList<String>();
//			List<String> decomList = new ArrayList<String>();
//			boolean replacementThresholdReached = true;
//
//			while(replacementThresholdReached){
//
//				SystemRoadmapOptimizer opt = new SystemRoadmapOptimizer();
//
//				//SETS SYSTEM DATA FOR THE OPTIMIZER
//				System.out.println("SETTING SYSTEM DATA");
//				opt.setSystemData(sysList, sysBudget, bluMap, dataSystemMap, granularBLUMap, retirementMap);
//				opt.setOptimizationConstants(year, totalYears, systemCostPenalty, totalSystemCount, limit);
//				try {
//					//SETS UP THE OPTIMIZATION MODEL
//					opt.setupModel();
//				} catch (LpSolveException e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//
//				//EXECUTES THE MODEL
//				opt.execute();
//				System.out.println("OPTIMIZATION COMPLETED");
//				keptSystems = opt.getKeptSystems();
//
//				decomList = getDecommissionedSystems(keptSystems);
//				replacementThresholdReached = determineCostPenalties(decomList, keptSystems);
//
//				//DELETES THE MODEL
//				opt.deleteModel();
//			}
//			buildInvestmentMap(decomList);
//			buildSustainmentMap();
//			for(String system: decomList){
//				if(!decommissionedSystems.contains(system)){
//					decommissionedSystems.add(system);
//					timeline.addSystemTransition(year, system, "");
//				}
//			}
//			updateSystemList(keptSystems);
//			updateInterfaceCounts(decomList);
//			year++;
//		}
//		buildSustainmentMap();
//		for(int i = 0; i<timeline.getTimeData().size(); i++){
//			Map<String, List<String>> yearMap = timeline.getTimeData().get(i);
//			for(String system: yearMap.keySet()){
//				Object[] row = new Object[2];
//				row[0] = i;
//				row[1] = system;
//				outputList.add(row);
//			}
//		}
//	}
//
//	/**
//	 * @param keptSystems
//	 */
//	private void updateSystemList(List<String> keptSystems){
//		//update sysList to have the keptSystems for the next run
//		sysList = new String[keptSystems.size()];
//
//		Iterator<String> keptSystemsIt = keptSystems.iterator();
//		int idx = 0;
//
//		while(keptSystemsIt.hasNext()){
//			String system = keptSystemsIt.next();
//			sysList[idx] = system;
//			idx++;
//		}
//	}
//
//	/**
//	 * @param decomList
//	 */
//	private boolean determineCostPenalties(List<String> decomList, List<String> keptSystems){
//
//		//clearPenalties so we don't carry over penalties from previous years or iterations
//		resetPenalties();
//
//		//this method calculates the alpha
//		Map<String, Double>	values = new HashMap<String, Double>();
//
//		for(String system: keptSystems){
//			double upstreamInterfaceCount = 0.0;
//			if(values.keySet().contains(system)){
//				continue;
//			}
//
//			if(interfaceCountMap.keySet().contains(system) && !decommissionedSystems.contains(system)){
//				//list of enduring systems
//				for(String sys: decomList){
//					//list of interfaces for this system
//					for(String upstreamSystem: interfaceCountMap.get(system)){
//						if(upstreamSystem.equals(sys)){
//							upstreamInterfaceCount++;
//						}
//					}
//				}
//			}
//			double penalty = (double)upstreamInterfaceCount * (double)	interfaceCost;
//			values.put(system, penalty);
//		}
//
//		for(String system: values.keySet()){
//			systemCostPenalty.put(system, values.get(system));
//		}
//
//		Collections.sort(decomList);
//
//		if(previousValues.contains(decomList)){
//			System.out.println("Repeated set found. Exiting loop.");
//			updateBudgets();
//			previousValues.clear();
//			return false;
//		}else if(year==totalYears){
//			System.out.println("Last year. Decommissioning everything remaining");
//			updateBudgets();
//			return false;
//		}else if(decomList.size()==0){
//			System.err.println("No systems to decommission? Something went wrong. Exiting loop");
//			previousValues.clear();
//			return false;
//		}else{
//			System.out.println("Re-running iteration.");
//			previousValues.add(decomList);
//			return true;
//		}
//
//	}
//
//	/**
//	 * 
//	 */
//	private void resetPenalties(){
//
//		System.out.println("CLEARING PENALTIES");
//		for(String system: systemCostPenalty.keySet()){
//			systemCostPenalty.put(system, 0.0);
//		}
//	}
//
//	/**
//	 * @param keptSystems
//	 */
//	private List<String> getDecommissionedSystems(List<String> keptSystems){
//		List<String> outputSystems = new ArrayList<String>();
//
//		for(String system: sysList){
//			if(!keptSystems.contains(system)){
//				outputSystems.add(system);
//			}
//		}
//		return outputSystems;
//	}
//	
//	/**
//	 * 
//	 */
//	private void updateBudgets(){
//
//		System.out.println("UPDATING BUDGETS TO INCLUDE PENALTY COSTS");
//		for(String system: systemCostPenalty.keySet()){
//			sysBudget.put(system, ((double)sysBudget.get(system)+(double)systemCostPenalty.get(system)));
//		}
//
//	}
//
//	/**
//	 * @param removedSystems
//	 */
//	private void updateInterfaceCounts(List<String> removedSystems){
//
//		//updates interface counts for the systems
//		for(String key: interfaceCountMap.keySet()){
//			List<String> interfaces = interfaceCountMap.get(key);
//			for(String system: removedSystems){
//				while(interfaces.contains(system)){
//					interfaces.remove(system);
//				}
//			}
//		}
//	}
//
//	/**
//	 * @param decomList
//	 */
//	public void buildInvestmentMap(List<String> decomList){
//
//		Map<String, Double> newYearMap = new HashMap<String, Double>();
//		for(String system: decomList){
//			int interfaceToDecomSystem = 0;
//			if(sdsMap.keySet().contains(system)){
//				List<List<String>> downstreams = sdsMap.get(system);
//				for(List<String> downstreamInterface: downstreams){
//					if(decommissionedSystems.contains(downstreamInterface.get(0)) || decomList.contains(downstreamInterface.get(0))){
//						interfaceToDecomSystem++;
//					}
//
//				}
//				if(sdsMap.keySet().contains(system)){
//					if(system.equals("CHOOSE")){
//						System.out.println();
//					}
//					if(sdsMap.get(system).size() > 0){
//						int count = sdsMap.get(system).size();
//						newYearMap.put(system, (sdsMap.get(system).size()-interfaceToDecomSystem)*interfaceCost);						
//					}else{
//						newYearMap.put(system, 0.0);
//					}
//				}else{
//					newYearMap.put(system, 0.0);
//				}
//			}
//		}
//		investmentMap.add(newYearMap);
//		timeline.setSystemInvestmentMap(investmentMap);
//	}
//
//	public void buildSustainmentMap(){
//
//		Map<String, Double> newYearMap = new HashMap<String, Double>();
//
//		for(String system: decommissionedSystems){
//			int localDownstreamCount = 0;
//			if(sdsMap.keySet().contains(system)){
//				for(List<String> downstreamInterface: sdsMap.get(system)){
//					if(!decommissionedSystems.contains(downstreamInterface.get(0))){
//						localDownstreamCount++;
//					}
//				}
//				newYearMap.put(system, localDownstreamCount*interfaceCost*interfaceSustainmentPercent);
//			}
//		}
//
//		sustainmentMap.add(newYearMap);
//		timeline.setInterfaceSustainmentMap(sustainmentMap);
//	}
//	
//	/* (non-Javadoc)
//	 * @see prerna.ui.components.playsheets.GridPlaySheet#getVariable(java.lang.String, prerna.engine.api.ISelectStatement)
//	 */
//	public Object getVariable(String varName, ISelectStatement sjss){
//		Object var = sjss.getVar(varName);
//		return var;
//	}
//
//	@Override
//	public void createTimeline() {
//		// TODO Auto-generated method stub
//	}
//
//	@Override
//	public void createTimeline(IEngine engine) {
//
//		createTimeline(engine, "DFAS");
//		
//	}
//}
