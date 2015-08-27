package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lpsolve.LpSolveException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.ui.components.ExecuteQueryProcessor;


public class ActivityGroupOptimizationGenerator implements ITimelineGenerator{


	protected static final Logger LOGGER = LogManager.getLogger(BLUSystemOptimizationPlaySheet.class.getName());

	boolean limit = true;
	boolean completed = true;
	
	OUSDTimeline timeline = new OUSDTimeline();
	IEngine roadmapEngine;
	ActivityGroupRiskCalculator calc = new ActivityGroupRiskCalculator();
	private String cleanActInsightString = "What clean groups can activities be put in?";

	//optimization values
	int year = 1;
	double interfaceCost = 350000.0;
	int interfaceCount = 0;
	double interfaceSustainmentPercent = 0.18;
	double budgetConstraint = 0;
	private double failureRate = 0.05;


	List<List<String>> previousValues = new ArrayList<List<String>>();
	Map<String, Integer> upstreamInterfaceCount = new HashMap<String, Integer>();
	Map<String, Integer> originalDownstreamInterfaceCount = new HashMap<String, Integer>();
	Map<String, Integer> currentDownstreamInterfaceCount = new HashMap<String, Integer>();

	//lists for tracking systems
	String[] sysList;
	List<String> decommissionedSystems = new ArrayList<String>();

	//maps to get values
	Map<String, Double> sysBudget;

	Map<String, Map<String, List<String>>> bluDataSystemMap = new HashMap<String, Map<String, List<String>>>(); //activity -> list of maps of blu/data -> system where blu/data supports activity, system supports blu/data	
	Map<String, List<String>> retirementMap = new HashMap<String, List<String>>();
	Map<String, List<String>> interfaceCountMap = new HashMap<String, List<String>>(); //down system -> list of upstream systems where an interface exists between the two systems
	Map<String, List<List<String>>> sdsMap; //systems -> list of data obj that are provided

	//calculated values
	Map<String, Double> maxFIHT = new HashMap<String, Double>();
	double treeMax = 0.0;

	@Override
	public void createTimeline(IEngine engine){
		roadmapEngine = engine;
		runOptimization(limit);
	}

	@Override
	public OUSDTimeline getTimeline(){
		return timeline;
	}

	public void runOptimization(boolean limit){
		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
		proc.processQuestionQuery(roadmapEngine, cleanActInsightString, emptyTable);
		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) proc.getPlaySheet();

		Map<Integer, List<List<Integer>>> decomGroups = activitySheet.collectData();
		Object[] groupData = activitySheet.getResults(decomGroups);

		calc.setGroupData((Map<String, List<String>>)groupData[0], (Map<String, List<String>>)groupData[1], (List<String>)groupData[2]);
		calc.setFailure(this.failureRate);

		List<String> owners = new ArrayList<String>();
		owners.add("DFAS");
		List<String> osystems = OUSDQueryHelper.getSystemsByOwners(roadmapEngine, owners);

		Object[] systemReturn = OUSDPlaysheetHelper.createSysLists(osystems);
		String[] sysList = (String[]) systemReturn[0];
		String sysBindingsString = (String) systemReturn[1];
		this.sysList = sysList;
		int totalSystemCount = osystems.size();

		sysBudget = OUSDQueryHelper.getBudgetData(roadmapEngine, sysList);

		interfaceCountMap = OUSDQueryHelper.getSystemToSystemDataWithSystemBind(roadmapEngine, sysBindingsString);

		retirementMap = OUSDQueryHelper.getSystemsByRetirementType(roadmapEngine, sysBindingsString);	
		sdsMap = OUSDQueryHelper.getSystemToSystemData(roadmapEngine);

		//TODO 
		Map<String, Map<String, List<String>>> activityDataSystemMap = OUSDQueryHelper.getActivityDataSystemMap(roadmapEngine, sysBindingsString);
		Map<String, Map<String, List<String>>> activityBluSystemMap = OUSDQueryHelper.getActivityGranularBluSystemMap(roadmapEngine, sysBindingsString);

		bluDataSystemMap = OUSDPlaysheetHelper.combineMaps(activityDataSystemMap, activityBluSystemMap);		
		calc.setBluMap(bluDataSystemMap);

		timeline.setRetirementMap(retirementMap);
		//TODO timeline.setDataSystemMap(dataSystemMap);
		timeline.setBudgetMap(sysBudget);
		///TODO timeline.setGranularBLUMap(granularBLUMap);
		timeline.setSystemDownstream(sdsMap);


		//first year setup stuff
		for(String system: sysList){
			upstreamInterfaceCount.put(system, 0);
			if(sdsMap.containsKey(system)){
				currentDownstreamInterfaceCount.put(system, sdsMap.get(system).size());
				originalDownstreamInterfaceCount.put(system, sdsMap.get(system).size());
			}else{
				currentDownstreamInterfaceCount.put(system, 0);
				originalDownstreamInterfaceCount.put(system, 0);
			}
		}

		while(completed){

			timeline.insertFy(year);

			System.out.println("RUNNING FOR YEAR "+year);

			if(year>1){
				sysList = this.sysList;
			}

			findTreeMax();
			updateMaxList();

			List<String> keptSystems = new ArrayList<String>();
			List<String> decomList = new ArrayList<String>();
			boolean replacementThresholdReached = true;

			while(replacementThresholdReached){

				ActivityGroupOptimizer opt = new ActivityGroupOptimizer();

				//SETS SYSTEM DATA FOR THE OPTIMIZER
				System.out.println("SETTING SYSTEM DATA");
				opt.setSystemData(sysList, sysBudget, retirementMap, maxFIHT, upstreamInterfaceCount, currentDownstreamInterfaceCount);
				opt.setOptimizationConstants(totalSystemCount, limit, treeMax, budgetConstraint, interfaceSustainmentPercent, interfaceCost);
				try {
					//SETS UP THE OPTIMIZATION MODEL
					opt.setupModel();
				} catch (LpSolveException e) {
					e.printStackTrace();
				}

				//EXECUTES THE MODEL
				opt.execute();
				System.out.println("OPTIMIZATION COMPLETED");
				keptSystems = opt.getKeptSystems();

				decomList = getDecommissionedSystems(keptSystems);
				replacementThresholdReached = determineCostPenalties(decomList, keptSystems);

				//DELETES THE MODEL
				opt.deleteModel();
			}
			for(String system: decomList){
				if(!decommissionedSystems.contains(system)){
					decommissionedSystems.add(system);
					timeline.addSystemTransition(year, system, "");
				}
			}
			updateSystemList(keptSystems);
			updateInterfaceCounts(keptSystems, decomList);
			updateBudgetConstraint(decomList);
			year++;
		}
	}

	/**
	 * @param decomList
	 */
	private boolean determineCostPenalties(List<String> decomList, List<String> keptSystems){

		for(String system: decomList){
			List<List<String>> downstreams = sdsMap.get(system);
			int removedSystemCount = 0;
			if(downstreams == null){
				currentDownstreamInterfaceCount.put(system, 0);
			}else{
				for(List<String> downstreamSystem: downstreams){
					String downSystem = downstreamSystem.get(0);
					if(decomList.contains(downSystem)){
						removedSystemCount++;
					}
				}
				if(originalDownstreamInterfaceCount.get(system)-removedSystemCount <= 0){
					currentDownstreamInterfaceCount.put(system, 0);
				}else{
					currentDownstreamInterfaceCount.put(system, originalDownstreamInterfaceCount.get(system)-removedSystemCount);
				}
			}
		}


		Collections.sort(decomList);

		if(previousValues.contains(decomList)){
			System.out.println("Repeated set found. Exiting loop.");
			previousValues.clear();
			return false;
		}else if(decomList.size()==0){
			System.err.println("No systems to decommission? Optimization completed.");
			decomList.addAll(keptSystems);
			completed = false;
			return false;
		}else{
			System.out.println("Re-running iteration.");
			previousValues.add(decomList);
			return true;
		}

	}

	/**
	 * @param keptSystems
	 */
	private List<String> getDecommissionedSystems(List<String> keptSystems){
		List<String> outputSystems = new ArrayList<String>();

		for(String system: sysList){
			if(!keptSystems.contains(system)){
				outputSystems.add(system);
			}
		}
		return outputSystems;
	}

	/**
	 * 
	 */
	private void updateBudgetConstraint(List<String> decomList){

		for(String system: decomList){
			budgetConstraint = budgetConstraint + sysBudget.get(system);
		}

		budgetConstraint = budgetConstraint - (interfaceCost*interfaceSustainmentPercent)*interfaceCount;

	}

	/**
	 * @param removedSystems
	 */
	private void updateInterfaceCounts(List<String> keptSystems, List<String> decomList){

		//this method calculates the alpha
		Map<String, Integer> values = new HashMap<String, Integer>();

		for(String system: keptSystems){
			int interfaceCount = 0;
			if(values.keySet().contains(system)){
				continue;
			}

			if(interfaceCountMap.keySet().contains(system) && !decommissionedSystems.contains(system)){
				//list of enduring systems
				for(String sys: decomList){
					//list of interfaces for this system
					for(String upstreamSystem: interfaceCountMap.get(system)){
						if(upstreamSystem.equals(sys)){
							interfaceCount++;
						}
					}
				}
			}
			upstreamInterfaceCount.put(system, interfaceCount);
		}
	}

	/**
	 * @param keptSystems
	 */
	private void updateSystemList(List<String> keptSystems){
		//update sysList to have the keptSystems for the next run
		sysList = new String[keptSystems.size()];

		Iterator<String> keptSystemsIt = keptSystems.iterator();
		int idx = 0;

		while(keptSystemsIt.hasNext()){
			String system = keptSystemsIt.next();
			sysList[idx] = system;
			idx++;
		}
	}

	private void findTreeMax(){

		Map<String, Double> systemResults = calc.runRiskCalculations();
		double treeMax = 0.0;
		for(String key: systemResults.keySet()){
			double value = systemResults.get(key);
			if(value > treeMax){
				treeMax = value;
			}
		}

		this.treeMax = (double) ((1)/(1 - treeMax));
	}

	private void updateMaxList(){

		for(String system: sysList){
			List<String> notSystem = new ArrayList<String>();
			for(String sys: sysList){
				if(!sys.equals(system)){
					notSystem.add(sys);
				}
			}
			calc.setData(notSystem);
			Map<String, Double> systemResults = calc.runRiskCalculations();
			double systemMax = 0.0;
			for(String key: systemResults.keySet()){
				double value = systemResults.get(key);
				if(value > systemMax){
					systemMax = value;
				}
			}
			maxFIHT.put(system, systemMax);
		}
	}

	/* (non-Javadoc)
	 * @see prerna.ui.components.playsheets.GridPlaySheet#getVariable(java.lang.String, prerna.engine.api.ISelectStatement)
	 */
	public Object getVariable(String varName, ISelectStatement sjss){
		Object var = sjss.getVar(varName);
		return var;
	}

	@Override
	public void createTimeline() {

	}

}
