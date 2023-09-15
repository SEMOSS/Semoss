package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lpsolve.LpSolveException;
import prerna.annotations.BREAKOUT;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;

@BREAKOUT
public class ActivityGroupOptimizationGenerator implements ITimelineGenerator{


	protected static final Logger LOGGER = LogManager.getLogger(ActivityGroupOptimizationGenerator.class.getName());

	boolean completed = true;
	boolean constraints = false;
	
	OUSDTimeline timeline = new OUSDTimeline();
	IDatabaseEngine roadmapEngine;
	String sysOwner;
	ActivityGroupRiskCalculator calc = new ActivityGroupRiskCalculator();
	private String cleanActInsightString = "What clean groups can activities supporting a given E2E be put into?";

	//optimization values
	int year = 1;
	double interfaceCost = 350000.0;
	double interfaceSustainmentPercent = 0.18;
	double budgetConstraint = 0;
	private double failureRate = 0.001;


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
	List<String> enduringSystems = new ArrayList<String>();
	Map<String, List<String>> dataSystemMap = new HashMap<String, List<String>>();
	Map<String, List<String>> granularBLUMap = new HashMap<String, List<String>>();
	Map<String, List<String>> interfaceCountMap = new HashMap<String, List<String>>(); //down system -> list of upstream systems where an interface exists between the two systems
	Map<String, List<List<String>>> sdsMap; //systems -> list of data obj that are provided

	//calculated values
	Map<String, Double> maxFIHT = new HashMap<String, Double>();
	double treeMax = 0.0;

	List<Map<String, Double>> investmentMap = new ArrayList<Map<String, Double>>();
	List<Map<String, Double>> sustainmentMap = new ArrayList<Map<String, Double>>();

	@Override
	public void createTimeline(IDatabaseEngine engine, String owner){
		roadmapEngine = engine;
		sysOwner = owner;
		runOptimization();
	}

	@Override
	public OUSDTimeline getTimeline(){
		return timeline;
	}

	public void runOptimization(){
		List<Double> treeList = new ArrayList<Double>();

//		ExecuteQueryProcessor proc = new ExecuteQueryProcessor();
//		Hashtable<String, Object> emptyTable = new Hashtable<String, Object>();
//		proc.processQuestionQuery(roadmapEngine, cleanActInsightString, emptyTable);
		SequencingDecommissioningPlaySheet activitySheet = (SequencingDecommissioningPlaySheet) OUSDPlaysheetHelper.getPlaySheetFromName(cleanActInsightString, roadmapEngine);

		Map<Integer, List<List<Integer>>> decomGroups = activitySheet.collectData();
		Object[] groupData = activitySheet.getResults(decomGroups);

		calc.setGroupData((Map<String, List<String>>)groupData[0], (Map<String, List<String>>)groupData[1], (List<String>)groupData[2]);
		calc.setFailure(this.failureRate);	

		List<String> owners = new ArrayList<String>();
		owners.add(sysOwner);
		List<String> osystems = OUSDQueryHelper.getSystemsByOwners(roadmapEngine, owners);

		Object[] systemReturn = OUSDPlaysheetHelper.createSysLists(osystems);
		String[] sysList = (String[]) systemReturn[0];
		String sysBindingsString = (String) systemReturn[1];
		this.sysList = sysList;
		int totalSystemCount = osystems.size();

		sysBudget = OUSDQueryHelper.getBudgetData(roadmapEngine, sysList);

		interfaceCountMap = OUSDQueryHelper.getSystemToSystemDataWithSystemBind(roadmapEngine, sysBindingsString);
		dataSystemMap = OUSDQueryHelper.getDataCreatedBySystem(roadmapEngine, sysBindingsString);

		enduringSystems = OUSDQueryHelper.getEnduringSystems(roadmapEngine);	
		sdsMap = OUSDQueryHelper.getSystemToSystemData(roadmapEngine);

		Map<String, Map<String, List<String>>> activityDataSystemMap = OUSDQueryHelper.getActivityDataSystemMap(roadmapEngine, sysBindingsString);
		Map<String, Map<String, List<String>>> activityBluSystemMap = OUSDQueryHelper.getActivityGranularBluSystemMap(roadmapEngine, sysBindingsString);

		bluDataSystemMap = OUSDPlaysheetHelper.combineMaps(activityDataSystemMap, activityBluSystemMap);
		calc.setBluMap(bluDataSystemMap);

		for(String activity: activityBluSystemMap.keySet()){
			Map<String, List<String>> bluMap = activityBluSystemMap.get(activity);
			for(String blu: bluMap.keySet()){
				if(granularBLUMap.containsKey(blu)){
					for(String system: bluMap.get(blu)){
						if(granularBLUMap.get(blu).contains(system)){
							continue;
						}else{
							List<String> systemList = granularBLUMap.get(blu);
							systemList.add(system);
							granularBLUMap.put(blu, systemList);
						}
					}
				}else{
					granularBLUMap.put(blu, bluMap.get(blu));
				}
			}
		}

		timeline.setDataSystemMap(dataSystemMap);
		timeline.setBudgetMap(sysBudget);
		timeline.setGranularBLUMap(granularBLUMap);
		timeline.setSystemDownstream(sdsMap);


		for(String key: sdsMap.keySet()){
			List<List<String>> downstreams = sdsMap.get(key);
			for(List<String> downstream: downstreams){
				if(upstreamInterfaceCount.keySet().contains(downstream.get(0))){
					continue;
				}else{
					upstreamInterfaceCount.put(downstream.get(0), 0);
				}
			}
		}

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

			calc.setData(Arrays.asList(sysList));

			findTreeMax(treeList);
			updateMaxList();

			List<String> keptSystems = new ArrayList<String>();
			List<String> decomList = new ArrayList<String>();
			boolean replacementThresholdReached = true;

			while(replacementThresholdReached){

				ActivityGroupOptimizer opt = new ActivityGroupOptimizer();

				//SETS SYSTEM DATA FOR THE OPTIMIZER
				System.out.println("SETTING SYSTEM DATA");
				opt.setSystemData(sysList, sysBudget, dataSystemMap, enduringSystems, granularBLUMap, maxFIHT, upstreamInterfaceCount, currentDownstreamInterfaceCount);
				opt.setOptimizationConstants(totalSystemCount, constraints, treeMax, budgetConstraint, interfaceSustainmentPercent, interfaceCost);
				try {
					//SETS UP THE OPTIMIZATION MODEL
					opt.setupModel();
					opt.writeLp("OUSD Optimization year " + year + ".lp");
				} catch (LpSolveException e) {
					e.printStackTrace();
				}

				//EXECUTES THE MODEL
				opt.execute();
				System.out.println("OPTIMIZATION COMPLETED");
				keptSystems = opt.getKeptSystems();

				decomList = getDecommissionedSystems(keptSystems);
				replacementThresholdReached = determineExitStrategy(decomList, keptSystems);

				//DELETES THE MODEL
				opt.deleteModel();
			}
			buildInvestmentMap(decomList);
			buildSustainmentMap();
			updateSystemList(keptSystems);
			updateInterfaceCounts(decomList);
			updateBudgetConstraint(decomList, keptSystems);
			updateGranularBLUMap(decomList);
			updateDataSystemMap(decomList);
			updateBluDataSystemMap(decomList);
			constraints = false;

			for(String system: decomList){
				if(!decommissionedSystems.contains(system)){
					decommissionedSystems.add(system);
					timeline.addSystemTransition(year, system, "");
				}
			}

			year++;
		}
		buildSustainmentMap();
	}

	/**
	 * @param decomList
	 */
	private boolean determineExitStrategy(List<String> decomList, List<String> keptSystems){

		boolean fullExit = true;

		for(String system: decomList){
			if(!enduringSystems.contains(system)){
				continue;
			}else{
				fullExit=false;
			}
		}

		for(String system: keptSystems){
			if(enduringSystems.contains(system)){
				continue;
			}else{
				fullExit=false;
			}
		}

		Collections.sort(decomList);

		if(previousValues.contains(decomList)){
			System.out.println("Repeated set found. Exiting loop.");
			previousValues.clear();
			return false;
		}else if(fullExit){
			System.out.println("Optimization kept everything. All remaining system provide unique BLU/Data. Decommissioning everything except enduring systems.");
			previousValues.clear();
			completed = false;
			return false;
		}else if(decomList.size()==0){
			System.err.println("No systems to decommission? Something may be wrong. Decommissioning everything.");
			for(String system: keptSystems){
				if(!enduringSystems.contains(system)){
					decomList.add(system);
				}
			}
			completed = false;
			return false;
		}else if(year == 1){
			System.out.println("Completed optimization for first year. Decommissioning only systems with no downstream interfaces.");
			previousValues.add(decomList);
			return false;
		}else{

			if(constraints){
				for(String system: keptSystems){
					List<List<String>> downstreams = sdsMap.get(system);
					int removedSystemCount = 0;
					if(downstreams == null){
						currentDownstreamInterfaceCount.put(system, 0);
					}else{
						for(List<String> downstreamSystem: downstreams){
							String downSystem = downstreamSystem.get(0);
							if(decomList.contains(downSystem) || decommissionedSystems.contains(downSystem)){
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
			}

			System.out.println("Repeating iteration with additional constraints.");
			constraints = true;
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
	private void updateBudgetConstraint(List<String> decomList, List<String> keptSystems){

		budgetConstraint = 0;

		for(String system: decomList){
			budgetConstraint = budgetConstraint + sysBudget.get(system);
		}
		for(String system: decommissionedSystems){
			budgetConstraint = budgetConstraint + sysBudget.get(system);
		}
		int localCount = 0;
		for(String system: upstreamInterfaceCount.keySet()){
			if(!decommissionedSystems.contains(system) && !decomList.contains(system)){
				localCount = localCount + upstreamInterfaceCount.get(system);				
			}
		}

		budgetConstraint = budgetConstraint - (interfaceCost*interfaceSustainmentPercent)*localCount;

	}

	private void updateDataSystemMap(List<String> decomList){

		Map<String, List<String>> tempMap = new HashMap<String, List<String>>();

		for(String data: dataSystemMap.keySet()){
			List<String> supportingSystems = dataSystemMap.get(data);
			List<String> remainingSystems = new ArrayList<String>();
			for(String system: supportingSystems){
				if(decomList.contains(system)){
					continue;
				}else{
					remainingSystems.add(system);
				}
			}
			tempMap.put(data, supportingSystems);
		}

		dataSystemMap = tempMap;
	}

	private void updateGranularBLUMap(List<String> decomList){

		Map<String, List<String>> tempMap = new HashMap<String, List<String>>();

		for(String blu: granularBLUMap.keySet()){
			List<String> supportingSystems = granularBLUMap.get(blu);
			List<String> remainingSystems = new ArrayList<String>();
			for(String system: supportingSystems){
				if(decomList.contains(system)){
					continue;
				}else{
					remainingSystems.add(system);
				}
			}
			tempMap.put(blu, remainingSystems);
		}

		granularBLUMap = tempMap;
	}

	/**
	 * @param removedSystems
	 */
	private void updateInterfaceCounts(List<String> decomList){

		//update list for remaining systems
		for(String system: upstreamInterfaceCount.keySet()){
			int localInterfaceCount = 0;
			if(system.equals("STARS-HCM")){
				System.out.println();
			}
			if(interfaceCountMap.keySet().contains(system) && !decommissionedSystems.contains(system) && !decomList.contains(system)){
				for(String sys: decomList){
					//list of upstream interfaces for this system
					for(String upstreamSystem: interfaceCountMap.get(system)){
						if(upstreamSystem.equals(sys)){
							localInterfaceCount++;
						}
					}
				}
				for(String sys: decommissionedSystems){
					for(String upstreamSystem: interfaceCountMap.get(system)){
						if(upstreamSystem.equals(sys)){
							localInterfaceCount++;
						}
					}
				}
			}
			upstreamInterfaceCount.put(system, localInterfaceCount);
		}

	}

	private void updateBluDataSystemMap(List<String> decomList){
		for(String activity: bluDataSystemMap.keySet()){
			Map<String, List<String>> bluDataSystem = bluDataSystemMap.get(activity);
			for(String bluData: bluDataSystem.keySet()){
				List<String> systems = bluDataSystem.get(bluData);
				for(String system: systems){
					if(decomList.contains(system)){
						List<String> updatedSystems = new ArrayList<String>(systems);
						updatedSystems.remove(system);
						systems = updatedSystems;
						bluDataSystem.put(bluData, updatedSystems);
					}
				}
			}
			bluDataSystemMap.put(activity, bluDataSystem);
		}
		calc.setBluMap(bluDataSystemMap);
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

	private void findTreeMax(List<Double> treeList){

		Map<String, Double> systemResults = calc.runRiskCalculations();
		double treeMax = 0.0;
		for(String key: systemResults.keySet()){
			double value = systemResults.get(key);
			if(value > treeMax){
				treeMax = value;
			}
		}
		this.treeMax = (double) ((1)/(1 - treeMax));
		treeList.add(treeMax);
		timeline.setTreeMaxList(treeList);
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

	/**
	 * @param decomList
	 */
	public void buildInvestmentMap(List<String> decomList){

		Map<String, Double> newYearMap = new HashMap<String, Double>();
		for(String system: decomList){
			int interfaceToDecomSystem = 0;
			if(sdsMap.keySet().contains(system)){
				List<List<String>> downstreams = sdsMap.get(system);
				for(List<String> downstreamInterface: downstreams){
					if(decommissionedSystems.contains(downstreamInterface.get(0)) || decomList.contains(downstreamInterface.get(0))){
						interfaceToDecomSystem++;
					}

				}
				newYearMap.put(system, (originalDownstreamInterfaceCount.get(system)-interfaceToDecomSystem)*interfaceCost);
			}
		}
		investmentMap.add(newYearMap);
		timeline.setSystemInvestmentMap(investmentMap);
	}

	public void buildSustainmentMap(){

		Map<String, Double> newYearMap = new HashMap<String, Double>();

		for(String system: decommissionedSystems){
			int localDownstreamCount = 0;
			if(sdsMap.keySet().contains(system)){
				for(List<String> downstreamInterface: sdsMap.get(system)){
					if(!decommissionedSystems.contains(downstreamInterface.get(0))){
						localDownstreamCount++;
					}
				}
				newYearMap.put(system, localDownstreamCount*interfaceCost*interfaceSustainmentPercent);
			}
		}

		sustainmentMap.add(newYearMap);
		timeline.setInterfaceSustainmentMap(sustainmentMap);
	}

	@Override
	public void createTimeline() {

	}

	@Override
	public void createTimeline(IDatabaseEngine engine) {
		createTimeline(engine, "DFAS");
	}

}
