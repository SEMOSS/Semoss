package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.annotations.BREAKOUT;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

@BREAKOUT
public class RoadmapFromDataGenerator implements ITimelineGenerator{

	OUSDTimeline timeline = new OUSDTimeline();
	IDatabaseEngine roadmapEngine;
	String sysOwner;

	String systemBindings;
	String[] sysList;
	double interfaceCost = 350000;
	double interfaceSustainmentPercent = 0.18;

	@Override
	public void createTimeline(IDatabaseEngine engine, String owner){
		roadmapEngine = engine;
		sysOwner = owner;			
		queryForRoadmap();
		queryForMissingInformation(sysList, systemBindings);
	}

	@Override
	public OUSDTimeline getTimeline() {
		return timeline;
	}

	/**
	 * @return
	 */
	private OUSDTimeline queryForRoadmap(){

		//query for systems owned by whoever
		List<String> owners = new ArrayList<String>();
		owners.add(sysOwner);
		List<String> systemList = OUSDQueryHelper.getSystemsByOwners(this.roadmapEngine, owners);

		Object[] systemReturn = OUSDPlaysheetHelper.createSysLists(systemList);
		sysList = (String[]) systemReturn[0];
		systemBindings = systemReturn[1].toString();

		//get query from engine and insert list of systems as bindings
		String query = this.roadmapEngine.getProperty("ROADMAP_QUERY");
		query = query.replaceAll("!SYSTEMS!", systemBindings);

		//instantiate timeline object and the fyIndexArray belonging to the timeline object
		timeline = new OUSDTimeline();
		timeline.setFyIndexArray(new ArrayList<Integer>());

		//retrieve results of query and insert them into the timeline object
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(this.roadmapEngine, query);
		String[] wNames = wrap.getVariables();
		while(wrap.hasNext()){
			ISelectStatement iss = wrap.next();

			Integer fy = Integer.parseInt(iss.getVar(wNames[0]).toString());
			String decomSys = iss.getVar(wNames[1]).toString();
			String endureSys = iss.getVar(wNames[2]).toString();

			timeline.insertFy(fy);
			timeline.addSystemTransition(fy, decomSys, endureSys);
		}

		Map<String, List<List<String>>> sdsMap = OUSDQueryHelper.getSystemToSystemData(roadmapEngine);
		timeline.setSystemDownstream(sdsMap);

		buildInvestmentMap();
		buildSustainmentCostMap();

		OUSDPlaysheetHelper.buildRiskList(timeline, sysList, systemBindings, roadmapEngine);

		return timeline;

	}

	/**
	 * @param sysBindingsString
	 */
	private void queryForMissingInformation(String[] sysList, String sysBindingsString){

		Map<String, Double> sysBudget = new HashMap<String, Double>();
		Map<String, List<String>> bluMap = new HashMap<String, List<String>>();
		Map<String, List<String>> dataSystemMap = new HashMap<String, List<String>>();
		Map<String, List<String>> granularBLUMap = new HashMap<String, List<String>>();
		Map<String, List<List<String>>> sdsMap = new HashMap<String, List<List<String>>>();

		sysBudget = OUSDQueryHelper.getBudgetData(roadmapEngine, sysList);
		dataSystemMap = OUSDQueryHelper.getDataCreatedBySystem(roadmapEngine, sysBindingsString);
		bluMap = OUSDQueryHelper.getBLUtoSystem(roadmapEngine, sysBindingsString);
		sdsMap = OUSDQueryHelper.getSystemToSystemData(roadmapEngine);

		String bluBindings = OUSDPlaysheetHelper.bluBindingStringMaker(bluMap);

		List<Object[]> bluDataList = OUSDQueryHelper.getDataConsumedByBLU(roadmapEngine, bluBindings);
		granularBLUMap = OUSDPlaysheetHelper.systemToGranularBLU(bluDataList, dataSystemMap, bluMap);

		timeline.setBudgetMap(sysBudget);
		timeline.setDataSystemMap(dataSystemMap);
		timeline.setGranularBLUMap(granularBLUMap);
		timeline.setSystemDownstream(sdsMap);
	}

	private void buildInvestmentMap(){
		List<Map<String, Double>> investmentMap = new ArrayList<Map<String, Double>>();
		List<String> decommissionedSystems = new ArrayList<String>();
		Map<String, List<List<String>>> sdsMap = timeline.getSystemDownstreamMap();

		for(int i = 0; i<timeline.getTimeData().size(); i++){
			Map<String, List<String>> yearMap = timeline.getTimeData().get(i);
			Map<String, Double> newYearMap = new HashMap<String, Double>();

			for(String system: yearMap.keySet()){
				boolean lastYear = true;

				for(int j = i+1; j<timeline.getTimeData().size(); j++){
					Map<String, List<String>> futureYearMap = timeline.getTimeData().get(j);
					if(futureYearMap.keySet().contains(system)){
						lastYear = false;
						break;
					}
				}
				double systemIntCount = 0;

				if(!lastYear){
					continue;
				}else{

					decommissionedSystems.add(system);

					List<List<String>> downstream = new ArrayList<List<String>>();
					if(sdsMap.keySet().contains(system)){
						downstream = sdsMap.get(system);
					}else{
						newYearMap.put(system, systemIntCount);
						continue;
					}

					for(List<String> downstreamInterface: downstream){
						if(decommissionedSystems.contains(downstreamInterface.get(0)) || yearMap.keySet().contains(downstreamInterface.get(0))){
							continue;
						}else{
							systemIntCount++;
						}
					}
				}
				if(lastYear){
					newYearMap.put(system, systemIntCount*interfaceCost);
				}
			}
			investmentMap.add(newYearMap);
		}

		timeline.setSystemInvestmentMap(investmentMap);
	}

	private void buildSustainmentCostMap(){

		List<String> decommissionedSystems = new ArrayList<String>();
		Map<String, List<List<String>>> sdsMap = timeline.getSystemDownstreamMap();
		List<Map<String, Double>> sustainmentMap = new ArrayList<Map<String, Double>>();

		sustainmentMap.add(new HashMap<String, Double>());

		for(int i=0; i<timeline.getTimeData().size(); i++){
			Map<String, List<String>> yearMap = timeline.getTimeData().get(i);

			for(String system: yearMap.keySet()){

				boolean lastYear = true;

				for(int j = i+1; j<timeline.getTimeData().size(); j++){
					Map<String, List<String>> futureYearMap = timeline.getTimeData().get(j);
					if(futureYearMap.keySet().contains(system)){
						lastYear = false;
						break;
					}
				}

				if(!lastYear){
					continue;
				}else{
					decommissionedSystems.add(system);			
				}

			}

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
		}

		timeline.setInterfaceSustainmentMap(sustainmentMap);

	}

	@Override
	public void createTimeline(){

	}

	@Override
	public void createTimeline(IDatabaseEngine engine){
		createTimeline(engine, "DFAS");
	}
}
