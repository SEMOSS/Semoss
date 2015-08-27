package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class RoadmapFromDataGenerator implements ITimelineGenerator{

	OUSDTimeline timeline = new OUSDTimeline();
	IEngine roadmapEngine;

	String systemBindings;
	String[] sysList;
	
	@Override
	public void createTimeline(IEngine engine){
		roadmapEngine = engine;
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
		owners.add("DFAS");
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
	
	@Override
	public void createTimeline(){
		
	}
}
