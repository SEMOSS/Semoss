//package prerna.ui.components.specific.ousd;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.engine.api.ISelectStatement;
//import prerna.engine.api.ISelectWrapper;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.ui.components.playsheets.GridPlaySheet;
//
//public class OUSDRoadMapRetrievalPlaySheet extends GridPlaySheet{
//
//	private static final Logger LOGGER = LogManager.getLogger(OUSDRoadMapRetrievalPlaySheet.class.getName());
//	OUSDTimeline timeline = new OUSDTimeline();
//	
//	public void createTimeline(){
//
//		//turn timeline into table
//		
//	}
//
//	/**
//	 * @return
//	 */
//	private OUSDTimeline queryForRoadmap(){
//
//		//query for systems owned by whoever
//		List<String> owners = new ArrayList<String>();
//		owners.add("DFAS");
//		List<String> systems = OUSDQueryHelper.getSystemsByOwners(this.engine, owners);
//
//		String systemBindings = createSystemBinding(systems);
//
//		//get query from engine and insert list of systems as bindings
//		this.query = this.engine.getProperty("ROADMAP_QUERY");
//		this.query = this.query.replaceAll("!SYSTEMS!", systemBindings);
//		
//		//instantiate timeline object and the fyIndexArray belonging to the timeline object
//		timeline = new OUSDTimeline();
//		timeline.setFyIndexArray(new ArrayList<Integer>());
//		
//		//retrieve results of query and insert them into the timeline object
//		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(this.engine, this.query);
//		String[] wNames = wrap.getVariables();
//		while(wrap.hasNext()){
//			ISelectStatement iss = wrap.next();
//			
//			Integer fy = Integer.parseInt(iss.getVar(wNames[0]).toString());
//			String decomSys = iss.getVar(wNames[1]).toString();
//			String endureSys = iss.getVar(wNames[2]).toString();
//			
//			timeline.insertFy(fy);
//			timeline.addSystemTransition(fy, decomSys, endureSys);
//		}
//
//		return timeline;
//		
//	}
//
//	/**
//	 * @param systemList
//	 * @return
//	 */
//	private String createSystemBinding(List<String> systemList){
//		String sysBindingsString = "";
//		List<String> sysArray = new ArrayList<String>();
//		for(String system: systemList){
//			if(!sysArray.contains(system)){
//				sysBindingsString = sysBindingsString + "(<http://semoss.org/ontologies/Concept/System/" + system + ">)";
//				sysArray.add(system);
//			}
//		}
//
//		return sysBindingsString;
//	}
//
//}
