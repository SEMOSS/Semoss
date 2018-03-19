package prerna.util.insight;

import java.util.Map;

import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc.PKQLRunner;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class InsightUtility {

	/**
	 * 
	 * @param insight
	 * @param pkqlCmd
	 * @return
	 * 
	 * Runs a pkql command on an insight
	 */
	public static Map runPkql(Insight insight, String pkqlCmd) {
		return insight.runPkql(pkqlCmd);
//		PKQLTransformation pkql = new PKQLTransformation();
//		Map<String, Object> props = new HashMap<String, Object>();
//		props.put(PKQLTransformation.EXPRESSION, pkqlCmd);
//		pkql.setProperties(props);
//		PKQLRunner runner = insight.getPKQLRunner();
//		pkql.setRunner(runner);
//		List<ISEMOSSTransformation> list = new Vector<ISEMOSSTransformation>();
//		list.add(pkql);
//
//		Map resultHash = null;
//		//synchronize applyCalc calls for each insight to prevent interference during calculation
//		synchronized(insight) {
//			insight.processPostTransformation(list);
//			insight.syncPkqlRunnerAndFrame(runner);
//			resultHash = insight.getPKQLData(true);
//		}
//		
//		return resultHash;
	}
	
	/**
	 * 
	 * @param insightId
	 * @param pkqlCmd
	 * @return
	 * 
	 * Runs a pkql command on an insight with the given insight ID
	 */
	public static Map runPkql(String insightId, String pkqlCmd) {
		Insight insight = InsightStore.getInstance().get(insightId);
		return runPkql(insight, pkqlCmd);
	}
	
	/**
	 * 
	 * @param insight
	 * @param sessionId
	 * @return
	 * 
	 * Static Utility Method to drop an Insight
	 */
	public static boolean dropInsight(Insight insight, String sessionId) {
		String insightID = insight.getInsightId();
//		if(insight.isJoined()){
//			insight.unJoin();
//		}
		boolean success = InsightStore.getInstance().remove(insightID);
		if(sessionId != null) {
			InsightStore.getInstance().removeFromSessionHash(sessionId, insightID);
		}
		
		IDataMaker dm = insight.getDataMaker();
		if(dm instanceof H2Frame) {
			H2Frame frame = (H2Frame)dm;
			frame.dropTable();
			if(!frame.isInMem()) {
				frame.dropOnDiskTemporalSchema();
			}
		} else if(dm instanceof RDataTable) {
			RDataTable frame = (RDataTable)dm;
			frame.closeConnection();
		} 
//		else if(dm instanceof Dashboard) {
//			Dashboard dashboard = (Dashboard)dm;
//			dashboard.dropDashboard();
//		}
		
		// also see if other variables in runner that need to be dropped
		PKQLRunner runner = insight.getPkqlRunner();
		runner.cleanUp();
		
		// native frame just holds a QueryStruct on an engine
		// nothing to do
//		else if(dm instanceof NativeFrame) {
//			NativeFrame frame = (NativeFrame) dm;
//			frame.close();
//		} 

		return success;
	}
	
	/**
	 * Creates a new insight
	 * 
	 * @return Insight
	 */
	public static Insight createInsight(String engineName) {
		//TODO: remove the engineName as a parameter
//		IEngine engine = Utility.getEngine(engineName);
//		Insight insight = new Insight(engine, "H2Frame", "Grid");
		Insight insight = new Insight();
//		insight.setUserId("myUserId");
		insight.setRdbmsId("myRdbmsId");
		insight.setInsightName("myInsightName");
		String uniqueId = InsightStore.getInstance().put(insight);
		return InsightStore.getInstance().get(uniqueId);
	}

}
