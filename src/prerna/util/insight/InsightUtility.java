package prerna.util.insight;

import java.util.Map;

import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.om.Dashboard;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc.PKQLRunner;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Utility;

public class InsightUtility {

	/**
	 * Runs a pkql command on an insight object
	 * 
	 * @param insight
	 * @param pkqlCmd 
	 * @return map of outputs from running pkql command
	 */
	public static Map<String, Object> runPkql(Insight insight, String pkqlCmd) {
		return insight.runPkql(pkqlCmd);
	}
	
	/**
	 * Runs a pkql command on an insight with the given insight ID
	 * 
	 * @param insightId
	 * @param pkqlCmd
	 * @return map of outputs from running pkql command
	 */
	public static Map<String, Object> runPkql(String insightId, String pkqlCmd) {
		Insight insight = InsightStore.getInstance().get(insightId);
		return runPkql(insight, pkqlCmd);
	}
	
	/**
	 * Static Utility Method to drop an Insight
	 * 
	 * @param insight
	 * @param sessionId
	 * @return successful or not
	 */
	public static boolean dropInsight(Insight insight, String sessionId) {
		String insightID = insight.getInsightId();
		boolean success = InsightStore.getInstance().remove(insightID);
		if(sessionId != null) {
			InsightStore.getInstance().removeFromSessionHash(sessionId, insightID);
		}
		
		IDataMaker dm = insight.getDataMaker();
		if(dm instanceof H2Frame) {
			H2Frame frame = (H2Frame)dm;
			frame.closeRRunner();
			frame.dropTable();
			if(!frame.isInMem()) {
				frame.dropOnDiskTemporalSchema();
			}
		} else if(dm instanceof RDataTable) {
			RDataTable frame = (RDataTable)dm;
			frame.closeConnection();
		} else if(dm instanceof Dashboard) {
			Dashboard dashboard = (Dashboard)dm;
			dashboard.dropDashboard();
		}
		
		// also see if other variables in runner that need to be dropped
		PKQLRunner runner = insight.getPkqlRunner();
		runner.cleanUp();

		return success;
	}
	
	/**
	 * Creates a temporary insight (i.e. it is not saved to an insight database)
	 * 
	 * @return Insight
	 */
	public static Insight createTemporaryInsight() {
		Insight insight = new Insight();
		insight.setUserId("myUserId");
		insight.setRdbmsId("myRdbmsId");
		insight.setInsightName("myInsightName");
		String uniqueId = InsightStore.getInstance().put(insight);
		return InsightStore.getInstance().get(uniqueId);
	}
		
	/**
	 * Retrieves a saved insight from the engine with the given name using the insight's RDBMS ID
	 * 
	 * @param engineName
	 * @param rdbmsId
	 * @return Insight
	 */
	public static Insight getSavedInsight(String engineName, String rdbmsId) {
		return getSavedInsight(Utility.getEngine(engineName), rdbmsId);
	}

	/**
	 * Retrieves a saved insight from an IEngine using the insight's RDBMS ID
	 * 
	 * @param engine
	 * @param rdbmsId
	 * @return
	 */
	public static Insight getSavedInsight(IEngine engine, String rdbmsId) {
		Insight insight = engine.getInsight(rdbmsId).get(0);
		insight.reRunInsight();		
		String uniqueId = InsightStore.getInstance().put(insight);
		return InsightStore.getInstance().get(uniqueId);
	}

}
