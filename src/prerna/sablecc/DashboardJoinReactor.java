package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.om.Dashboard;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc.PKQLEnum.PKQLReactor;

public class DashboardJoinReactor extends AbstractReactor {
	
	public DashboardJoinReactor() {
		// example join is data.join("insight1", "insight2", c:Title, c:Movie_Title, inner.join);
		//TODO : changing back to table once we make the reactor
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.REL_TYPE, PKQLEnum.OPEN_DATA, PKQLEnum.JOIN_PARAM, PKQLReactor.VAR.toString()};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DASHBOARD_JOIN;
	}

	@Override
	public Iterator process() {

		
		System.out.println("entered dashboard join");
		System.out.println("mystore has...");
		for(String s : myStore.keySet()) {
			System.out.println(s + "\t\t" + myStore.get(s));
		}
		
		// insightsToJoin contains the two insights that are being joined together
		// colsForInsightsToJoin are the two columns that are being used within these two joins
		// order matters for these two lists!
		// the first entry in insightsToJoin corresponds with the first entry in colsForInsightsToJoin
		
		List<String> insightsToJoin;
		List<List<String>> colsForInsightsToJoin;
		
		try {
			Dashboard dashboard = (Dashboard)myStore.get("G");
			
			insightsToJoin = (List<String>) myStore.get(PKQLEnum.JOIN_PARAM);			
			colsForInsightsToJoin = new ArrayList<>();
			colsForInsightsToJoin.add((List<String>) myStore.get(PKQLEnum.COL_CSV));
			String joinType = (String) myStore.get(PKQLEnum.REL_TYPE);
			
			List<Insight> insights = new ArrayList<>();
			for(String insightID : insightsToJoin) {
				Insight insight = InsightStore.getInstance().get(insightID);
				if(insight == null) {
					System.err.println("insight "+ insightID+" not found...");
					return null;
				}
				insights.add(insight);
			}
			
			dashboard.joinInsights(insights, colsForInsightsToJoin);
			setDashboardData(insightsToJoin, colsForInsightsToJoin, joinType);
		} catch(IllegalArgumentException e) {
			e.printStackTrace();
			myStore.put("RESPONSE", e.getMessage());
			myStore.put("STATUS", "fail");
			return null;
		} catch(Exception e) {
			e.printStackTrace();
			System.err.println("Error retrieving insights");
			myStore.put("RESPONSE", "Error retrieving insights");
			myStore.put("STATUS", "fail");
			return null;
		}
		
//		dashboard.joinInsights(null, colsForInsightsToJoin);
//		dashboard.addInsight(in1);
//		dashboard.addInsight(in2);
//		
//		H2Frame frame1 = null;
//		String[] frame1Headers = null;
//		H2Frame frame2 = null;
//		String[] frame2Headers = null;
//
//		try {
////			frame1 = (H2Frame) in1.getDataMaker();
////			frame1Headers = frame1.getColumnHeaders();
////			
////			frame2 = (H2Frame) in2.getDataMaker();
////			frame2Headers = frame2.getColumnHeaders();
////			
////			Map<String, String> joinCols = new HashMap<String, String>(2);
////			joinCols.put(colsForInsightsToJoin.get(0)[0], colsForInsightsToJoin.get(0)[1]);
////			
//////			H2Joiner.joinFrames(frame1, frame2, joinCols);
////			H2Joiner.joinFrames(colsForInsightsToJoin, new H2Frame[]{frame1, frame2});
////			
////			// update the data id so FE knows data has been changed
////			frame1.updateDataId();
////			frame2.updateDataId();
//		} catch (ClassCastException e) {
//			System.err.println("currently can only join h2 frames...");
//			return null;			
//		} catch (Exception e) {
//			System.err.println("Join error");
//			return null;
//		}
//		
		
		
		return null;
	}
	
	private Map getDashboardData(List<String> insightIDs, List<String> joinCols, String joinType) {
		
		List<Object> joinList = new ArrayList<>();
		for(int i = 0; i < insightIDs.size(); i++) {
			String insightID = insightIDs.get(i);
			Map<String, String> joinMap = new HashMap<>();
			joinMap.put("insightID", insightID);
			joinMap.put("column", joinCols.get(i));
			joinList.add(joinMap);
		}
		
		Map joins = new HashMap();
		joins.put("joins", joinList);
		joins.put("type", joinType);
		
		List joinDataList = new ArrayList();
		joinDataList.add(joins);
		
//		Map dashboardMap = new HashMap();
//		dashboardMap.put(dashboardID, joinDataList);
//		
//		Map<String, Object> DashboardMap = new HashMap<>();
//		DashboardMap.put("Dashboard", dashboardMap);
//		this.myStore.put("DashboardData", joinDataList);
		return joins;
	}
	
	private void setDashboardData(List<String> insightIDs, List<List<String>> joinCols, String joinType) {
		List joinDataList = new ArrayList();
		for(List<String> nextJoinCols : joinCols) {
			joinDataList.add(getDashboardData(insightIDs, nextJoinCols, joinType));
		}
		Map<String, List> data = new HashMap<>();
		data.put("joinedInsights", joinDataList);
		this.myStore.put("DashboardData", data);
	}
}
