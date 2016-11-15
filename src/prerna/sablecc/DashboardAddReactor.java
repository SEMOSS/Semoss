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
import prerna.sablecc.meta.IPkqlMetadata;

public class DashboardAddReactor extends AbstractReactor {

	private static int WIDGET_ID_INDEX = 0;
	private static int INSIGHT_ID_INDEX = 1;
	private static int PANEL_ID_INDEX = 2;
	
	public DashboardAddReactor() {
		// example join is data.join("insight1", "insight2", c:Title, c:Movie_Title, inner.join);
		//TODO : changing back to table once we make the reactor
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.REL_TYPE, PKQLEnum.OPEN_DATA, PKQLEnum.JOIN_PARAM, PKQLReactor.VAR.toString()};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DASHBOARD_ADD;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		List<String> insightsToJoin;
		try {
			Dashboard dashboard = (Dashboard)myStore.get("G");
			
			insightsToJoin = (List<String>) myStore.get(PKQLEnum.JOIN_PARAM);			
			String widgetId = insightsToJoin.get(WIDGET_ID_INDEX);
			String insightId = insightsToJoin.get(INSIGHT_ID_INDEX);
			String panelId = insightsToJoin.get(PANEL_ID_INDEX);
			
//			List<Insight> insights = new ArrayList<>();
//			for(String insightID : insightsToJoin) {
//				Insight insight = InsightStore.getInstance().get(insightID);
//				if(insight == null) {
//					System.err.println("insight "+ insightID+" not found...");
//					return null;
//				}
//				insights.add(insight);
//			}
			
			List<Insight> insights = new ArrayList<>();
			Insight insight = InsightStore.getInstance().get(insightId);
			if(insight == null) {
				System.err.println("insight "+ insightId+" not found...");
				return null;
			}
			insights.add(insight);
			
			dashboard.addInsights(insights);
			dashboard.setWidgetId(insightId, new String[]{widgetId, panelId});
			setDashboardData(insightsToJoin);
		} catch(Exception e) {
			e.printStackTrace();
			System.err.println("Error retrieving insights");
			return null;
		}
		
		return null;
	}
	
	private void setDashboardData(List<String> insightIDs) {
		List joinDataList = new ArrayList();

		Map map = new HashMap();
		map.put("insightId", insightIDs.get(INSIGHT_ID_INDEX));
		map.put("widgetId", insightIDs.get(WIDGET_ID_INDEX));
		map.put("panelId", insightIDs.get(PANEL_ID_INDEX));
		joinDataList.add(map);

		Map<String, List> data = new HashMap<>();
		data.put("addedInsights", joinDataList);
		
		this.myStore.put("DashboardData", data);
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
