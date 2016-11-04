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

public class DashboardRemoveReactor extends AbstractReactor {

	public DashboardRemoveReactor() {
		// example join is data.join("insight1", "insight2", c:Title, c:Movie_Title, inner.join);
		//TODO : changing back to table once we make the reactor
		String [] thisReacts = {PKQLEnum.COL_CSV, PKQLEnum.REL_TYPE, PKQLEnum.OPEN_DATA, PKQLEnum.JOIN_PARAM, PKQLReactor.VAR.toString()};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DASHBOARD_REMOVE;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		List<String> insightToRemove;
		try {
			Dashboard dashboard = (Dashboard)myStore.get("G");
			
			insightToRemove = (List<String>) myStore.get(PKQLEnum.JOIN_PARAM);			
			String widgetId = insightToRemove.get(0);
			String insightId = insightToRemove.get(1);
			String panelId = insightToRemove.get(2);
			
			Insight insight = InsightStore.getInstance().get(insightId);
			if(insight == null) {
				System.err.println("insight "+ insightId +" not found...");
				return null;
			}
			
			dashboard.removeInsight(insightId, insight);
			dashboard.removeWidgetId(insightId, new String[]{widgetId, panelId});
			setDashboardData(insightToRemove);
			
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
		map.put("insightId", insightIDs.get(1));
		map.put("widgetId", insightIDs.get(0));
		joinDataList.add(map);

		Map<String, List> data = new HashMap<>();
		data.put("removedInsights", joinDataList);
		
		this.myStore.put("DashboardData", data);
	}
}