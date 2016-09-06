package prerna.sablecc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.om.Dashboard;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc.PKQLEnum.PKQLReactor;

public class DashboardAddReactor extends AbstractReactor {

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
			
			List<Insight> insights = new ArrayList<>();
			for(String insightID : insightsToJoin) {
				Insight insight = InsightStore.getInstance().get(insightID);
				if(insight == null) {
					System.err.println("insight "+ insightID+" not found...");
					return null;
				}
				insights.add(insight);
			}
			
			dashboard.addInsights(insights);
		} catch(Exception e) {
			e.printStackTrace();
			System.err.println("Error retrieving insights");
			return null;
		}
		
		return null;
	}
}
