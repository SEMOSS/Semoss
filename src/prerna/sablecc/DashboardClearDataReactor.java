package prerna.sablecc;

import java.util.Iterator;

import prerna.om.Dashboard;

public class DashboardClearDataReactor extends AbstractReactor {

	public DashboardClearDataReactor() {
		String[] thisReacts = {}; 
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.CLEAR_DATA;
	}
	
	@Override
	public Iterator process() {
		Dashboard dashboard = (Dashboard)myStore.get("G");
		dashboard.dropDashboard();
		dashboard.clearData();
		return null;
	}
}
