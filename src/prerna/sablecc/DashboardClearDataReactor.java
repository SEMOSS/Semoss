package prerna.sablecc;

import java.util.Iterator;

import prerna.om.Dashboard;
import prerna.sablecc.meta.IPkqlMetadata;

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

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
