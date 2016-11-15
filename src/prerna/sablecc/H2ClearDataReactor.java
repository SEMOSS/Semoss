package prerna.sablecc;

import java.util.Iterator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.om.Dashboard;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Utility;

public class H2ClearDataReactor extends AbstractReactor {

	public H2ClearDataReactor() {
		String[] thisReacts = {}; 
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.CLEAR_DATA;
	}
	
	@Override
	public Iterator process() {
		H2Frame frame = (H2Frame)myStore.get(PKQLEnum.G);
		if(frame.isJoined()) {
			Dashboard dashboard = frame.getJoiner().getDashboard();
			dashboard.dropDashboard();
			dashboard.clearData();
		}
		ITableDataFrame newFrame = (ITableDataFrame) Utility.getDataMaker(null, frame.getClass().getSimpleName());
		newFrame.setUserId(frame.getUserId());
		myStore.put(PKQLEnum.G, newFrame);
		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
