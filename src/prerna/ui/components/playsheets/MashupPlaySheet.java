package prerna.ui.components.playsheets;

import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The DashboardPlaySheet class is used to send individual dashboard insights back to the front-end and does not process any query.
 */

public class MashupPlaySheet extends AbstractRDFPlaySheet {
	static final Logger logger = LogManager.getLogger(MashupPlaySheet.class.getName());

	@Override
	public Object getData() {
		Hashtable returnHash = (Hashtable) super.getData();
		returnHash.put("specificData", query);
		
		logger.info("Dashboard data: " + query);
		return returnHash;
	}
	
	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createData() {
		// TODO Auto-generated method stub

	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}

}
