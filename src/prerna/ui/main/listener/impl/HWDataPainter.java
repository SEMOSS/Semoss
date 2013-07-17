package prerna.ui.main.listener.impl;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class HWDataPainter extends AbstractPopupMenuListener {

	String query = DIHelper.getInstance().getProperty(Constants.DBCM_SW_NEIGHBORHOOD);
	
	public String getQuery()
	{
		return this.query;
	}
}
