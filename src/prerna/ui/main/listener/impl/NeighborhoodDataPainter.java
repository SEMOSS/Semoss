package prerna.ui.main.listener.impl;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class NeighborhoodDataPainter extends AbstractPopupMenuListener {

	String query = DIHelper.getInstance().getProperty(Constants.DBCM_DATA_NEIGHBORHOOD);
	
	public String getQuery()
	{
		return this.query;
	}
}
