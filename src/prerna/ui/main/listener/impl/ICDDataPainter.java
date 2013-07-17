package prerna.ui.main.listener.impl;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class ICDDataPainter extends AbstractPopupMenuListener {

	String query = DIHelper.getInstance().getProperty(Constants.DBCM_ICD_NEIGHBORHOOD);
	
	public String getQuery()
	{
		return this.query;
	}
}
