package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.log4j.Logger;

import prerna.om.DBCMVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class AbstractPopupMenuListener implements ActionListener {

	IPlaySheet ps = null;
	DBCMVertex [] vertices = null;
	
	Logger logger = Logger.getLogger(getClass());
	
	public void setPlaysheet(IPlaySheet ps)
	{
		this.ps = ps;
	}
	
	public void setDBCMVertex(DBCMVertex [] vertices)
	{
		logger.debug("Set the vertices " + vertices.length);
		this.vertices = vertices;
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		// get the engine
		// execute the neighbor hood 
		// paint it
		// get the query from the 
		
		String query = getQuery();
		
		logger.debug(" The Query is " + query);
		logger.debug("Vertices are set to " + vertices);

		// get the DBCM Vertices and fill the filter value
		String filter = "";
		logger.debug("Vertices Length " + vertices.length);
		for(int nodeIndex = 0;nodeIndex < vertices.length;nodeIndex++)
		{
			filter = filter + "\"" + vertices[nodeIndex].getProperty(Constants.VERTEX_NAME) + "\"";
			if(nodeIndex + 1 < vertices.length)
				filter = filter + ",";
				
		}
		logger.info("Filter is " + filter);
		
		// contemplating between this one and before
		//filter = vfd.getFilterString();
		
		query = query.replace(DIHelper.getInstance().getProperty(Constants.FILTER), filter);

		logger.debug(" Filtered Query is " + query);

		ps.setQuery(query);
		ps.extendView();
		//ps.createView();
	}
	
	public abstract String getQuery();
	
}
