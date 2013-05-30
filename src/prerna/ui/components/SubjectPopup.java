package prerna.ui.components;

import java.awt.Component;
import java.awt.event.MouseListener;

import org.apache.log4j.Logger;

import prerna.om.DBCMVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;

public class SubjectPopup extends AbstractRelationPopup implements MouseListener{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	DBCMVertex [] pickedVertex = null;
	Logger logger = Logger.getLogger(getClass());
	Component comp = null;
	int x,y;
	
	// core class for neighbor hoods etc.
	public SubjectPopup(IPlaySheet ps, DBCMVertex [] pickedVertex)
	{
		super("Neighbor Types", ps, pickedVertex, Constants.NEIGHBORHOOD_OBJECT_QUERY, 
				Constants.NEIGHBORHOOD_OBJECT_TYPE_PAINTER_QUERY, // neighborhood
				Constants.NEIGHBORHOOD_OBJECT_TYPE_PAINTER_QUERY, // alternate neighborhood
				Constants.NEIGHBORHOOD_OBJECT_ALT_QUERY, // alt main query
				Constants.NEIGHBORHOOD_OBJECT_TYPE_ALT2_PAINTER_QUERY // alt query 2
				); // alternate main query
		
	}	
}
