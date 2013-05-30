package prerna.ui.components;

import java.awt.Component;
import java.awt.event.MouseListener;

import org.apache.log4j.Logger;

import prerna.om.DBCMVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;

public class RelationPopup extends AbstractRelationPopup implements MouseListener{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	DBCMVertex [] pickedVertex = null;
	Logger logger = Logger.getLogger(getClass());
	Component comp = null;
	int x,y;
	
	// core class for neighbor hoods etc.
	public RelationPopup(IPlaySheet ps, DBCMVertex [] pickedVertex)
	{
		super("Relation Types", ps, pickedVertex, Constants.NEIGHBORHOOD_PREDICATE_QUERY, // main query
				Constants.NEIGHBORHOOD_PREDICATE_TYPE_PAINTER_QUERY,  // predicate query
				Constants.NEIGHBORHOOD_PREDICATE_TYPE_PAINTER_QUERY, // type painter query for alternate
				Constants.NEIGHBORHOOD_PREDICATE_ALT_QUERY,  // alternative to main query
				Constants.NEIGHBORHOOD_PREDICATE_TYPE_ALT2_PAINTER_QUERY); 
	}	
}
