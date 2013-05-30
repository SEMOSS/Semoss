package prerna.ui.components;

import java.awt.event.MouseListener;

import prerna.om.DBCMVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;

public class SubjectInstancePopup extends AbstractRelationPopup implements MouseListener{
		
	// core class for neighbor hoods etc.
	public SubjectInstancePopup(IPlaySheet ps, DBCMVertex [] pickedVertex)
	{
		super("Neighbor Instances", ps, pickedVertex,
				Constants.NEIGHBORHOOD_OBJECT_QUERY, 
				Constants.NEIGHBORHOOD_OBJECT_INSTANCE_PAINTER_QUERY, 
				Constants.NEIGHBORHOOD_OBJECT_ALT_INSTANCE_PAINTER_QUERY, 
				Constants.NEIGHBORHOOD_OBJECT_ALT_QUERY,
				Constants.NEIGHBORHOOD_OBJECT_INSTANCE_ALT2_PAINTER_QUERY);
		instance = true;
	}	
}
