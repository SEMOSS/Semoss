package prerna.ui.components;

import java.awt.event.MouseListener;

import org.apache.log4j.Logger;

import prerna.om.DBCMVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;

public class RelationInstancePopup extends AbstractRelationPopup implements MouseListener{
	
	Logger logger = Logger.getLogger(getClass());
	
	// core class for neighbor hoods etc.
	public RelationInstancePopup(IPlaySheet ps, DBCMVertex [] pickedVertex)
	{
		super("Relation Instances", ps, pickedVertex, Constants.NEIGHBORHOOD_PREDICATE_QUERY, // main predicate query
				Constants.NEIGHBORHOOD_PREDICATE_INSTANCE_PAINTER_QUERY, // predicate instance query used when everything is fine
				Constants.NEIGHBORHOOD_PREDICATE_ALT_INSTANCE_PAINTER_QUERY, // used when one of the relation is hosed
				Constants.NEIGHBORHOOD_PREDICATE_ALT_QUERY, 
				Constants.NEIGHBORHOOD_PREDICATE_ALT2_INSTANCE_PAINTER_QUERY);
		instance = true;
	}	
}
