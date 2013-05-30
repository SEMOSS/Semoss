package prerna.ui.transformer;

import java.awt.Color;
import java.awt.Paint;
import java.util.Hashtable;

import org.apache.commons.collections15.Transformer;

import prerna.om.DBCMEdge;

public class SearchArrowFillPaintTransformer implements Transformer <DBCMEdge, Paint> {
	
	
	Hashtable <String, DBCMEdge> edges = null;
	
	public SearchArrowFillPaintTransformer()
	{
		
	}
	
	public void setEdges(Hashtable <String, DBCMEdge> edges)
	{
		this.edges = edges;
	}
	

	@Override
	public Paint transform(DBCMEdge edge)
	{
		
		Paint p = Color.lightGray;
		return p;
	}
}
