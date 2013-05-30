package prerna.ui.transformer;

import java.awt.BasicStroke;
import java.awt.Stroke;
import java.util.Hashtable;

import org.apache.commons.collections15.Transformer;

import prerna.om.DBCMEdge;
import prerna.util.Constants;

public class SearchEdgeStrokeTransformer implements Transformer <DBCMEdge, Stroke> {
	
	
	Hashtable <String, DBCMEdge> edges = null;
	
	public SearchEdgeStrokeTransformer()
	{
		
	}
	
	public void setEdges(Hashtable <String, DBCMEdge> edges)
	{
		this.edges = edges;
	}
	

	@Override
	public Stroke transform(DBCMEdge edge)
	{
		
		float dash[] = {10.0f};
		
		Stroke retStroke = new BasicStroke(1.0f);
		try
		{	
               if (edges != null) {
                    if (edges.containsKey(edge.getProperty(Constants.URI))) {
                          retStroke = new BasicStroke(3.0f, BasicStroke.CAP_BUTT,
                                      BasicStroke.JOIN_MITER, 10.0f, dash, 0.0f);
                          // System.out.println("shortest path edges");
                    } else{
                          retStroke = new BasicStroke(0.1f);
                          // System.out.println(count);
                    }
                }
                else
                {
                	retStroke = new BasicStroke(0.1f, BasicStroke.CAP_BUTT,
                            BasicStroke.JOIN_ROUND);
                }
                

		}
		catch(Exception ex)
		{
			//ignore
		}
		return retStroke;
	}
}
