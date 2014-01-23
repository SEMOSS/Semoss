package prerna.rdf.engine.api;

import java.util.Vector;

import prerna.om.Insight;

public interface IExplorable {
	// gets the perspectives for this engine
	public Vector getPerspectives();
	
	// gets the questions for a given perspective
	public Vector getInsights(String perspective);
	
	// get all the insights irrespective of perspective
	public Vector getInsights();

	// get the insight for a given question description
	public Insight getInsight(String label);
	
	
	// gets insights for a given type of entity
	public Vector <String> getInsight4Type(String type);
	
	// get insights for a tag
	public Vector<String> getInsight4Tag(String tag) ;
	
	// gets the from neighborhood for a given node
	public Vector <String> getFromNeighbors(String nodeType, int neighborHood);
	
	// gets the to nodes
	public Vector <String> getToNeighbors(String nodeType, int neighborHood);
	
	// gets the from and to nodes
	public Vector <String> getNeighbors(String nodeType, int neighborHood);
	


}
