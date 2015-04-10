package prerna.ds;

public interface ILinkeableEngine {

	// gets the OWL file
	// for now I am not implementing it
	// public Stream getOWL();
	
	// gets the nodes this node can be connected
	public String [] getRelatedTypes(String nodeType);
	
	// gets the nodes for the given set of nodes and type
	// usually this is returning the ID
	// the main node itself is possibly sitting in the redis zone
	public ISEMOSSNode [][] getRelatedNodes(ISEMOSSNode [] fromNodes, String toNodeType);
}
