package prerna.ds.export.graph;

import java.util.Map;
import java.util.Set;

import prerna.ds.h2.H2Frame;

public class RdbmsGraphExporter implements IGraphExporter {

	private H2Frame frame;
	private Map<String, Set<String>> edgeHash;
	
	public RdbmsGraphExporter(H2Frame frame) {
		this.frame = frame;
		this.edgeHash = frame.getEdgeHash();
	}
	
	@Override
	public boolean hasNextEdge() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, Object> getNextEdge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNextVert() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, Object> getNextVert() {
		// TODO Auto-generated method stub
		return null;
	}

}
