package prerna.ds.export.graph;

import java.util.Map;

import prerna.ds.r.RDataTable;

public class RGraphExporter extends AbstractGraphExporter {

	private RDataTable frame;
	
	public RGraphExporter(RDataTable frame) {
		this.frame = frame;
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
