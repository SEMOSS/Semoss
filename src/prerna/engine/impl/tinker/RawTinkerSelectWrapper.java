package prerna.engine.impl.tinker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.ds.QueryStruct;
import prerna.ds.TinkerFrameIterator;
import prerna.ds.TinkerIterator;
import prerna.ds.TinkerQueryEngineInterpreter;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.rdf.engine.wrappers.AbstractWrapper;

public class RawTinkerSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {
	private TinkerGraph g;
	private TinkerIterator it;

	@Override
	public IHeadersDataRow next() {
		// TODO Auto-generated method stub
		Object[] row = it.next();
		return new HeadersDataRow(displayVar, row, row);
	}

	@Override
	public String[] getDisplayVariables() {
		// displayVar = new String[numColumns];
		QueryStruct qs = (QueryStruct) ((TinkerEngine) engine).getQueryStruct();
		System.out.println("?");
		Hashtable<String, Vector<String>> selectors = qs.selectors;
		ArrayList<String> dV = new ArrayList<>();
		for (String selectorKey : selectors.keySet()) {
			Vector<String> props = selectors.get(selectorKey);
			// Set<String> downNodeTypes = edgeHash.get(selectorKey);
			System.out.println(selectorKey);
			dV.add(selectorKey);
			for (String prop : props) {
				// TODO fix properties
				if (!prop.contains("PRIM_KEY_PLACEHOLDER")) {
					dV.add(prop);
				}
			}

		}
		displayVar = dV.toArray(new String[dV.size()]);
		return displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute() {
		TinkerQueryEngineInterpreter interp = (TinkerQueryEngineInterpreter) ((TinkerEngine) this.engine)
				.getQueryInterpreter();
		interp.setQueryStruct(((TinkerEngine) this.engine).getQueryStruct());
		it = (TinkerIterator) interp.composeIterator();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return it.hasNext();
	}

}
