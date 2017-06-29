package prerna.engine.impl.tinker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.ds.TinkerIterator;
import prerna.ds.TinkerQueryInterpreter;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.HeadersDataRow;
import prerna.rdf.engine.wrappers.AbstractWrapper;

public class RawTinkerSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {
	
	private TinkerIterator it;

	@Override
	public void execute() {
		// we cast interp so that we can compose iterator since all other engines return a query string
		TinkerQueryInterpreter interp = (TinkerQueryInterpreter) this.engine.getQueryInterpreter();
		interp.setQueryStruct(this.qs);
		it = (TinkerIterator) interp.composeIterator();
	}
	
	@Override
	public IHeadersDataRow next() {
		Object[] row = it.next();
		if (displayVar == null) {
			getDisplayVariables();
		}
		return new HeadersDataRow(displayVar, row, row);
	}

	@Override
	public String[] getDisplayVariables() {
		if (displayVar == null) {
			Map<String, List<String>> selectors = this.qs.selectors;
			ArrayList<String> dV = new ArrayList<>();
			for (String selectorKey : selectors.keySet()) {
				List<String> props = selectors.get(selectorKey);
				dV.add(selectorKey);
				for (String prop : props) {
					if (!prop.contains("PRIM_KEY_PLACEHOLDER")) {
						dV.add(prop);
					}
				}

			}
			displayVar = dV.toArray(new String[dV.size()]);
		}
		return displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		return null;
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

}
