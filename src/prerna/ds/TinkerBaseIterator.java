package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import prerna.rdf.query.builder.IQueryInterpreter;

public abstract class TinkerBaseIterator implements Iterator{
	protected GraphTraversal gt;
	protected List<String> selectors;
	protected Object[] nextRow;
	protected Map<String, Map<String, List>> filters;
	protected int performCount;
	protected int rowCount;
	
	/**
	 * Tinker does not support regular expressions so treat the value as a
	 * string and apply a regex
	 * 
	 * @param value
	 * @param filterInfo
	 * @return
	 */
	public String search(String value, Map<String, List> filterInfo) {
		String match = null;
		for (String filterType : filterInfo.keySet()) {
			if (filterType.equals(IQueryInterpreter.SEARCH_COMPARATOR)) {
				List filterVals = filterInfo.get(filterType);
				String strPattern = (String) filterVals.get(0);
				String pattern = ".*" + strPattern.toLowerCase() + ".*";
				Pattern r = Pattern.compile(pattern);

				Matcher m = r.matcher(value.toLowerCase());
				if (m.find()) {
					match = value;
				}
			}
			else {
				match = value;
			}
		}
		return match;
	}
	
	@Override
	public boolean hasNext() {
		return nextRow != null;
	}

	@Override
	public Object[] next() {
		Object[] row = nextRow;
		getNextValidRow();
		return row;
	}
	
	public abstract void getNextValidRow();

}
