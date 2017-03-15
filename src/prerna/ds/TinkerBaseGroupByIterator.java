package prerna.ds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerBaseGroupByIterator extends TinkerBaseIterator {
	private ArrayList<Object[]> rowList;
	private int index;

	/**
	 * The group by step groups vertices to the group by value. To iterate
	 * through the values the vertices are flushed out to a list of rows.
	 * 
	 * @param gt
	 * @param selectors
	 * @param qs
	 */
	public TinkerBaseGroupByIterator(GraphTraversal gt, List<String> selectors, QueryStruct qs) {
		this.gt = gt;
		this.selectors = selectors;
		this.filters = qs.andfilters;
		this.performCount = qs.getPerformCount();
		// Grab groupBy values from the GraphTraversal and add to the rowList
		selectors.remove("GROUP_BY");
		rowList = new ArrayList<Object[]>();
		// System.out.println("**************************************");
		populateRowList();
		gt = null;
		index = 0;
		getNextValidRow();

	}

	private void populateRowList() {
		Object data = gt.next();

		// data will be a map for multi columns
		if (data instanceof Map) {
			Map<String, Object> dataMap = (Map<String, Object>) data;
			for (Object keys : dataMap.keySet()) {
				System.out.println("************************************************************************************GROUP BY VALUE"+keys);
				int groupByRows = 1;
				ArrayList<Map<String, Object>> verticies = (ArrayList<Map<String, Object>>) dataMap.get(keys);
				for (Map<String, Object> o : verticies) {
					Object[] retObject = new Object[selectors.size()];
					for (int colIndex = 0; colIndex < selectors.size(); colIndex++) {
						// Map<String, Object> mapData = (Map<String,
						// Object>) data;
						String select = selectors.get(colIndex);
						Object vertOrProp = o.get(select);
						Object value = null;
						if (vertOrProp instanceof Vertex) {
							value = ((Vertex) vertOrProp).property(TinkerFrame.TINKER_NAME).value();
						} else {
							value = vertOrProp;
						}
						retObject[colIndex] = value;
					}
					groupByRows++;
					rowList.add(retObject);
				}
				System.out.println("************************************************************************************GROUP BY ROW COUNT"+groupByRows);


			}

		}
	}

	/**
	 * Sets the nextRow to the valid row or null
	 */
	public void getNextValidRow() {
		if (index < rowList.size()) {
			nextRow = getNextRow();
			index++;
		}
		else {
			nextRow = null;
		}
	}

	private Object[] getNextRow() {
		return rowList.get(index);
	}

}
