package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.rdf.query.builder.GremlinBuilder;

public class UniqueScaledTinkerFrameIterator implements Iterator<List<Object[]>> {

//	private String dataType;
	private String columnName;
	private GraphTraversal gt;
	private int columnNameIndex;
	private List<String> selectors;
	private String[] finalColumns;
	
	private Double[] maxArr;
	private Double[] minArr;
	
	private Object[] nextRow;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param getRawData 
	 * @param typeRoot			A list of nodes corresponding to the leaves in the tree
	 */
	public UniqueScaledTinkerFrameIterator(
			String columnName,
			//boolean getRawData, 
			List<String> selectors, 
			Graph g, Graph metaG,
			Double[] maxArr, 
			Double[] minArr) {
		this.selectors = selectors;
//		this.dataType = getRawData ? Constants.VALUE : Constants.NAME;
		this.columnName = columnName;
		this.columnNameIndex = selectors.indexOf(columnName);
		this.maxArr = maxArr;
		this.minArr = minArr;
		this.gt = openTraversal(selectors, g, metaG);
	}
	
	private GraphTraversal openTraversal(List<String> selectors, Graph g, Graph metaG){
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(selectors, g, metaG, null);
		builder.setOrderBySelector(columnName);
		this.finalColumns = selectors.toArray(new String[0]);//finalColumns.toArray(new String[0]);
		//finally execute it to get the executor
		GraphTraversal <Vertex, Map<String, Object>> gt = (GraphTraversal <Vertex, Map<String, Object>>)builder.executeScript();
//		gt = gt.order();
//		gt = gt.order().by(__.V().has(Constants.TYPE, columnName).values(Constants.NAME));
		return gt;
	}
	
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 * Must also take into consideration the number of instances associated with each node
	 * Must also take into consideration the fan-out of the btree for siblings of node
	 */
	@Override
	public boolean hasNext() {
		return (gt.hasNext() || this.nextRow != null);
	}
	
	@Override
	public List<Object[]> next() {
		List<Object[]> retList = new Vector<Object[]>();
		
		String instanceValue = "";
		if(this.nextRow == null) {
			Object data = gt.next();
			Object[] row = getRowFromTraversal(data);
			instanceValue = row[columnNameIndex] + "";
			retList.add(row);
		} else {
			retList.add(nextRow);
			instanceValue = nextRow[columnNameIndex] + "";
			this.nextRow = null;
		}
		
		while(gt.hasNext()) {
			this.nextRow = getRowFromTraversal(gt.next());
			if(this.nextRow[columnNameIndex].equals(instanceValue)) {
				retList.add(this.nextRow);
				this.nextRow = null;
			} else {
				break;
			}
		}

		return retList;
	}
	
	private Object[] getRowFromTraversal(Object data) {
		Object[] row = new Object[finalColumns.length];
		if(data instanceof Map) {
			for(int colIndex = 0; colIndex < finalColumns.length; colIndex++) {
				Map<String, Object> mapData = (Map<String, Object>) data; //cast to map
				Object value = ((Vertex)mapData.get(finalColumns[colIndex])).property(TinkerFrame.TINKER_NAME).value();
				if(colIndex != this.columnNameIndex && maxArr[colIndex] != null && minArr[colIndex] != null && value instanceof Number) {
					if(value instanceof Number) {
						row[colIndex] = ( ((Number)value).doubleValue() - minArr[colIndex])/(maxArr[colIndex] - minArr[colIndex]);
					} else {
						row[colIndex] = null;
					}
				} else {
					// this is not a number and doesn't need to be scaled
					row[colIndex] = value;
				}
			}
		} else {
			row[0] = ((Vertex)data).property(TinkerFrame.TINKER_NAME).value();
		}
		
		return row;
	}
}
