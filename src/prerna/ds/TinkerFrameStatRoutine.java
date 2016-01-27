package prerna.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;
import prerna.rdf.query.builder.GremlinBuilder;
import prerna.util.Constants;

public class TinkerFrameStatRoutine implements IAnalyticTransformationRoutine {
	
	private static final String PRIMARY_SELECTOR = "primary_selector";
	private static final String SECONDARY_SELECTOR = "secondary_selector";
	
	private static final String COUNT = "COUNT";
	private static final String AVERAGE = "AVERAGE";
	private static final String MIN = "MIN";
	private static final String MAX = "MAX";
	private static final String SUM = "SUM";
	
	private Map<String, Object> functionMap;
	private String newColumn;
	
	public TinkerFrameStatRoutine() {
		functionMap = new HashMap<String, Object>(0);
	}
	
	@Override
	public String getName() {
		return "TinkerFrameStatRoutine";
	}

	@Override
	public String getResultDescription() {
		return "";
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		functionMap = selected;		
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return null;
	}

	@Override
	public String getDefaultViz() {
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		return null;
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		// TODO Auto-generated method stub
		//grab first, cast to tinker
		ITableDataFrame table = data[0];
		if(table instanceof TinkerFrame) {
			
			TinkerFrame tinkerGraph = (TinkerFrame)table;
			
			//gather the necessary parameters
			String valueColumn = this.functionMap.get("name").toString();
			String mathType = this.functionMap.get("math").toString();
			String newColumnHeader = this.functionMap.get("calcName").toString();
			String columnHeader = ((List<String>)this.functionMap.get("GroupBy")).get(0);
			
			//calculate group by and add to tinker
			addStatColumnToTinker(tinkerGraph, columnHeader, mathType, newColumnHeader, valueColumn);
			
			this.newColumn = newColumnHeader;
		} else {
			throw new IllegalArgumentException("Cannot run stat routine on instance of"+table.getClass().getName());
		}
		return null;
	}
	
	/**
	 * 
	 * @param tinker
	 * @param columnHeader - the column to join on
	 * @param mathType - type of operation to do
	 * @param newColumnName - name of the new column
	 * @param valueColumn - the column to do calculations on
	 */
	private void addStatColumnToTinker(TinkerFrame tinker, String columnHeader, String mathType, String newColumnName, String valueColumn) {
				
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(tinker.getColumnHeaders(), tinker.columnsToSkip, tinker.g);
		GraphTraversal statIterator = (GraphTraversal)builder.executeScript(tinker.g);
		
		statIterator = statIterator.group().by(__.select(columnHeader).values(Constants.NAME)).as(PRIMARY_SELECTOR);
		switch(mathType.toUpperCase()) {
			case AVERAGE : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).mean()).as(SECONDARY_SELECTOR).select(PRIMARY_SELECTOR); break;
			}
			case MIN : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).min()).as(SECONDARY_SELECTOR).select(PRIMARY_SELECTOR); break;
			}
			case MAX : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).max()).as(SECONDARY_SELECTOR).select(PRIMARY_SELECTOR); break;
			}
			case SUM : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).sum()).as(SECONDARY_SELECTOR).select(PRIMARY_SELECTOR); break;
			}
			case COUNT : {
				statIterator = statIterator.by(__.count()); break;
			}
			default : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).mean()).as(SECONDARY_SELECTOR).select(PRIMARY_SELECTOR); break;
			}
		}
		
		
		if(statIterator.hasNext()) {
			
			//the result of the graph traversal
			Map<String, Map<Object, Object>> resultMap;
			Map<Object, Object> groupByMap;
			
			//Count is a special case
//			if(mathType.toUpperCase().equals(COUNT)) {
//				groupByMap = (Map<Object, Object>)statIterator.next();
//			} else {
				groupByMap = (Map<Object, Object>)statIterator.next();
//				groupByMap = resultMap.get(PRIMARY_SELECTOR);
//			}
			
			//create the edgehash associated with the new result map
			Map<String, Set<String>> newEdgeHash = new HashMap<String, Set<String>>(1);
			Set<String> edgeSet = new HashSet<String>(1);
			edgeSet.add(newColumnName);
			
			//merge the new edge hash with the existing one in tinker
			newEdgeHash.put(columnHeader, edgeSet);			
			tinker.mergeEdgeHash(newEdgeHash);
			
			//add that relationship to the tinker graph
			for(Object key : groupByMap.keySet()) {
			
				Map<String, Object> newRow = new HashMap<String, Object>(2);
				newRow.put(columnHeader, key);
				newRow.put(newColumnName, groupByMap.get(key));
				
				tinker.addRelationship(newRow, newRow);
			}			
		}
	}

	@Override
	public List<String> getChangedColumns() {
		List<String> changedColumns = new ArrayList<String>(1);
		changedColumns.add(this.newColumn);
		return changedColumns;
	}

}
