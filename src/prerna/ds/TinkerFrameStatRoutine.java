package prerna.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.H2.TinkerH2Frame;
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
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) throws RuntimeException {
		// TODO Auto-generated method stub
		//grab first, cast to tinker
		ITableDataFrame table = data[0];
		if(table instanceof TinkerH2Frame) {
			TinkerH2Frame tinkerGraph = (TinkerH2Frame)table;
			
			//gather the necessary parameters
			String valueColumn = this.functionMap.get("name").toString();
			String mathType = this.functionMap.get("math").toString();
			String newColumnHeader = this.functionMap.get("calcName").toString();
			String[] columnHeaders = ((List<String>)this.functionMap.get("GroupBy")).toArray(new String[0]);
			if(columnHeaders.length > 1) {
				throw new UnsupportedOperationException("H2 currently does not support heat maps or multi column joins");
			}
			//calculate group by and add to tinker
			try {
				addStatColumnToTinker(tinkerGraph, columnHeaders[0], mathType, newColumnHeader, valueColumn);
			} catch(ClassCastException e) {
				throw new ClassCastException("Error with computation. Please make sure aggregation values are non-empty and numerical.");
			}
			this.newColumn = newColumnHeader;
		}
		
		else if(table instanceof TinkerFrame) {
			
			TinkerFrame tinkerGraph = (TinkerFrame)table;
			
			//gather the necessary parameters
			String valueColumn = this.functionMap.get("name").toString();
			String mathType = this.functionMap.get("math").toString();
			String newColumnHeader = this.functionMap.get("calcName").toString();
//			String columnHeader = ((List<String>)this.functionMap.get("GroupBy")).get(0);
			String[] columnHeaders = ((List<String>)this.functionMap.get("GroupBy")).toArray(new String[0]);
			
			//calculate group by and add to tinker
			try {
				addStatColumnToTinker(tinkerGraph, columnHeaders, mathType, newColumnHeader, valueColumn);
			} catch(ClassCastException e) {
				throw new ClassCastException("Error with computation. Please make sure aggregation values are non-empty and numerical.");
			}
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
	private void addStatColumnToTinker(TinkerH2Frame tinker, String columnHeader, String mathType, String newColumnName, String valueColumn) {	
		tinker.connectTypes(columnHeader, newColumnName);
		tinker.applyGroupBy(columnHeader, newColumnName, valueColumn, mathType);
	}
	
	/**
	 * 
	 * @param tinker
	 * @param columnHeader - the column to join on
	 * @param mathType - type of operation to do
	 * @param newColumnName - name of the new column
	 * @param valueColumn - the column to do calculations on
	 */
	private void addStatColumnToTinker(TinkerFrame tinker, String[] columnHeader, String mathType, String newColumnName, String valueColumn) throws ClassCastException{
				
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(tinker.getSelectors(), tinker.g);
		GraphTraversal statIterator = (GraphTraversal)builder.executeScript();
		
		boolean singleColumn = columnHeader.length == 1;
		
		if(singleColumn) {
			statIterator = statIterator.group().by(__.select(columnHeader[0]).values(Constants.NAME)).as(PRIMARY_SELECTOR);
		} else if(columnHeader.length == 2) {
			statIterator = statIterator.group().by(__.select(columnHeader[0], columnHeader[1]).by(Constants.NAME)).as(PRIMARY_SELECTOR);
		} else {
			String[] restOfHeaders = Arrays.copyOfRange(columnHeader, 2, columnHeader.length-1);
			statIterator = statIterator.group().by(__.select(columnHeader[0], columnHeader[1], restOfHeaders).by(Constants.NAME)).as(PRIMARY_SELECTOR);
		}
		
		switch(mathType.toUpperCase()) {
			case AVERAGE : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).mean()).as(SECONDARY_SELECTOR);break;
			}
			case MIN : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).min()).as(SECONDARY_SELECTOR);break;
			}
			case MAX : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).max()).as(SECONDARY_SELECTOR);break;
			}
			case SUM : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).sum()).as(SECONDARY_SELECTOR);break;
			}
			case COUNT : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).count()).as(SECONDARY_SELECTOR); break;
			}
			default : {
				statIterator = statIterator.by(__.select(valueColumn).values(Constants.NAME).mean()).as(SECONDARY_SELECTOR);break;
			}
		}
		
		statIterator = statIterator.select(PRIMARY_SELECTOR);
		
		try {
			if(statIterator.hasNext()) {
				
				//the result of the graph traversal
				Map<String, Map<Object, Object>> resultMap;
				Map<Object, Object> groupByMap;
				
				groupByMap = (Map<Object, Object>)statIterator.next();
				
				//create the edgehash associated with the new result map
				Map<String, Set<String>> newEdgeHash = new HashMap<String, Set<String>>(1);
				Set<String> edgeSet = new HashSet<String>(1);
				edgeSet.add(newColumnName);
				
				
				if(singleColumn) {
					
					for(String column : columnHeader) {
						newEdgeHash.put(column, edgeSet);			
					}
					tinker.mergeEdgeHash(newEdgeHash);
					
					String cHeader = columnHeader[0];
					for(Object key : groupByMap.keySet()) {
						
						Map<String, Object> newRow = new HashMap<String, Object>(2);
						newRow.put(cHeader, key);
						newRow.put(newColumnName, groupByMap.get(key));
						
						tinker.addRelationship(newRow, newRow, newEdgeHash);
					}
					
					// add appropriate blanks
					List<String> addedCols = new ArrayList<String>();
					addedCols.add(newColumnName);
					tinker.insertBlanks(columnHeader[0], addedCols);
				} else {
					
					tinker.connectTypes(columnHeader, newColumnName);
					
					String primKeyName = tinker.getPrimaryKey(columnHeader);
					for(Object key : groupByMap.keySet()) {
						Map<String, Object> cleanRow = new HashMap<String, Object>(columnHeader.length+1);
						Map<String, Object> rawRow = new HashMap<String, Object>(columnHeader.length+1);
						Map<String, Object> mapKey = (Map<String, Object>)key;
						
						Object[] values = new Object[columnHeader.length];
						for(int i = 0; i < columnHeader.length; i++) {
							values[i] = mapKey.get(columnHeader[i]);
							cleanRow.put(columnHeader[i], values[i]);
						}
						
						cleanRow.put(newColumnName, groupByMap.get(key));
						cleanRow.put(primKeyName, tinker.getPrimaryKey(values));
						
						rawRow.putAll(cleanRow);
						rawRow.put(primKeyName, TinkerFrame.PRIM_KEY);
						tinker.addRelationship(cleanRow, rawRow);
					}
					
					// add appropriate blanks
					List<String> addedCols = new ArrayList<String>();
					addedCols.add(primKeyName);
					for(String column : columnHeader) {
						tinker.insertBlanks(column, addedCols);
					}
					addedCols.clear();
					addedCols.add(newColumnName);
					tinker.insertBlanks(primKeyName, addedCols);

				}	

				String[] newHeaders = new String[]{newColumnName};
				String[] newHeaderType = new String[]{"NUMBER"};
				tinker.addMetaDataTypes(newHeaders, newHeaderType);
			}
		} catch(ClassCastException e) {
			throw new ClassCastException(e.getMessage());
		}
	}

	@Override
	public List<String> getChangedColumns() {
		List<String> changedColumns = new ArrayList<String>(1);
		changedColumns.add(this.newColumn);
		return changedColumns;
	}

}
