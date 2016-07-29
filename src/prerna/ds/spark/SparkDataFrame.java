package prerna.ds.spark;

import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerMetaData;
import prerna.ds.H2.H2Builder.Join;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.ColAddReactor;
import prerna.sablecc.H2ImportDataReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.SparkImportDataReactor;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker.DATA_FRAME_REACTORS;
import prerna.util.ArrayUtilityMethods;
public class SparkDataFrame extends AbstractTableDataFrame{

	SparkBuilder builder;
	
	public SparkDataFrame() {
		builder = new SparkBuilder();
		this.metaData = new TinkerMetaData();
	}
	@Override
	public void addRow(Object[] rowCleanData, Object[] rowRawData) {
		
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
		
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		return null;
	}

	@Override
	public Double getMax(String columnHeader) {
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		return null;
	}

	@Override
	public int getNumRows() {
		if(this.builder.frame == null) return 0;
		
		//will error out for big numbers...should change on interface level to be long?
		return (int) builder.frame.count();
		
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
		return new SparkFrameIterator(this.builder.frame);
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options) {
		return new SparkFrameIterator(this.builder.buildIterator(options));
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll) {
		return new UniqueValueSparkFrameIterator(this.builder.getUniqueValues(columnHeader));
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unfilter(String columnHeader) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unfilter() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeColumn(String columnHeader) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object[] getFilterModel() {
		return null;
	}

	@Override
	public void save(String fileName) {
		//create a parquet file?
	}

	@Override
	public ITableDataFrame open(String fileName, String userId) {
		//open a parquet file?
		return null;
	}

	@Override
	public void addRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {
		//does this apply to spark?
	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {
		//does this apply to spark?
	}

	@Override
	public void addRow(Object[] cleanCells, Object[] rawCells, String[] headers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToValMap) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object[]> getFilterTransformationValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addRelationship(String[] headers, Object[] values, Object[] rawValues, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToValMap) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		
	}
	
	@Override
	public boolean isEmpty() {
		return (this.builder.frame == null || this.builder.frame.count() == 0);
	}
	
	public void processIterator(Iterator<IHeadersDataRow> iterator, String[] newHeaders, Map<String, String> logicalToValue, Vector<Map<String, String>> joins, String joinType) {
		
		//convert the new headers into value headers
		String[] valueHeaders = new String[newHeaders.length];
		if(logicalToValue == null) {
			for(int i = 0; i < newHeaders.length; i++) {
					valueHeaders[i] = this.metaData.getValueForUniqueName(newHeaders[i]);
				}
			} else {	
				for(int i = 0; i < newHeaders.length; i++) {
				valueHeaders[i] = logicalToValue.get(newHeaders[i]);
			}
		}
		
		String[] types = new String[newHeaders.length];
		for(int i = 0; i < newHeaders.length; i++) {
			types[i] = convertDataTypeToString(this.metaData.getDataType(newHeaders[i]));
		}
		
		String[] columnHeaders = getColumnHeaders();
		
		// my understanding
		// need to get the list of columns that are currently inside the frame
		// this is because mergeEdgeHash has already occured and added the headers into the metadata
		// thus, columnHeaders has both the old headers and the new ones that we want to add
		// thus, go through and only keep the list of headers that are not in the new ones
		// but also need to add those that are in the joinCols in case 2 headers match
		List<String> adjustedColHeadersList = new Vector<String>();
		for(String header : columnHeaders) {
			if(!ArrayUtilityMethods.arrayContainsValueIgnoreCase(newHeaders, header)) {
				adjustedColHeadersList.add(this.metaData.getValueForUniqueName(header));
			} else {
				joinLoop : for(Map<String, String> join : joins) {
					if(join.keySet().contains(header)) {
						adjustedColHeadersList.add(this.metaData.getValueForUniqueName(header));
						break joinLoop;
					}
				}
			}
		}
		String[] adjustedColHeaders = adjustedColHeadersList.toArray(new String[]{});
		
		//get the join type
//		Join jType = Join.INNER;
//		if(joinType != null) {
//			if(joinType.toUpperCase().startsWith("INNER")) {
//				jType = Join.INNER;
//			} else if(joinType.toUpperCase().startsWith("OUTER")) {
//				jType = Join.FULL_OUTER;
//			} else if(joinType.toUpperCase().startsWith("LEFT")) {
//				jType = Join.LEFT_OUTER;
//			} else if(joinType.toUpperCase().startsWith("RIGHT")) {
//				jType = Join.RIGHT_OUTER;
//			}
//
//		}
		
		this.builder.processIterator(iterator, adjustedColHeaders, valueHeaders, types, " inner join ");
	}

	public void processList(List<Object[]> list, String[] oldHeaders, String[] newHeaders, String[] types, String joinType) {
		this.builder.processList(list, oldHeaders, newHeaders, types, joinType);
	}
	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<String, String>();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.SparkMathReactor");
		reactorNames.put(PKQLEnum.CSV_TABLE, "prerna.sablecc.CsvTableReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.SparkColAddReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.SparkImportDataReactor");
		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put("SUM", "prerna.algorithm.impl.spark.SparkSumReactor");
		reactorNames.put("AVERAGE", "prerna.algorithm.impl.spark.SparkAverageReactor");
		reactorNames.put("COUNT", "prerna.algorithm.impl.spark.SparkCountReactor");
		reactorNames.put("MAX", "prerna.algorithm.impl.spark.SparkMaxReactor");
		reactorNames.put("MIN", "prerna.algorithm.impl.spark.SparkMinReactor");
		
		return reactorNames;
	}
	
	public Object mapReduce(Vector<String> columns, Vector<String> groupBys, String algoName, String expr) {
		String algType;
		algoName = algoName.toUpperCase();
		if(algoName.contains("AVERAGE")) {
			algType = "AVERAGE";
		} else if(algoName.contains("COUNT")) {
			algType = "COUNT";
		} else if(algoName.contains("SUM")) {
			algType = "SUM";
		}  else if(algoName.contains("MAX")) {
			algType = "MAX";
		} else if(algoName.contains("MIN")) {
			algType = "MIN";
		} else if(algoName.contains("CONCAT")) {
			algType = "AVERAGE";
		} else {
			algType = "COUNT";
		}
		
		Object o = builder.mapReduce(columns, groupBys, algType, expr);
		System.out.println(o);
		return o;
	}
	
	//idea is the a dataframe was previously created by some group by or calculation
	//we want to merge that frame back into the existing frame
	public void mergeTable(String key, String joinType, String newCol) {
		this.builder.mergeFrame(key, newCol);
	}
	
	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData, Map<String, Object> options) {
		return null;
	}
	
	public String convertDataTypeToString(IMetaData.DATA_TYPES type) {
		if(type.equals(IMetaData.DATA_TYPES.NUMBER)) { 
			return "double";
		} else if(type.equals(IMetaData.DATA_TYPES.STRING)) {
			return "varchar(800)";
		} else {
			return "date";
		}
	}

	
	@Override
	public String getDataMakerName() {
		return "SparkDataFrame";
	}
	@Override
	public void filter(String columnHeader,
			Map<String, List<Object>> filterValues) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void resetDataId() {
		// TODO Auto-generated method stub
		
	}
}
