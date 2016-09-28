package prerna.ds.H2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ScaledUniqueH2FrameIterator implements Iterator<List<Object[]>> {

//	private String dataType;
	private String columnName;
	private int columnNameIndex;
	private List<String> selectors;
	private String tableName;
	private String[] finalColumns;
	
	private Double[] maxArr;
	private Double[] minArr;
	
	private List<Object[]> nextBatch;
	
	private H2Builder builder;
	private Iterator<Object> valueIterator;
	
	private Map<String, String> headerTypeMap;
	
	public ScaledUniqueH2FrameIterator(
		String columnName,
//		boolean getRawData, 
		String tableName, 
		H2Builder builder, 
		Double[] maxArr, 
		Double[] minArr,
		Map<String, String> headerTypeMap,
		List<String> selectors) {
		
		this.selectors = selectors;
		this.tableName = tableName;
//		this.dataType = getRawData ? Constants.VALUE : Constants.NAME;
		this.columnName = columnName;
		this.columnNameIndex = selectors.indexOf(columnName);
		this.maxArr = maxArr;
		this.minArr = minArr;
		this.builder = builder;
		this.headerTypeMap = headerTypeMap;
		
		Object[] column = builder.getColumn(columnName, true);
		valueIterator = Arrays.asList(column).iterator();
	}
	@Override
	public boolean hasNext() {
		return this.valueIterator.hasNext();
	}

	@Override
	public List<Object[]> next() {
		
		if(hasNext()) {
			Object nextVal = valueIterator.next();
			List<Object[]> retData = builder.getScaledData(tableName, selectors, headerTypeMap, columnName, nextVal, maxArr, minArr);
			return retData;
		} else {
			throw new NoSuchElementException("No more elements"); 
		}
		
	}
}
