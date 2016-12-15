package prerna.ds.h2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import prerna.algorithm.api.IMetaData.DATA_TYPES;

public class ScaledUniqueH2FrameIterator implements Iterator<List<Object[]>> {

	private String tableName;
	private String columnName;

	private List<String> selectors;
	private List<DATA_TYPES> dataTypes;

	private Double[] maxArr;
	private Double[] minArr;
	
	private H2Builder builder;
	private Iterator<Object> valueIterator;
	
	public ScaledUniqueH2FrameIterator(
		String columnName,
		String tableName, 
		H2Builder builder, 
		Double[] maxArr, 
		Double[] minArr,
		List<DATA_TYPES> dataTypes,
		List<String> selectors) {
		
		this.selectors = selectors;
		this.tableName = tableName;
		this.columnName = columnName;
		this.maxArr = maxArr;
		this.minArr = minArr;
		this.builder = builder;
		this.dataTypes = dataTypes;
		
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
			List<Object[]> retData = builder.getScaledData(tableName, selectors, dataTypes, columnName, nextVal, maxArr, minArr);
			return retData;
		} else {
			throw new NoSuchElementException("No more elements"); 
		}
		
	}
}
