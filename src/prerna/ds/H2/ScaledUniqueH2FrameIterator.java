package prerna.ds.H2;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Graph;

import prerna.util.Constants;

public class ScaledUniqueH2FrameIterator implements Iterator<List<Object[]>> {

	private String dataType;
	private String columnName;
	private int columnNameIndex;
	private List<String> selectors;
	private String[] finalColumns;
	
	private Double[] maxArr;
	private Double[] minArr;
	
	private List<Object[]> nextBatch;
	
	private H2Builder builder;
	private Iterator<Object> valueIterator;
	
	public ScaledUniqueH2FrameIterator(
		String columnName,
		boolean getRawData, 
		List<String> selectors, 
		H2Builder builder, 
		Double[] maxArr, 
		Double[] minArr) {
		
		this.selectors = selectors;
		this.dataType = getRawData ? Constants.VALUE : Constants.NAME;
		this.columnName = columnName;
		this.columnNameIndex = selectors.indexOf(columnName);
		this.maxArr = maxArr;
		this.minArr = minArr;
		this.builder = builder;
		
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
			List<Object[]> retData = builder.getData(selectors, columnName, nextVal);
			return retData;
		} else {
			throw new NoSuchElementException("No more elements"); 
		}
		
	}
}
