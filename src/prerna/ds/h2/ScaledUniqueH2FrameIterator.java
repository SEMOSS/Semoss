package prerna.ds.h2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryArithmeticSelector;
import prerna.query.querystruct.QueryColumnSelector;
import prerna.query.querystruct.QueryConstantSelector;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.QueryFilter;

public class ScaledUniqueH2FrameIterator implements Iterator<List<Object[]>> {

//	private String tableName;
//
//	private List<String> selectors;
//	private List<DATA_TYPES> dataTypes;
//
//	private Double[] maxArr;
//	private Double[] minArr;
	
	private String uniqueColumnName;
	
	private H2Frame frame;
	private QueryStruct2 qs;
	private Iterator<Object> valueIterator;
	
	public ScaledUniqueH2FrameIterator(
		H2Frame frame, 
		String columnName,
		Double[] maxArr, 
		Double[] minArr,
		List<DATA_TYPES> dataTypes,
		List<String> selectors) {
		
//		this.selectors = selectors;
//		this.tableName = tableName;
//		this.maxArr = maxArr;
//		this.minArr = minArr;
//		this.dataTypes = dataTypes;
		
		this.uniqueColumnName = columnName;
		this.frame = frame;
		Object[] column = frame.getColumn(columnName);
		this.valueIterator = Arrays.asList(column).iterator();
		
		// create the QS being used for querying
		this.qs = new QueryStruct2();
		QueryColumnSelector instanceSelector = new QueryColumnSelector();
		String[] split = columnName.split("__");
		instanceSelector.setTable(split[0]);
		instanceSelector.setColumn(split[1]);
		
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			String unqiueSelectorName = selectors.get(i);
			
			if(maxArr[i] != null && minArr[i] != null) {
				// we need to normalize this guy
				double range = maxArr[i] - minArr[i];
				QueryColumnSelector sColumn = new QueryColumnSelector();
				String[] sSplit = unqiueSelectorName.split("__");
				sColumn.setTable(sSplit[0]);
				sColumn.setColumn(sSplit[1]);
				
				QueryConstantSelector minConst = new QueryConstantSelector();
				minConst.setConstant(minArr[i]);
				
				QueryArithmeticSelector minusSelector = new QueryArithmeticSelector();
				minusSelector.setLeftSelector(sColumn);
				minusSelector.setRightSelector(minConst);
				minusSelector.setMathExpr("-");
				
				QueryConstantSelector rangeConst = new QueryConstantSelector();
				rangeConst.setConstant(range);
				
				QueryArithmeticSelector normalizeSelector = new QueryArithmeticSelector();
				normalizeSelector.setLeftSelector(minusSelector);
				normalizeSelector.setRightSelector(rangeConst);
				normalizeSelector.setMathExpr("/");
				
				qs.addSelector(normalizeSelector);
			} else {
				// we just add this as a column
				QueryColumnSelector sColumn = new QueryColumnSelector();
				String[] sSplit = unqiueSelectorName.split("__");
				sColumn.setTable(sSplit[0]);
				sColumn.setColumn(sSplit[1]);
				
				qs.addSelector(sColumn);
			}
		}
		
		// dont forget about filters
		qs.setFilters(frame.getFrameFilters());
	}
	
	@Override
	public boolean hasNext() {
		if(this.valueIterator.hasNext()) {
			return true;
		}
		else {
			//remove filters for the last value in the iterator
			this.qs.getFilters().removeColumnFilter(uniqueColumnName);
			return false;
		}
		
	}

	@Override
	public List<Object[]> next() {
		if(hasNext()) {
			Object nextVal = valueIterator.next();
			QueryFilter instanceFilter = null;
			if(nextVal instanceof Number) {
				instanceFilter = new QueryFilter(
						new NounMetadata(uniqueColumnName, PixelDataType.COLUMN), 
						"==", 
						new NounMetadata(nextVal, PixelDataType.CONST_DECIMAL)
						);
			} else {
				instanceFilter = new QueryFilter(
						new NounMetadata(uniqueColumnName, PixelDataType.COLUMN), 
						"==", 
						new NounMetadata(nextVal, PixelDataType.CONST_STRING)
						);
			}
			
			// remove all previous filters
			this.qs.getFilters().removeColumnFilter(uniqueColumnName);
			// and add this one single value filter
			this.qs.addFilter(instanceFilter);
			
			List<Object[]> retData = new ArrayList<Object[]>();
			Iterator<IHeadersDataRow> it = frame.query(qs);
			while(it.hasNext()) {
				retData.add(it.next().getValues());
			}
			return retData;
		} else {
			throw new NoSuchElementException("No more elements"); 
		}
		
	}
}
